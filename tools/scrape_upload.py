#!/usr/bin/env python3

'''
Generic scrape upload tool.

Reads Brotli-compressed JSON files produced by scraping tools,
validates each asset against its configured JSON Schema, uploads via
either the bulk endpoint or the ExchangeClient background queue, and
moves successfully uploaded files into the ``uploaded/`` subdirectory
owned by the directory they were read from.
'''

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import brotli
import redis.asyncio as aioredis
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import SettingsConfigDict
from watchfiles import Change, awatch

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    list_bulk_states,
    resume_pending_bulk_uploads,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    RedisChannelScrapeQueue,
)
from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    parse_priority_queues,
)
from scrape_exchange.exchange_client import (
    METRIC_BACKGROUND_UPLOADS,
    ExchangeClient,
)
from scrape_exchange.file_management import (
    CHANNEL_FILE_PREFIX,
    COMPRESSED_JSON_SUFFIX,
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
    AssetFileManagement,
)
from scrape_exchange.instagram import InstagramCreator
from scrape_exchange.logging import (
    configure_logging as configure_shared_logging,
)
from scrape_exchange.metrics_server import start_metrics_server
from scrape_exchange.name_map import NameMap, NullNameMap, RedisNameMap
from scrape_exchange.onlyfans.onlyfans_creator import OnlyFansCreator
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.schema_validator import SchemaValidator, fetch_schema_dict
from scrape_exchange.scraper_metrics import (
    METRIC_BULK_JOBS_IN_FLIGHT,
    METRIC_FILES_PENDING_UPLOAD,
    METRIC_UPLOAD_BATCHES,
    METRIC_UPLOADS_FAILED,
    METRIC_UPLOADS_MISSING_RESULT,
)
from scrape_exchange.settings import ScraperSettings, normalize_log_level
from scrape_exchange.streaming_listing import (
    LISTING_CHUNK_SIZE,
    StreamingListing,
    count_matching_files,
)
from scrape_exchange.tiktok import (
    TikTokCreator,
    TikTokHashtag,
    TikTokVideo,
)
from scrape_exchange.twitch import TwitchCreator
from scrape_exchange.upload import (
    BulkUploadConfig,
    emit_bulk_batch_metrics,
    ndjson_line,
    finalize_prepared_bulk_batch,
    post_prepared_bulk_batch,
    validate_upload_record,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds
from scrape_exchange.youtube.youtube_channel import YouTubeChannel, fallback_handle
from scrape_exchange.youtube.youtube_video import YouTubeVideo

UploadMode = Literal['bulk', 'background']
RecordLoader = Callable[[dict[str, Any]], dict[str, Any]]

SCRAPER_LABEL: str = 'scrape_uploader'
TIKTOK_VIDEO_PREFIX: str = 'tiktok-video-'
TIKTOK_CREATOR_PREFIX: str = 'tiktok-creator-'
TIKTOK_HASHTAG_PREFIX: str = 'tiktok-hashtag-'
INSTAGRAM_CREATOR_PREFIX: str = 'instagram-creator-'
TWITCH_CREATOR_PREFIX: str = 'twitch-creator-'
ONLYFANS_CREATOR_PREFIX: str = 'onlyfans-creator-'
# Longest wait in the round-robin before re-polling every target while
# bulk jobs are still running.
ROUND_ROBIN_POLL_SECONDS: float = 60.0
# Upper bound on the shutdown wait for in-flight bulk jobs; stays below
# the container stop grace period so the drain finishes before SIGKILL.
BULK_SHUTDOWN_DRAIN_SECONDS: float = 45.0
SHUTDOWN_SIGNALS: tuple[signal.Signals, ...] = (
    signal.SIGTERM, signal.SIGINT,
)
SCRAPE_UPLOAD_DEFAULT_LOG_FILE: str = (
    '/var/log/scrape/scrape_upload.log'
)


@dataclass(frozen=True)
class AssetDescriptor:
    platform: str
    entity: str
    prefixes: tuple[str, ...]
    schema_owner: str
    schema_version: str
    filename_prefix: str
    load_record: RecordLoader

    @property
    def prefix_rankings(self) -> dict[str, list[str]]:
        return {
            self.entity: list(self.prefixes),
        }


@dataclass(frozen=True)
class AssetTargetSpec:
    descriptor: AssetDescriptor
    directory: str


@dataclass
class TargetUploadState:
    '''Mutable bulk-upload state of one target: its streaming listing
    and the jobs POSTed but not yet reconciled.

    ``in_flight_jobs`` holds every background task the round-robin
    waits on: the finalize task of each POSTed job (also keyed to its
    job id in ``in_flight_job_ids``) and, at most one at a time, the
    ``resume_task`` that reconciles orphaned jobs.'''
    listing: StreamingListing | None = None
    pending: deque[str] = field(default_factory=deque)
    pass_ended: bool = False
    in_flight_files: set[str] = field(default_factory=set)
    in_flight_jobs: set[asyncio.Task] = field(default_factory=set)
    in_flight_job_ids: dict[asyncio.Task, str] = field(
        default_factory=dict,
    )
    resume_task: asyncio.Task | None = None
    # One orphan resume per round-robin drain, so a resume that cannot
    # reconcile anything does not loop.
    resume_attempted: bool = False
    # Set when a finalize released its files, so the next drain may
    # need a fresh listing pass to see them again.
    files_released: bool = False


@dataclass(frozen=True)
class AssetUploadTarget:
    descriptor: AssetDescriptor
    fm: AssetFileManagement
    validator: SchemaValidator
    processor: AssetProcessor | None = None
    state: Any = None
    upload: TargetUploadState = field(
        default_factory=TargetUploadState, compare=False,
    )


def _youtube_video_record(data: dict[str, Any]) -> dict[str, Any]:
    return YouTubeVideo.from_dict(data).to_dict()


def _youtube_channel_record(data: dict[str, Any]) -> dict[str, Any]:
    return YouTubeChannel.from_dict(data).to_dict(
        with_video_ids=False,
    )


def _tiktok_video_record(data: dict[str, Any]) -> dict[str, Any]:
    return TikTokVideo.model_validate(data).to_dict()


def _tiktok_creator_record(data: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = (
        TikTokCreator.model_validate(data).to_dict()
    )
    record['videos'] = []
    return record


def _tiktok_hashtag_record(data: dict[str, Any]) -> dict[str, Any]:
    return TikTokHashtag.model_validate(data).to_dict()


def _instagram_creator_record(data: dict[str, Any]) -> dict[str, Any]:
    return InstagramCreator.model_validate(data).to_dict()


def _twitch_creator_record(data: dict[str, Any]) -> dict[str, Any]:
    return TwitchCreator.model_validate(data).to_dict()


def _onlyfans_creator_record(data: dict[str, Any]) -> dict[str, Any]:
    return OnlyFansCreator.model_validate(data).to_dict()


ASSET_DESCRIPTORS: dict[tuple[str, str], AssetDescriptor] = {
    ('onlyfans', 'creator'): AssetDescriptor(
        platform='onlyfans',
        entity='creator',
        prefixes=(ONLYFANS_CREATOR_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='onlyfans-creators',
        load_record=_onlyfans_creator_record,
    ),
    ('twitch', 'creator'): AssetDescriptor(
        platform='twitch',
        entity='creator',
        prefixes=(TWITCH_CREATOR_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='twitch-creators',
        load_record=_twitch_creator_record,
    ),
    ('youtube', 'video'): AssetDescriptor(
        platform='youtube',
        entity='video',
        prefixes=(VIDEO_MIN_FILE_PREFIX, VIDEO_YTDLP_FILE_PREFIX),
        schema_owner='boinko',
        schema_version='0.0.2',
        filename_prefix='videos',
        load_record=_youtube_video_record,
    ),
    ('youtube', 'channel'): AssetDescriptor(
        platform='youtube',
        entity='channel',
        prefixes=(CHANNEL_FILE_PREFIX,),
        schema_owner='boinko',
        schema_version='0.0.2',
        filename_prefix='channels',
        load_record=_youtube_channel_record,
    ),
    ('tiktok', 'video'): AssetDescriptor(
        platform='tiktok',
        entity='video',
        prefixes=(TIKTOK_VIDEO_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='tiktok-videos',
        load_record=_tiktok_video_record,
    ),
    ('tiktok', 'creator'): AssetDescriptor(
        platform='tiktok',
        entity='creator',
        prefixes=(TIKTOK_CREATOR_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='tiktok-creators',
        load_record=_tiktok_creator_record,
    ),
    ('tiktok', 'hashtag'): AssetDescriptor(
        platform='tiktok',
        entity='hashtag',
        prefixes=(TIKTOK_HASHTAG_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='tiktok-hashtags',
        load_record=_tiktok_hashtag_record,
    ),
    ('instagram', 'creator'): AssetDescriptor(
        platform='instagram',
        entity='creator',
        prefixes=(INSTAGRAM_CREATOR_PREFIX,),
        schema_owner='drand',
        schema_version='0.0.1',
        filename_prefix='instagram-creators',
        load_record=_instagram_creator_record,
    ),
}


class ScrapeUploadSettings(ScraperSettings):
    model_config = SettingsConfigDict(
        env_file=ScraperSettings.model_config['env_file'],
        env_file_encoding='utf-8',
        cli_parse_args=False,
        populate_by_name=True,
        extra='ignore',
    )

    youtube_video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR',
            'youtube_video_data_directory',
        ),
        description='Directory containing scraped YouTube video data.',
    )
    youtube_channel_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR',
            'YOUTUBE_CHANNELS_DATA_DIR',
            'youtube_channel_data_directory',
        ),
        description='Directory containing scraped YouTube channel data.',
    )
    tiktok_video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_DATA_DIR',
            'tiktok_video_data_directory',
        ),
        description='Directory containing scraped TikTok video data.',
    )
    tiktok_creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_DATA_DIR',
            'tiktok_creator_data_directory',
        ),
        description='Directory containing scraped TikTok creator data.',
    )
    tiktok_hashtag_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_HASHTAG_DATA_DIR',
            'tiktok_hashtag_data_directory',
        ),
        description='Directory containing scraped TikTok hashtag data.',
    )
    ig_creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'IG_CREATOR_DATA_DIR',
            'ig_creator_data_directory',
        ),
        description='Directory containing scraped Instagram creator data.',
    )
    twitch_creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TWITCH_CREATOR_DATA_DIR',
            'twitch_creator_data_directory',
        ),
        description='Directory containing scraped Twitch creator data.',
    )
    onlyfans_creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'ONLYFANS_CREATOR_DATA_DIR',
            'onlyfans_creator_data_directory',
        ),
        description='Directories containing scraped OnlyFans creator data.',
    )
    youtube_channel_map_file: str = Field(
        default='channel_map.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_MAP_FILE',
            'youtube_channel_map_file',
        ),
        description='Fallback file for YouTube channel ID to handle map.',
    )
    tiktok_creator_priority_queues: str = Field(
        default='4:1000000,12:100000,24:10000,48:0',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PRIORITY_QUEUES',
            'tiktok_creator_priority_queues',
        ),
        description='TikTok creator tier spec for corrupt-file requeue.',
    )
    instagram_creator_priority_queues: str = Field(
        default='72:10000000,168:1000000,336:100000,720:10000,4320:0',
        validation_alias=AliasChoices(
            'IG_CREATOR_PRIORITY_QUEUES',
            'INSTAGRAM_CREATOR_PRIORITY_QUEUES',
            'instagram_creator_priority_queues',
        ),
        description='Instagram creator tier spec for corrupt-file requeue.',
    )
    upload_mode: UploadMode = Field(
        default='bulk',
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_MODE',
            'ASSET_UPLOAD_MODE',
            'upload_mode',
        ),
        description='Upload mode: bulk or background.',
    )
    scrape_upload_watch: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_WATCH',
            'ASSET_UPLOAD_WATCH',
            'scrape_upload_watch',
        ),
        description='Watch directories for new files after initial drain.',
    )
    scrape_upload_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_CONCURRENCY',
            'ASSET_UPLOAD_CONCURRENCY',
            'scrape_upload_concurrency',
        ),
        description='Concurrent per-file preparation tasks per directory.',
    )
    metrics_port: int = Field(
        default=9800,
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_METRICS_PORT',
            'scrape_upload_metrics_port',
            'ASSET_UPLOAD_METRICS_PORT',
            'asset_upload_metrics_port',
            'metrics_port',
        ),
        description='Port for the Prometheus metrics HTTP server.',
    )
    scrape_upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_LOG_LEVEL',
            'ASSET_UPLOAD_LOG_LEVEL',
            'scrape_upload_log_level',
            'LOG_LEVEL',
            'log_level',
        ),
        description='Logging level.',
    )
    scrape_upload_log_file: str = Field(
        default=SCRAPE_UPLOAD_DEFAULT_LOG_FILE,
        validation_alias=AliasChoices(
            'SCRAPE_UPLOAD_LOG_FILE',
            'ASSET_UPLOAD_LOG_FILE',
            'scrape_upload_log_file',
        ),
        description='File path for scrape-upload logs.',
    )
    bulk_batch_size: int = Field(
        default=1000,
        validation_alias=AliasChoices(
            'BULK_BATCH_SIZE', 'bulk_batch_size',
        ),
        description='Maximum records per bulk-upload batch.',
    )
    bulk_max_batch_bytes: int = Field(
        default=7 * 1024 ** 3,
        validation_alias=AliasChoices(
            'BULK_MAX_BATCH_BYTES', 'bulk_max_batch_bytes',
        ),
        description='Soft byte cap for one bulk-upload batch.',
    )
    bulk_progress_timeout_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'BULK_PROGRESS_TIMEOUT',
            'bulk_progress_timeout',
            'bulk_progress_timeout_seconds',
        ),
        description='Seconds to wait for bulk progress.',
    )
    max_active_bulk_jobs: int = Field(
        default=3,
        ge=1,
        validation_alias='BULK_MAX_ACTIVE_JOBS',
        description=(
            'Bulk jobs in flight per upload target: POSTed but not yet '
            'reconciled. The uploader prepares and POSTs the next batch '
            'while up to this many jobs run on the server.'
        ),
    )
    pending_count_interval_seconds: float = Field(
        default=600.0,
        gt=0,
        validation_alias='SCRAPE_UPLOAD_PENDING_COUNT_INTERVAL',
        description=(
            'Seconds between counts of files waiting to upload, which '
            'set the files_pending_upload gauge.'
        ),
    )
    background_drain_timeout_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            'BACKGROUND_DRAIN_TIMEOUT_SECONDS',
            'background_drain_timeout_seconds',
        ),
        description='Seconds to wait for background uploads on exit.',
    )

    @field_validator('scrape_upload_log_level', mode='before')
    @classmethod
    def _normalize_scrape_upload_log_level(cls, value: str) -> str:
        return normalize_log_level(value)

    @field_validator('scrape_upload_log_file', mode='before')
    @classmethod
    def _validate_scrape_upload_log_file(cls, value: str) -> str:
        if value in ('/dev/stdout', '/dev/stderr', '-'):
            raise ValueError(
                'SCRAPE_UPLOAD_LOG_FILE must be a regular log file path'
            )
        return value


@dataclass(frozen=True)
class AssetProcessingContext:
    settings: ScrapeUploadSettings
    client: ExchangeClient
    fm: AssetFileManagement
    descriptor: AssetDescriptor
    state: Any = None


class AssetProcessor:
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> Any:
        del settings
        del client
        del fm
        del descriptor
        return None

    async def prepare_record(
        self,
        data: dict[str, Any],
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
    ) -> dict[str, Any] | None:
        del filename
        del content_id
        return context.descriptor.load_record(data)

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del filename
        del content_id
        del context
        del exc
        return False

    def bulk_kwargs(
        self,
        context: AssetProcessingContext,
    ) -> dict[str, Any]:
        return {
            'id_from_filename': (
                lambda filename: content_id_from_filename(
                    filename, context.descriptor,
                )
            ),
        }

    def background_kwargs(
        self,
        context: AssetProcessingContext,
        content_id: str,
    ) -> dict[str, Any]:
        del context
        del content_id
        return {}

    async def on_success_id(
        self,
        content_id: str,
        context: AssetProcessingContext,
    ) -> None:
        del content_id
        del context


@dataclass(frozen=True)
class YouTubeChannelProcessorState:
    creator_map: CreatorMap
    name_map: NameMap
    scrape_queue: RedisChannelScrapeQueue | None
    exchange_set: RedisExchangeChannelsSet | None


class YouTubeChannelProcessor(AssetProcessor):
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> YouTubeChannelProcessorState:
        del client
        del fm
        del descriptor
        creator_map: CreatorMap
        name_map: NameMap
        redis_client: aioredis.Redis | None = None
        if settings.redis_dsn:
            creator_map = RedisCreatorMap(
                settings.redis_dsn,
                platform='youtube',
            )
            name_map = RedisNameMap(
                settings.redis_dsn,
                platform='youtube',
            )
            redis_client = creator_map.redis_client
        else:
            creator_map = FileCreatorMap(
                settings.youtube_channel_map_file,
            )
            name_map = NullNameMap()
        return YouTubeChannelProcessorState(
            creator_map=creator_map,
            name_map=name_map,
            scrape_queue=(
                RedisChannelScrapeQueue(
                    redis_client,
                    ChannelScrapeQueueSettings(),
                )
                if redis_client is not None else None
            ),
            exchange_set=(
                RedisExchangeChannelsSet(redis_client)
                if redis_client is not None else None
            ),
        )

    async def prepare_record(
        self,
        data: dict[str, Any],
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
    ) -> dict[str, Any] | None:
        del content_id
        state: YouTubeChannelProcessorState = context.state
        channel: YouTubeChannel = YouTubeChannel.from_dict(data)
        handle: str | None = channel.channel_handle
        if not channel.channel_id:
            logging.warning(
                'Channel has no channel_id, skipping upload',
                extra={
                    'filename': filename,
                    'channel_handle': handle,
                },
            )
            return None
        if handle is not None:
            await state.creator_map.put(channel.channel_id, handle)
            if channel.title:
                await state.name_map.put(
                    asset_title=channel.title,
                    asset_id=channel.channel_id,
                )
        channel.channel_handle = handle
        return channel.to_dict(with_video_ids=False)

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del exc
        state: YouTubeChannelProcessorState = context.state
        if state.scrape_queue is None:
            logging.warning(
                'Cannot reschedule corrupt channel without Redis queue',
                extra={'filename': filename, 'channel_id': content_id},
            )
            return True
        await state.scrape_queue.force_rescrape(
            f'i:{content_id}',
            mode='full',
            source='scrape_upload_corrupt_channel_file',
        )
        await context.fm.delete(filename, fail_ok=False)
        return True

    def bulk_kwargs(
        self,
        context: AssetProcessingContext,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = super().bulk_kwargs(context)
        state: YouTubeChannelProcessorState = context.state
        if state.exchange_set is not None:
            kwargs['exchange_set'] = state.exchange_set
        return kwargs


@dataclass(frozen=True)
class YouTubeVideoProcessorState:
    creator_map: CreatorMap
    scrape_queue: RedisVideoScrapeQueue | None
    uploaded: UploadedVideoIds | None


class YouTubeVideoProcessor(AssetProcessor):
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> YouTubeVideoProcessorState:
        del client
        del fm
        del descriptor
        creator_map: CreatorMap
        if settings.redis_dsn:
            creator_map = RedisCreatorMap(
                settings.redis_dsn,
                platform='youtube',
            )
            redis_client: aioredis.Redis = creator_map.redis_client
            return YouTubeVideoProcessorState(
                creator_map=creator_map,
                scrape_queue=RedisVideoScrapeQueue(
                    redis_client,
                    VideoScrapeQueueSettings(),
                ),
                uploaded=UploadedVideoIds(settings.redis_dsn),
            )
        return YouTubeVideoProcessorState(
            creator_map=FileCreatorMap(settings.youtube_channel_map_file),
            scrape_queue=None,
            uploaded=None,
        )

    async def _resolve_handle(
        self,
        video: YouTubeVideo,
        state: YouTubeVideoProcessorState,
    ) -> str | None:
        if not video.channel_id:
            if not video.channel_handle:
                return None
            try:
                return fallback_handle(video.channel_handle)
            except ValueError:
                return None
        cached: str | None = await state.creator_map.get(
            video.channel_id,
        )
        if cached:
            return cached
        try:
            resolved: str | None = await YouTubeChannel.resolve_channel_id(
                video.channel_id,
            )
        except Exception as exc:
            logging.warning(
                'Video upload handle resolution failed',
                extra={
                    'video_id': video.video_id,
                    'channel_id': video.channel_id,
                    'error': repr(exc),
                },
            )
            return None
        handle: str | None = resolved
        if handle is None:
            try:
                handle = fallback_handle(
                    video.channel_handle or video.channel_id,
                )
            except ValueError:
                return None
        await state.creator_map.put(video.channel_id, handle)
        return handle

    async def prepare_record(
        self,
        data: dict[str, Any],
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
    ) -> dict[str, Any] | None:
        state: YouTubeVideoProcessorState = context.state
        if state.uploaded is not None and await state.uploaded.contains(
            content_id,
        ):
            await context.fm.mark_uploaded(filename)
            return None
        video: YouTubeVideo = YouTubeVideo.from_dict(data)
        handle: str | None = await self._resolve_handle(video, state)
        if handle is None or any(char.isspace() for char in handle):
            await context.fm.mark_invalid(filename)
            return None
        video.channel_handle = handle
        if not video.channel_url:
            video.channel_url = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
                channel_handle=handle,
            )
        if not video.video_id:
            return None
        thumbnails: dict = getattr(video, 'thumbnails', {})
        if not thumbnails:
            await context.fm.mark_invalid(filename)
            return None
        for thumbnail in thumbnails.values():
            thumbnail_url: str | None
            if isinstance(thumbnail, dict):
                thumbnail_url = thumbnail.get('url')
            else:
                thumbnail_url = getattr(thumbnail, 'url', None)
            if not thumbnail_url:
                await context.fm.mark_invalid(filename)
                return None
        return video.to_dict()

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del exc
        state: YouTubeVideoProcessorState = context.state
        if state.scrape_queue is None:
            logging.warning(
                'Cannot reschedule corrupt video without Redis queue',
                extra={'filename': filename, 'video_id': content_id},
            )
            return True
        await state.scrape_queue.force_enqueue(
            content_id,
            source='scrape_upload_corrupt_video_file',
        )
        await context.fm.delete(filename, fail_ok=False)
        return True

    async def on_success_id(
        self,
        content_id: str,
        context: AssetProcessingContext,
    ) -> None:
        state: YouTubeVideoProcessorState = context.state
        if state.uploaded is not None:
            await state.uploaded.add(content_id)

    def background_kwargs(
        self,
        context: AssetProcessingContext,
        content_id: str,
    ) -> dict[str, Any]:
        state: YouTubeVideoProcessorState = context.state
        if state.uploaded is None:
            return {}
        return {
            'on_success': lambda: state.uploaded.add(content_id),
        }


@dataclass(frozen=True)
class TikTokCreatorProcessorState:
    queue: RedisCreatorQueue | None


class TikTokCreatorProcessor(AssetProcessor):
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> TikTokCreatorProcessorState:
        del client
        del fm
        del descriptor
        if not settings.redis_dsn:
            return TikTokCreatorProcessorState(queue=None)
        queue = RedisCreatorQueue(
            settings.redis_dsn,
            settings.worker_id or 'scrape-upload',
            platform='tiktok',
            key_namespace='scrape',
        )
        tiers = parse_priority_queues(
            settings.tiktok_creator_priority_queues,
        )
        queue._tiers = tiers
        queue._key_queues = queue._build_queue_keys(tiers)
        return TikTokCreatorProcessorState(queue=queue)

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del exc
        state: TikTokCreatorProcessorState = context.state
        if state.queue is not None:
            await state.queue.schedule_if_absent(
                content_id,
                content_id,
                0,
            )
            await context.fm.delete(filename, fail_ok=False)
        return True


@dataclass(frozen=True)
class InstagramCreatorProcessorState:
    queue: RedisCreatorQueue | None


class InstagramCreatorProcessor(AssetProcessor):
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> InstagramCreatorProcessorState:
        del client
        del fm
        del descriptor
        if not settings.redis_dsn:
            return InstagramCreatorProcessorState(queue=None)
        queue = RedisCreatorQueue(
            settings.redis_dsn,
            settings.worker_id or 'scrape-upload',
            platform='instagram',
            key_namespace='scrape',
        )
        tiers = parse_priority_queues(
            settings.instagram_creator_priority_queues,
        )
        queue._tiers = tiers
        queue._key_queues = queue._build_queue_keys(tiers)
        return InstagramCreatorProcessorState(queue=queue)

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del exc
        state: InstagramCreatorProcessorState = context.state
        if state.queue is not None:
            await state.queue.schedule_if_absent(
                content_id,
                content_id,
                0,
            )
            await context.fm.delete(filename, fail_ok=False)
        return True


@dataclass(frozen=True)
class TikTokVideoProcessorState:
    scrape_queue: RedisVideoScrapeQueue | None


class TikTokVideoProcessor(AssetProcessor):
    async def setup(
        self,
        *,
        settings: ScrapeUploadSettings,
        client: ExchangeClient,
        fm: AssetFileManagement,
        descriptor: AssetDescriptor,
    ) -> TikTokVideoProcessorState:
        del client
        del fm
        del descriptor
        if not settings.redis_dsn:
            return TikTokVideoProcessorState(scrape_queue=None)
        redis: aioredis.Redis = redis_from_url(
            settings.redis_dsn,
            component='scrape-upload-tiktok-video-queue',
            decode_responses=True,
        )
        return TikTokVideoProcessorState(
            scrape_queue=RedisVideoScrapeQueue(
                redis,
                VideoScrapeQueueSettings(),
                platform='tiktok',
            ),
        )

    async def handle_brotli_error(
        self,
        *,
        filename: str,
        content_id: str,
        context: AssetProcessingContext,
        exc: brotli.error,
    ) -> bool:
        del exc
        state: TikTokVideoProcessorState = context.state
        if state.scrape_queue is None:
            logging.warning(
                'Cannot reschedule corrupt TikTok video without Redis '
                'queue',
                extra={'filename': filename, 'video_id': content_id},
            )
            return True
        await state.scrape_queue.force_enqueue(
            content_id,
            source='scrape_upload_corrupt_video_file',
        )
        await context.fm.delete(filename, fail_ok=False)
        return True


PROCESSORS: dict[tuple[str, str], AssetProcessor] = {
    ('youtube', 'channel'): YouTubeChannelProcessor(),
    ('youtube', 'video'): YouTubeVideoProcessor(),
    ('tiktok', 'creator'): TikTokCreatorProcessor(),
    ('tiktok', 'video'): TikTokVideoProcessor(),
    ('instagram', 'creator'): InstagramCreatorProcessor(),
}


def processor_for(descriptor: AssetDescriptor) -> AssetProcessor:
    return PROCESSORS.get(
        (descriptor.platform, descriptor.entity),
        AssetProcessor(),
    )


def descriptor_for(platform: str, entity: str) -> AssetDescriptor:
    key: tuple[str, str] = (platform.lower(), entity.lower())
    try:
        return ASSET_DESCRIPTORS[key]
    except KeyError as exc:
        supported: str = ', '.join(
            f'{p}/{e}' for p, e in sorted(ASSET_DESCRIPTORS)
        )
        raise ValueError(
            f'unsupported platform/entity {platform}/{entity}; '
            f'supported: {supported}'
        ) from exc


def _split_csv(value: str) -> list[str]:
    return [
        item.strip()
        for item in value.split(',')
        if item.strip()
    ]


def _append_target_specs(
    specs: list[AssetTargetSpec],
    *,
    platform: str,
    entity: str,
    directories: str | None,
) -> None:
    if directories is None:
        return
    descriptor: AssetDescriptor = descriptor_for(platform, entity)
    for directory in _split_csv(directories):
        specs.append(AssetTargetSpec(
            descriptor=descriptor,
            directory=directory,
        ))


def configured_asset_target_specs(
    settings: ScrapeUploadSettings,
) -> list[AssetTargetSpec]:
    specs: list[AssetTargetSpec] = []
    _append_target_specs(
        specs,
        platform='youtube',
        entity='video',
        directories=settings.youtube_video_data_directory,
    )
    _append_target_specs(
        specs,
        platform='youtube',
        entity='channel',
        directories=settings.youtube_channel_data_directory,
    )
    _append_target_specs(
        specs,
        platform='tiktok',
        entity='video',
        directories=settings.tiktok_video_data_directory,
    )
    _append_target_specs(
        specs,
        platform='tiktok',
        entity='creator',
        directories=settings.tiktok_creator_data_directory,
    )
    _append_target_specs(
        specs,
        platform='tiktok',
        entity='hashtag',
        directories=settings.tiktok_hashtag_data_directory,
    )
    _append_target_specs(
        specs,
        platform='instagram',
        entity='creator',
        directories=settings.ig_creator_data_directory,
    )
    _append_target_specs(
        specs,
        platform='twitch',
        entity='creator',
        directories=settings.twitch_creator_data_directory,
    )
    _append_target_specs(
        specs,
        platform='onlyfans',
        entity='creator',
        directories=settings.onlyfans_creator_data_directory,
    )
    if not specs:
        raise ValueError(
            'no scraper data directories configured for upload'
        )
    return specs


def is_upload_file(filename: str, descriptor: AssetDescriptor) -> bool:
    if not filename.endswith(COMPRESSED_JSON_SUFFIX):
        return False
    if '.tmp' in filename:
        return False
    if AssetFileManagement.is_marker(filename):
        return False
    return any(
        filename.startswith(prefix)
        for prefix in descriptor.prefixes
    )


def content_id_from_filename(
    filename: str, descriptor: AssetDescriptor,
) -> str:
    for prefix in sorted(descriptor.prefixes, key=len, reverse=True):
        if filename.startswith(prefix):
            return filename[
                len(prefix):-len(COMPRESSED_JSON_SUFFIX)
            ]
    raise ValueError(
        f'filename does not match {descriptor.platform}/'
        f'{descriptor.entity}: {filename}'
    )


def _is_target_upload_file(
    name: str, descriptor: AssetDescriptor,
) -> bool:
    return (
        name.endswith(COMPRESSED_JSON_SUFFIX)
        and is_upload_file(name, descriptor)
    )


def iter_asset_files(
    fm: AssetFileManagement,
    descriptor: AssetDescriptor,
) -> list[str]:
    return [
        name
        for name in fm.list_base(suffix=COMPRESSED_JSON_SUFFIX)
        if _is_target_upload_file(name, descriptor)
    ]


def bulk_config(
    settings: ScrapeUploadSettings,
    descriptor: AssetDescriptor,
) -> BulkUploadConfig:
    return BulkUploadConfig(
        schema_owner=descriptor.schema_owner,
        schema_version=descriptor.schema_version,
        platform=descriptor.platform,
        entity=descriptor.entity,
        exchange_url=settings.exchange_url,
        progress_timeout_seconds=(
            settings.bulk_progress_timeout_seconds
        ),
        filename_prefix=descriptor.filename_prefix,
    )


async def prepare_asset_line(
    filename: str,
    *,
    fm: AssetFileManagement,
    descriptor: AssetDescriptor,
    validator: SchemaValidator,
    settings: ScrapeUploadSettings | None = None,
    client: ExchangeClient | None = None,
    processor: AssetProcessor | None = None,
    state: Any = None,
) -> tuple[str, str, bytes, dict[str, Any]] | None:
    content_id: str = content_id_from_filename(filename, descriptor)
    active_processor: AssetProcessor = (
        processor if processor is not None else processor_for(descriptor)
    )
    context: AssetProcessingContext = AssetProcessingContext(
        settings=settings or ScrapeUploadSettings(_env_file=None),
        client=client,
        fm=fm,
        descriptor=descriptor,
        state=state,
    )
    if fm.is_superseded(filename):
        logging.debug(
            'Asset file superseded, deleting',
            extra={'filename': filename, 'content_id': content_id},
        )
        await fm.delete(filename, fail_ok=False)
        return None

    try:
        data: dict[str, Any] = await fm.read_file(filename)
    except brotli.error as exc:
        handled: bool = await active_processor.handle_brotli_error(
            filename=filename,
            content_id=content_id,
            context=context,
            exc=exc,
        )
        if handled:
            return None
        logging.warning(
            'Failed to decompress asset file; marking invalid',
            exc_info=exc,
            extra={'filename': filename, 'content_id': content_id},
        )
        await fm.mark_invalid(filename)
        return None
    except FileNotFoundError:
        # Listed, then moved to uploaded/ or removed before the read.
        logging.debug(
            'Asset file vanished before upload',
            extra={'filename': filename, 'content_id': content_id},
        )
        return None
    except Exception as exc:
        logging.warning(
            'Failed to read asset file',
            extra={
                'filename': filename,
                'content_id': content_id,
                'error': repr(exc),
            },
        )
        return None

    try:
        record: dict[str, Any] | None = (
            await active_processor.prepare_record(
                data,
                filename=filename,
                content_id=content_id,
                context=context,
            )
        )
    except Exception as exc:
        logging.warning(
            'Failed to parse asset record; marking invalid',
            extra={
                'filename': filename,
                'content_id': content_id,
                'error': repr(exc),
            },
        )
        await fm.mark_invalid(filename)
        return None
    if record is None:
        return None

    if not await validate_upload_record(
        record,
        validator,
        fm,
        filename,
        invalid_log_message=(
            'Asset record failed schema validation, '
            'marking invalid and skipping upload'
        ),
        mark_invalid_warning='Failed to mark asset file invalid',
        log_extra={
            'platform': descriptor.platform,
            'entity': descriptor.entity,
            'content_id': content_id,
        },
    ):
        return None

    return content_id, filename, ndjson_line(record), record


async def resume_bulk_target(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
) -> None:
    descriptor: AssetDescriptor = target.descriptor
    processor: AssetProcessor = (
        target.processor if target.processor is not None
        else processor_for(descriptor)
    )
    context: AssetProcessingContext = AssetProcessingContext(
        settings=settings,
        client=client,
        fm=target.fm,
        descriptor=descriptor,
        state=target.state,
    )
    await resume_pending_bulk_uploads(
        target.fm,
        client,
        settings.exchange_url,
        poll_timeout_seconds=(
            settings.bulk_progress_timeout_seconds
        ),
        **processor.bulk_kwargs(context),
    )


def _processing(
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
) -> tuple[AssetProcessor, AssetProcessingContext]:
    processor: AssetProcessor = (
        target.processor if target.processor is not None
        else processor_for(target.descriptor)
    )
    context: AssetProcessingContext = AssetProcessingContext(
        settings=settings,
        client=client,
        fm=target.fm,
        descriptor=target.descriptor,
        state=target.state,
    )
    return processor, context


def _set_in_flight_gauge(target: AssetUploadTarget) -> None:
    METRIC_BULK_JOBS_IN_FLIGHT.labels(
        platform=target.descriptor.platform,
        scraper=SCRAPER_LABEL,
        entity=target.descriptor.entity,
        worker_id=get_worker_id(),
    ).set(len(target.upload.in_flight_job_ids))


async def _emit_batch_outcome(
    outcome: BulkBatchOutcome,
    *,
    target: AssetUploadTarget,
    processor: AssetProcessor,
    context: AssetProcessingContext,
) -> None:
    async def _on_success(content_id: str) -> None:
        await processor.on_success_id(content_id, context)

    await emit_bulk_batch_metrics(
        outcome,
        platform=target.descriptor.platform,
        scraper=SCRAPER_LABEL,
        entity=target.descriptor.entity,
        batches_counter=METRIC_UPLOAD_BATCHES,
        uploaded_counter=METRIC_BACKGROUND_UPLOADS,
        failed_counter=METRIC_UPLOADS_FAILED,
        missing_result_counter=METRIC_UPLOADS_MISSING_RESULT,
        on_success_id=_on_success,
    )


async def _next_upload_names(
    target: AssetUploadTarget, count: int,
) -> list[str]:
    '''Up to *count* upload-file names for *target* that are not in an
    in-flight batch. Returns ``[]`` once at the end of each listing
    pass; the next call starts a new pass.'''
    upload: TargetUploadState = target.upload
    if upload.listing is None:
        upload.listing = StreamingListing(
            target.fm.base_dir, chunk_size=LISTING_CHUNK_SIZE,
        )
    names: list[str] = []
    while len(names) < count:
        if not upload.pending:
            if upload.pass_ended:
                # Report the end of the pass on its own so the next
                # listing pass starts only after these names are in
                # flight or gone.
                if not names:
                    upload.pass_ended = False
                break
            chunk: list[str]
            ended: bool
            chunk, ended = await upload.listing.next_chunk()
            upload.pending.extend(
                name for name in chunk
                if _is_target_upload_file(name, target.descriptor)
            )
            upload.pass_ended = ended
            continue
        name: str = upload.pending.popleft()
        if name not in upload.in_flight_files:
            names.append(name)
    return names


async def _finalize_in_background(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
    job_id: str,
    batch_id: str,
    batch_records: list[tuple[str, str]],
) -> None:
    '''Wait for one POSTed job, apply its results and emit metrics.
    Always frees its files, even on error or cancellation.'''
    processor: AssetProcessor
    context: AssetProcessingContext
    processor, context = _processing(settings, target, client)
    files: set[str] = {filename for _cid, filename in batch_records}
    try:
        outcome: BulkBatchOutcome = await finalize_prepared_bulk_batch(
            job_id,
            batch_id,
            None,
            batch_records,
            bulk_config(settings, target.descriptor),
            client=client,
            fm=target.fm,
            **processor.bulk_kwargs(context),
        )
        await _emit_batch_outcome(
            outcome, target=target, processor=processor, context=context,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.error(
            'Bulk finalize failed',
            extra={
                'job_id': job_id,
                'batch_id': batch_id,
                'platform': target.descriptor.platform,
                'entity': target.descriptor.entity,
                'records': len(batch_records),
                'error': repr(exc),
            },
        )
        METRIC_UPLOAD_BATCHES.labels(
            platform=target.descriptor.platform,
            scraper=SCRAPER_LABEL,
            entity=target.descriptor.entity,
            mode='bulk',
            worker_id=get_worker_id(),
            outcome='finalize_error',
        ).inc()
    finally:
        target.upload.in_flight_files -= files
        target.upload.files_released = True
        task: asyncio.Task | None = asyncio.current_task()
        if task is not None:
            target.upload.in_flight_jobs.discard(task)
            target.upload.in_flight_job_ids.pop(task, None)
        _set_in_flight_gauge(target)


async def _post_batch(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
    batch_buf: bytearray,
    batch_records: list[tuple[str, str]],
) -> int:
    '''POST one batch and hand its finalize to a background task.
    Returns the number of records POSTed; 0 for an empty batch or a
    failed POST, so the round-robin does not re-post the same files
    right away (they come back on a later pass).'''
    if not batch_records:
        return 0
    processor: AssetProcessor
    context: AssetProcessingContext
    processor, context = _processing(settings, target, client)
    job_id: str
    batch_id: str
    err: BulkBatchOutcome | None
    job_id, batch_id, err = await post_prepared_bulk_batch(
        bytes(batch_buf),
        batch_records,
        bulk_config(settings, target.descriptor),
        client=client,
        fm=target.fm,
    )
    if err is not None:
        await _emit_batch_outcome(
            err, target=target, processor=processor, context=context,
        )
        return 0
    target.upload.in_flight_files.update(
        filename for _cid, filename in batch_records
    )
    task: asyncio.Task = asyncio.create_task(_finalize_in_background(
        settings=settings,
        target=target,
        client=client,
        job_id=job_id,
        batch_id=batch_id,
        batch_records=list(batch_records),
    ))
    target.upload.in_flight_jobs.add(task)
    target.upload.in_flight_job_ids[task] = job_id
    _set_in_flight_gauge(target)
    return len(batch_records)


def _orphaned_job_count(target: AssetUploadTarget) -> int:
    '''Number of ``.bulk`` state files of *target* that no in-flight
    finalize task owns: jobs whose finalize gave up (e.g. the progress
    WebSocket failed) and that only a resume can reconcile.'''
    owned: set[str] = set(target.upload.in_flight_job_ids.values())
    return sum(
        1 for state in list_bulk_states(target.fm)
        if state.job_id not in owned
    )


async def _resume_in_background(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
) -> None:
    '''Reconcile *target*'s orphaned bulk jobs via status polling.'''
    try:
        await resume_bulk_target(
            settings=settings, target=target, client=client,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.error(
            'Resuming orphaned bulk jobs failed',
            extra={
                'platform': target.descriptor.platform,
                'entity': target.descriptor.entity,
                'error': repr(exc),
            },
        )
    finally:
        upload: TargetUploadState = target.upload
        task: asyncio.Task | None = asyncio.current_task()
        if task is not None:
            upload.in_flight_jobs.discard(task)
        upload.resume_task = None


def _start_orphan_resume(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
    orphans: int,
) -> None:
    '''Start the one background resume of *target*'s orphaned jobs.
    Call only while the target has no in-flight finalize tasks: the
    resume reconciles every state file of the target.'''
    upload: TargetUploadState = target.upload
    logging.warning(
        'Orphaned bulk jobs reached the cap; resuming them',
        extra={
            'platform': target.descriptor.platform,
            'entity': target.descriptor.entity,
            'orphaned_jobs': orphans,
        },
    )
    upload.resume_attempted = True
    task: asyncio.Task = asyncio.create_task(_resume_in_background(
        settings=settings, target=target, client=client,
    ))
    upload.resume_task = task
    upload.in_flight_jobs.add(task)


async def drain_bulk_target_once(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
) -> int:
    '''Prepare and POST at most one batch for *target* without waiting
    for the server to finish it. Returns the number of records POSTed;
    0 when in-flight plus orphaned jobs already reach
    ``max_active_bulk_jobs``, the POST failed or the listing pass ended
    without eligible files. Orphaned jobs at the cap with nothing in
    flight start one background resume.'''
    upload: TargetUploadState = target.upload
    if upload.resume_task is not None:
        return 0
    cap: int = max(settings.max_active_bulk_jobs, 1)
    orphans: int = _orphaned_job_count(target)
    if len(upload.in_flight_jobs) + orphans >= cap:
        if (
            not upload.in_flight_jobs
            and orphans >= cap
            and not upload.resume_attempted
        ):
            _start_orphan_resume(
                settings=settings, target=target, client=client,
                orphans=orphans,
            )
        return 0
    processor: AssetProcessor
    processor, _context = _processing(settings, target, client)
    concurrency: int = max(settings.scrape_upload_concurrency, 1)
    batch_buf: bytearray = bytearray()
    batch_records: list[tuple[str, str]] = []
    # A pass can end right after earlier turns consumed its names, while
    # a finalize released files back to the directory (e.g. after it
    # failed). Then try one fresh pass before reporting "nothing to
    # do"; at most one per release, so files that never prepare cannot
    # loop forever and an idle target is scanned once per call.
    while True:
        names: list[str] = await _next_upload_names(target, concurrency)
        if not names:
            if batch_records or not upload.files_released:
                break
            upload.files_released = False
            continue
        prepared = await asyncio.gather(*(
            prepare_asset_line(
                filename,
                fm=target.fm,
                descriptor=target.descriptor,
                validator=target.validator,
                settings=settings,
                client=client,
                processor=processor,
                state=target.state,
            )
            for filename in names
        ))
        index: int
        for index, entry in enumerate(prepared):
            if entry is None:
                continue
            content_id, filename, line, _record = entry
            if len(line) > settings.bulk_max_batch_bytes:
                logging.warning(
                    'Asset record exceeds bulk-batch byte cap',
                    extra={
                        'filename': filename,
                        'content_id': content_id,
                        'record_bytes': len(line),
                        'max_bytes': settings.bulk_max_batch_bytes,
                    },
                )
                continue
            if batch_records and (
                len(batch_records) >= settings.bulk_batch_size
                or len(batch_buf) + len(line)
                > settings.bulk_max_batch_bytes
            ):
                # Batch full: this name and the rest of the chunk go
                # back to the front of the queue for the next turn.
                upload.pending.extendleft(reversed(names[index:]))
                return await _post_batch(
                    settings=settings, target=target, client=client,
                    batch_buf=batch_buf, batch_records=batch_records,
                )
            batch_buf.extend(line)
            batch_records.append((content_id, filename))
        if len(batch_records) >= settings.bulk_batch_size:
            break
    return await _post_batch(
        settings=settings, target=target, client=client,
        batch_buf=batch_buf, batch_records=batch_records,
    )


def in_flight_tasks(
    targets: list[AssetUploadTarget],
) -> set[asyncio.Task]:
    '''Every background finalize task of *targets*.'''
    return {
        task for target in targets for task in target.upload.in_flight_jobs
    }


async def publish_pending_counts(
    targets: list[AssetUploadTarget],
) -> None:
    '''Count each target's upload files (without keeping names), set
    ``files_pending_upload`` and log the count.'''
    target: AssetUploadTarget
    for target in targets:
        descriptor: AssetDescriptor = target.descriptor
        count: int = await asyncio.to_thread(
            count_matching_files,
            target.fm.base_dir,
            lambda name, d=descriptor: _is_target_upload_file(name, d),
        )
        METRIC_FILES_PENDING_UPLOAD.labels(
            platform=descriptor.platform,
            scraper=SCRAPER_LABEL,
            entity=descriptor.entity,
            worker_id=get_worker_id(),
        ).set(count)
        logging.info(
            'Files pending upload',
            extra={
                'platform': descriptor.platform,
                'entity': descriptor.entity,
                'count': count,
            },
        )


async def pending_count_loop(
    targets: list[AssetUploadTarget], interval_seconds: float,
) -> None:
    '''Refresh ``files_pending_upload`` every *interval_seconds*.'''
    while True:
        try:
            await publish_pending_counts(targets)
        except Exception as exc:
            logging.warning(
                'Counting files pending upload failed',
                extra={'error': repr(exc)},
            )
        await asyncio.sleep(interval_seconds)


async def drain_in_flight(
    targets: list[AssetUploadTarget], timeout_seconds: float,
) -> None:
    '''On shutdown: wait up to *timeout_seconds* for background
    finalize tasks, cancel the rest (their ``.bulk`` state files make
    the next start resume them) and close open listings.'''
    pending: set[asyncio.Task] = in_flight_tasks(targets)
    if pending:
        _done, still_running = await asyncio.wait(
            pending, timeout=timeout_seconds,
        )
        task: asyncio.Task
        for task in still_running:
            task.cancel()
        if still_running:
            await asyncio.gather(*still_running, return_exceptions=True)
    target: AssetUploadTarget
    for target in targets:
        listing: StreamingListing | None = target.upload.listing
        if listing is None:
            continue
        target.upload.listing = None
        try:
            listing.close()
        except OSError as exc:
            logging.warning(
                'Closing directory listing failed',
                extra={
                    'platform': target.descriptor.platform,
                    'entity': target.descriptor.entity,
                    'error': repr(exc),
                },
            )


async def drain_bulk_directory(
    *,
    settings: ScrapeUploadSettings,
    descriptor: AssetDescriptor,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
) -> None:
    target: AssetUploadTarget = AssetUploadTarget(
        descriptor=descriptor,
        fm=fm,
        validator=validator,
    )
    await drain_targets_round_robin(
        settings=settings,
        targets=[target],
        client=client,
    )


async def enqueue_background_asset(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
    fm: AssetFileManagement,
    content_id: str,
    source_filename: str,
    record: dict[str, Any],
) -> bool:
    descriptor: AssetDescriptor = target.descriptor
    processor: AssetProcessor = (
        target.processor if target.processor is not None
        else processor_for(descriptor)
    )
    context: AssetProcessingContext = AssetProcessingContext(
        settings=settings,
        client=client,
        fm=fm,
        descriptor=descriptor,
        state=target.state,
    )
    accepted: bool = client.enqueue_upload(
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': descriptor.schema_owner,
            'platform': descriptor.platform,
            'entity': descriptor.entity,
            'version': descriptor.schema_version,
            'source_url': record.get('url'),
            'data': record,
        },
        file_manager=fm,
        filename=source_filename,
        platform=descriptor.platform,
        entity=descriptor.entity,
        log_extra={
            'platform': descriptor.platform,
            'entity': descriptor.entity,
            'content_id': content_id,
            'filename': source_filename,
        },
        **processor.background_kwargs(context, content_id),
    )
    if not accepted:
        logging.warning(
            'Background upload queue rejected asset',
            extra={
                'filename': source_filename,
                'content_id': content_id,
            },
        )
    return accepted


async def drain_background_target_once(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    client: ExchangeClient,
    queued_files: set[tuple[str, str]] | None = None,
) -> int:
    descriptor: AssetDescriptor = target.descriptor
    fm: AssetFileManagement = target.fm
    processor: AssetProcessor = (
        target.processor if target.processor is not None
        else processor_for(descriptor)
    )
    files: list[str] = await asyncio.to_thread(
        iter_asset_files, fm, descriptor,
    )
    if queued_files is not None:
        base_dir: str = str(fm.base_dir)
        files = [
            filename for filename in files
            if (base_dir, filename) not in queued_files
        ]
    logging.info(
        'Found asset files for background upload',
        extra={
            'base_dir': str(fm.base_dir),
            'platform': descriptor.platform,
            'entity': descriptor.entity,
            'files_length': len(files),
        },
    )
    accepted_count: int = 0
    concurrency: int = max(settings.scrape_upload_concurrency, 1)
    for start in range(0, len(files), concurrency):
        chunk: list[str] = files[start:start + concurrency]
        prepared = await asyncio.gather(*(
            prepare_asset_line(
                filename,
                fm=fm,
                descriptor=descriptor,
                validator=target.validator,
                settings=settings,
                client=client,
                processor=processor,
                state=target.state,
            )
            for filename in chunk
        ))
        for entry in prepared:
            if entry is None:
                continue
            content_id, source_filename, _line, record = entry
            if await enqueue_background_asset(
                settings=settings,
                target=target,
                client=client,
                fm=fm,
                content_id=content_id,
                source_filename=source_filename,
                record=record,
            ):
                accepted_count += 1
                if queued_files is not None:
                    queued_files.add((str(fm.base_dir), source_filename))
        if accepted_count:
            return accepted_count
    return accepted_count


async def drain_background_directory(
    *,
    settings: ScrapeUploadSettings,
    descriptor: AssetDescriptor,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
) -> None:
    target: AssetUploadTarget = AssetUploadTarget(
        descriptor=descriptor,
        fm=fm,
        validator=validator,
    )
    await drain_background_target_once(
        settings=settings,
        target=target,
        client=client,
    )


async def drain_directory(
    *,
    settings: ScrapeUploadSettings,
    descriptor: AssetDescriptor,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
) -> None:
    if settings.upload_mode == 'bulk':
        await drain_bulk_directory(
            settings=settings,
            descriptor=descriptor,
            client=client,
            fm=fm,
            validator=validator,
        )
        return
    await drain_background_directory(
        settings=settings,
        descriptor=descriptor,
        client=client,
        fm=fm,
        validator=validator,
    )


async def drain_targets_round_robin(
    *,
    settings: ScrapeUploadSettings,
    targets: list[AssetUploadTarget],
    client: ExchangeClient,
    resume_bulk: bool = True,
) -> None:
    if not targets:
        raise ValueError('at least one scrape upload target is required')
    target: AssetUploadTarget
    for target in targets:
        target.upload.resume_attempted = False
    if settings.upload_mode == 'bulk' and resume_bulk:
        await asyncio.gather(*(
            resume_bulk_target(
                settings=settings,
                target=target,
                client=client,
            )
            for target in targets
        ))

    queued_files: set[tuple[str, str]] = set()
    while True:
        progress_count: int = 0
        for target in targets:
            if settings.upload_mode == 'bulk':
                progress_count += await drain_bulk_target_once(
                    settings=settings,
                    target=target,
                    client=client,
                )
                continue
            progress_count += await drain_background_target_once(
                settings=settings,
                target=target,
                client=client,
                queued_files=queued_files,
            )
        if progress_count:
            continue
        pending: set[asyncio.Task] = in_flight_tasks(targets)
        if not pending:
            return
        # Re-poll every target at least every ROUND_ROBIN_POLL_SECONDS
        # so one hanging job does not stall the others.
        await asyncio.wait(
            pending,
            timeout=ROUND_ROBIN_POLL_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )


async def watch_directory(
    *,
    settings: ScrapeUploadSettings,
    descriptor: AssetDescriptor,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
) -> None:
    base_dir: Path = fm.base_dir
    logging.info(
        'Watching asset directory',
        extra={'base_dir': str(base_dir)},
    )

    def _watch_filter(change: Change, raw_path: str) -> bool:
        if change not in (Change.added, Change.modified):
            return False
        path: Path = Path(raw_path)
        return (
            path.parent == base_dir
            and is_upload_file(path.name, descriptor)
        )

    async for changes in awatch(
        base_dir,
        watch_filter=_watch_filter,
        recursive=False,
        yield_on_timeout=True,
    ):
        if not changes:
            continue
        await drain_directory(
            settings=settings,
            descriptor=descriptor,
            client=client,
            fm=fm,
            validator=validator,
        )


async def watch_target(
    *,
    settings: ScrapeUploadSettings,
    target: AssetUploadTarget,
    targets: list[AssetUploadTarget],
    client: ExchangeClient,
    drain_lock: asyncio.Lock,
) -> None:
    base_dir: Path = target.fm.base_dir
    descriptor: AssetDescriptor = target.descriptor
    logging.info(
        'Watching asset directory',
        extra={
            'base_dir': str(base_dir),
            'platform': descriptor.platform,
            'entity': descriptor.entity,
        },
    )

    def _watch_filter(change: Change, raw_path: str) -> bool:
        if change not in (Change.added, Change.modified):
            return False
        path: Path = Path(raw_path)
        return (
            path.parent == base_dir
            and is_upload_file(path.name, descriptor)
        )

    async for changes in awatch(
        base_dir,
        watch_filter=_watch_filter,
        recursive=False,
        yield_on_timeout=True,
    ):
        if not changes:
            continue
        async with drain_lock:
            await drain_targets_round_robin(
                settings=settings,
                targets=targets,
                client=client,
                resume_bulk=False,
            )


def build_file_manager(
    directory: str,
    descriptor: AssetDescriptor,
) -> AssetFileManagement:
    if not os.path.isdir(directory):
        logging.info(
            'Creating asset data directory',
            extra={'directory': directory},
        )
        os.makedirs(directory, exist_ok=True)
    return AssetFileManagement(
        directory,
        prefix_rankings=descriptor.prefix_rankings,
    )


async def build_upload_targets(
    settings: ScrapeUploadSettings,
    client: ExchangeClient,
) -> list[AssetUploadTarget]:
    specs: list[AssetTargetSpec] = configured_asset_target_specs(
        settings,
    )
    validators: dict[tuple[str, str, str, str], SchemaValidator] = {}
    targets: list[AssetUploadTarget] = []
    for spec in specs:
        descriptor: AssetDescriptor = spec.descriptor
        schema_owner: str = descriptor.schema_owner
        schema_version: str = descriptor.schema_version
        schema_key: tuple[str, str, str, str] = (
            schema_owner,
            descriptor.platform,
            descriptor.entity,
            schema_version,
        )
        if schema_key not in validators:
            schema_dict: dict = await fetch_schema_dict(
                client,
                settings.exchange_url,
                schema_owner,
                descriptor.platform,
                descriptor.entity,
                schema_version,
            )
            validators[schema_key] = SchemaValidator(schema_dict)
        fm: AssetFileManagement = build_file_manager(
            spec.directory,
            descriptor,
        )
        processor: AssetProcessor = processor_for(descriptor)
        state: Any = await processor.setup(
            settings=settings,
            client=client,
            fm=fm,
            descriptor=descriptor,
        )
        targets.append(AssetUploadTarget(
            descriptor=descriptor,
            fm=fm,
            validator=validators[schema_key],
            processor=processor,
            state=state,
        ))
    if not targets:
        raise ValueError('at least one scrape upload target is required')
    return targets


def configure_logging(settings: ScrapeUploadSettings) -> None:
    log_path: Path = Path(settings.scrape_upload_log_file)
    if log_path.parent != Path('.'):
        log_path.parent.mkdir(parents=True, exist_ok=True)
    configure_shared_logging(
        level=settings.scrape_upload_log_level,
        filename=str(log_path),
        log_format=settings.log_format,
    )


def install_shutdown_handlers(
    loop: asyncio.AbstractEventLoop, task: asyncio.Task,
) -> list[signal.Signals]:
    '''Cancel *task* on SIGTERM / SIGINT so its ``finally`` blocks
    drain in-flight bulk jobs before exit (under ``docker stop`` the
    process is PID 1, which ignores SIGTERM without a handler).
    Returns the signals handled, for :func:`remove_shutdown_handlers`.
    '''
    installed: list[signal.Signals] = []
    sig: signal.Signals
    for sig in SHUTDOWN_SIGNALS:
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError, ValueError) as exc:
            # No signal support here (e.g. not the main thread).
            logging.warning(
                'Cannot install shutdown signal handler',
                extra={'signal': sig.name, 'error': repr(exc)},
            )
            continue
        installed.append(sig)
    return installed


def remove_shutdown_handlers(
    loop: asyncio.AbstractEventLoop, signals: list[signal.Signals],
) -> None:
    sig: signal.Signals
    for sig in signals:
        loop.remove_signal_handler(sig)


async def run(settings: ScrapeUploadSettings) -> None:
    configure_logging(settings)
    start_metrics_server(settings.metrics_port)
    if not settings.api_key_id or not settings.api_key_secret:
        raise RuntimeError(
            'API_KEY_ID/API_KEY_SECRET must be configured'
        )
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    main_task: asyncio.Task | None = asyncio.current_task()
    handled: list[signal.Signals] = (
        install_shutdown_handlers(loop, main_task)
        if main_task is not None else []
    )
    try:
        await _run_with_client(settings)
    finally:
        remove_shutdown_handlers(loop, handled)


async def _run_with_client(settings: ScrapeUploadSettings) -> None:
    client: ExchangeClient = await ExchangeClient.setup(
        settings.api_key_id,
        settings.api_key_secret,
        settings.exchange_url,
    )
    try:
        targets: list[AssetUploadTarget] = await build_upload_targets(
            settings,
            client,
        )
        count_task: asyncio.Task | None = None
        if settings.upload_mode == 'bulk':
            count_task = asyncio.create_task(pending_count_loop(
                targets, settings.pending_count_interval_seconds,
            ))
        try:
            await drain_targets_round_robin(
                settings=settings,
                targets=targets,
                client=client,
            )
            if settings.upload_mode == 'background':
                await client.drain_uploads(
                    timeout=settings.background_drain_timeout_seconds,
                )
            if settings.scrape_upload_watch:
                drain_lock: asyncio.Lock = asyncio.Lock()
                await asyncio.gather(*(
                    watch_target(
                        settings=settings,
                        target=target,
                        targets=targets,
                        client=client,
                        drain_lock=drain_lock,
                    )
                    for target in targets
                ))
        finally:
            if count_task is not None:
                count_task.cancel()
                await asyncio.gather(count_task, return_exceptions=True)
            await drain_in_flight(
                targets,
                min(
                    settings.background_drain_timeout_seconds,
                    BULK_SHUTDOWN_DRAIN_SECONDS,
                ),
            )
    finally:
        await client.aclose()


def main() -> None:
    settings: ScrapeUploadSettings = ScrapeUploadSettings()
    try:
        asyncio.run(run(settings))
    except asyncio.CancelledError:
        # SIGTERM / SIGINT: run() already drained in-flight jobs.
        logging.info('scrape_upload stopped by signal')
    except KeyboardInterrupt:
        raise
    except Exception:
        logging.exception('scrape_upload failed')
        raise SystemExit(1)


if __name__ == '__main__':
    main()
