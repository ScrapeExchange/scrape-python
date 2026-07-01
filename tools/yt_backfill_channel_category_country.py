#!/usr/bin/env python3

'''
One-shot YouTube category and channel-country backfill tool.

Derives missing channel categories from video categories, propagates a
channel's origin country to video payloads as ``channel_country``, and
stores category-count evidence in Redis.
'''

from __future__ import annotations

import asyncio
import logging
import sys

from collections import Counter
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.brotli import brotli_write_async
from scrape_exchange.datatypes import Platform
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
    COMPRESSED_JSON_SUFFIX,
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.schema_validator import SchemaValidator
from scrape_exchange.schema_validator import fetch_schema_dict
from scrape_exchange.scrape_api import (
    GetDataResponseModel,
    PostFilterRequestModel,
    filter_data,
    fetch_youtube_channel,
    fetch_youtube_video,
    get_data_by_param,
    iter_filter_data,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo


LOGGER: logging.Logger = logging.getLogger(__name__)

CATEGORY_THRESHOLD: int = 20
DEFAULT_MAX_VIDEOS_PER_CHANNEL: int = 200
CHANNEL_CATEGORY_COUNTS_PREFIX: str = 'youtube:channel_category_counts:'
CHANNELS_WITH_CATEGORY_COUNTS: str = 'youtube:channels_with_category_counts'
CHANNEL_SCHEMA_PATH: Path = Path(
    'tests/collateral/boinko-youtube-channel-schema.json'
)
VIDEO_SCHEMA_PATH: Path = Path(
    'tests/collateral/boinko-youtube-video-schema.json'
)

EvidenceSource = Literal['local', 'api']


@dataclass
class SelectionResult:
    category: str | None
    count: int = 0
    reason: str | None = None


@dataclass
class FieldUpdatePlan:
    should_update: bool
    value: str | None = None
    reason: str | None = None


@dataclass
class ChannelEvidence:
    channel: YouTubeChannel
    source: EvidenceSource
    filename: str | None = None
    uploaded: bool = False
    file_manager: AssetFileManagement | None = None
    api_entry: GetDataResponseModel | None = None


@dataclass
class VideoEvidence:
    video: YouTubeVideo
    source: EvidenceSource
    filename: str | None = None
    uploaded: bool = False
    file_manager: AssetFileManagement | None = None
    api_entry: GetDataResponseModel | None = None


@dataclass
class ApiVideoFetchResult:
    videos: list[VideoEvidence] = field(default_factory=list)
    api_calls: int = 0


@dataclass
class ChannelPlan:
    channel_id: str
    source: str
    country: str | None
    country_source: str | None
    country_conflict: str | None
    observed_counts: dict[str, int]
    existing_counts: dict[str, int]
    final_counts: dict[str, int]
    selected_category: SelectionResult
    category_update: FieldUpdatePlan
    channel_country_update: FieldUpdatePlan
    video_updates: list[tuple[VideoEvidence, FieldUpdatePlan]]
    failures: list[str] = field(default_factory=list)


@dataclass
class RunSummary:
    channels_seen: int = 0
    channels_planned: int = 0
    local_writes: int = 0
    redis_writes: int = 0
    remote_writes: int = 0
    failures: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.failures)


class BackfillSettings(YouTubeScraperSettings):
    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices('SCHEMA_OWNER', 'schema_owner'),
        description='Schema owner for YouTube channel/video schemas.',
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices('SCHEMA_VERSION', 'schema_version'),
        description='Schema version used for reads and writes.',
    )
    redis_dsn: str | None = Field(
        default=None,
        validation_alias=AliasChoices('REDIS_DSN', 'redis_dsn'),
        description='Redis DSN for category-count evidence.',
    )
    include_api_channels: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'INCLUDE_API_CHANNELS', 'include_api_channels',
        ),
        description='Include current-account API channel records.',
    )
    reuse_existing_data: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'REUSE_EXISTING_DATA', 'reuse_existing_data',
        ),
        description='Add observed category counts to existing Redis counts.',
    )
    apply_local: bool = Field(
        default=False,
        validation_alias=AliasChoices('APPLY_LOCAL', 'apply_local'),
        description='Write local files and Redis evidence.',
    )
    apply_remote: bool = Field(
        default=False,
        validation_alias=AliasChoices('APPLY_REMOTE', 'apply_remote'),
        description='Repost updated records to scrape.exchange.',
    )
    channel_ids: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices('CHANNEL_ID', 'channel_id'),
        description='Optional channel ids to process.',
    )
    limit_channels: int | None = Field(
        default=None,
        validation_alias=AliasChoices('LIMIT_CHANNELS', 'limit_channels'),
        description='Stop after this many channels.',
    )
    max_videos_per_channel: int = Field(
        default=DEFAULT_MAX_VIDEOS_PER_CHANNEL,
        validation_alias=AliasChoices(
            'MAX_VIDEOS_PER_CHANNEL', 'max_videos_per_channel',
        ),
        description='Maximum API videos to fetch per channel.',
    )
    overwrite_video_channel_country: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'OVERWRITE_VIDEO_CHANNEL_COUNTRY',
            'overwrite_video_channel_country',
        ),
        description='Replace conflicting video channel_country values.',
    )
    overwrite_channel_category: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'OVERWRITE_CHANNEL_CATEGORY',
            'overwrite_channel_category',
        ),
        description='Replace existing channel category values.',
    )

    @field_validator('channel_ids', mode='before')
    @classmethod
    def _parse_channel_ids(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [
                item.strip() for item in value.split(',')
                if item.strip()
            ]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [str(value).strip()]


def normalize_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized: str = ' '.join(value.split())
    return normalized or None


def count_video_categories(videos: list[YouTubeVideo]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for video in videos:
        category: str | None = normalize_category(video.category)
        if category:
            counts[category] += 1
    return dict(counts)


def top_category_counts(
    counts: dict[str, int],
    *,
    limit: int = 3,
) -> list[dict[str, int | str]]:
    ordered: list[tuple[str, int]] = sorted(
        counts.items(), key=lambda item: (-item[1], item[0])
    )
    return [
        {'category': category, 'video_count': count}
        for category, count in ordered[:limit]
    ]


def needs_api_video_evidence(
    *,
    channel: YouTubeChannel,
    local_videos: list[VideoEvidence],
    country: str | None,
    apply_remote: bool,
) -> bool:
    if apply_remote and country:
        return True
    if channel.category:
        return False
    local_counts: dict[str, int] = count_video_categories(
        [item.video for item in local_videos]
    )
    return select_channel_category(local_counts).category is None


def select_channel_category(
    counts: dict[str, int],
    threshold: int = CATEGORY_THRESHOLD,
) -> SelectionResult:
    if not counts:
        return SelectionResult(category=None, reason='no_categories')
    ordered: list[tuple[str, int]] = sorted(
        counts.items(), key=lambda item: (-item[1], item[0])
    )
    winner, winner_count = ordered[0]
    if winner_count <= threshold:
        return SelectionResult(
            category=None, count=winner_count, reason='below_threshold',
        )
    if len(ordered) > 1 and ordered[1][1] == winner_count:
        return SelectionResult(
            category=None, count=winner_count, reason='tie',
        )
    return SelectionResult(category=winner, count=winner_count)


def merge_category_counts(
    *,
    current: dict[str, int],
    observed: dict[str, int],
    reuse_existing_data: bool,
) -> dict[str, int]:
    if not reuse_existing_data:
        return dict(observed)
    merged: Counter[str] = Counter(current)
    merged.update(observed)
    return {key: value for key, value in merged.items() if value > 0}


def plan_channel_category_update(
    channel: YouTubeChannel,
    category: str | None,
    *,
    overwrite: bool = False,
) -> FieldUpdatePlan:
    if not category:
        return FieldUpdatePlan(False, reason='no_inferred_category')
    if channel.category and not overwrite:
        return FieldUpdatePlan(False, reason='existing_category')
    if channel.category == category:
        return FieldUpdatePlan(False, reason='already_set')
    return FieldUpdatePlan(True, value=category)


def plan_video_country_update(
    video: YouTubeVideo,
    country: str | None,
    *,
    overwrite: bool = False,
) -> FieldUpdatePlan:
    if not country:
        return FieldUpdatePlan(False, reason='no_channel_country')
    if video.channel_country == country:
        return FieldUpdatePlan(False, reason='already_set')
    if video.channel_country and not overwrite:
        return FieldUpdatePlan(
            False, reason='conflicting_channel_country',
        )
    return FieldUpdatePlan(True, value=country)


def plan_channel_country_update(
    channel: YouTubeChannel,
    api_country: str | None,
) -> FieldUpdatePlan:
    if channel.country:
        return FieldUpdatePlan(False, reason='already_set')
    if not api_country:
        return FieldUpdatePlan(False, reason='no_api_country')
    return FieldUpdatePlan(True, value=api_country)


def dedupe_video_evidence(
    videos: list[VideoEvidence],
) -> list[VideoEvidence]:
    by_id: dict[str, VideoEvidence] = {}
    for evidence in videos:
        video_id: str | None = evidence.video.video_id
        if not video_id:
            continue
        existing: VideoEvidence | None = by_id.get(video_id)
        if existing is None or (
            existing.source == 'api' and evidence.source == 'local'
        ):
            by_id[video_id] = evidence
    return list(by_id.values())


def _channel_source(local: bool, api: bool) -> str:
    if local and api:
        return 'local+api'
    if local:
        return 'local'
    return 'api'


def _record_failure(summary: RunSummary, message: str) -> None:
    summary.failures.append(message)
    LOGGER.warning(message)


async def _read_schema(path: Path) -> dict:
    import orjson

    return orjson.loads(path.read_bytes())


def _redis_counts_key(channel_id: str) -> str:
    return f'{CHANNEL_CATEGORY_COUNTS_PREFIX}{channel_id}'


def _is_channel_file(filename: str) -> bool:
    return (
        filename.startswith(CHANNEL_FILE_PREFIX)
        and filename.endswith(COMPRESSED_JSON_SUFFIX)
    )


def _channel_filename(channel_id: str) -> str:
    return f'{CHANNEL_FILE_PREFIX}{channel_id}{COMPRESSED_JSON_SUFFIX}'


def _video_filenames(video_id: str) -> tuple[str, str]:
    return (
        f'{VIDEO_YTDLP_FILE_PREFIX}{video_id}{COMPRESSED_JSON_SUFFIX}',
        f'{VIDEO_MIN_FILE_PREFIX}{video_id}{COMPRESSED_JSON_SUFFIX}',
    )


def _iter_filenames(
    directory: Path,
    *,
    prefix: str,
    suffix: str,
) -> Iterator[str]:
    for entry in directory.iterdir():
        filename: str = entry.name
        if not filename.startswith(prefix):
            continue
        if not filename.endswith(suffix):
            continue
        if not entry.is_file():
            continue
        yield filename


async def _read_local_channel_file(
    fm: AssetFileManagement,
    *,
    filename: str,
    uploaded: bool,
) -> ChannelEvidence | None:
    try:
        data: dict = (
            await fm.read_uploaded(filename)
            if uploaded else await fm.read_file(filename)
        )
        channel: YouTubeChannel = YouTubeChannel.from_dict(data)
    except Exception as exc:
        LOGGER.warning(
            'Failed to read local channel file',
            extra={'filename': filename, 'error': str(exc)},
        )
        return None
    if not channel.channel_id:
        return None
    return ChannelEvidence(
        channel=channel,
        source='local',
        filename=filename,
        uploaded=uploaded,
        file_manager=fm,
    )


async def load_local_channel_by_id(
    fm: AssetFileManagement,
    channel_id: str,
) -> ChannelEvidence | None:
    filename: str = _channel_filename(channel_id)
    base_path: Path = fm.base_dir / filename
    uploaded_path: Path = fm.uploaded_dir / filename
    evidence: ChannelEvidence | None = None
    if base_path.is_file():
        evidence = await _read_local_channel_file(
            fm, filename=filename, uploaded=False,
        )
    if uploaded_path.is_file():
        uploaded_evidence: ChannelEvidence | None = (
            await _read_local_channel_file(
                fm, filename=filename, uploaded=True,
            )
        )
        if uploaded_evidence is not None:
            evidence = uploaded_evidence
    return evidence


async def iter_local_channels(
    fm: AssetFileManagement,
    *,
    channel_ids: set[str] | None,
) -> AsyncIterator[ChannelEvidence]:
    if channel_ids is not None:
        for channel_id in sorted(channel_ids):
            evidence: ChannelEvidence | None = await load_local_channel_by_id(
                fm, channel_id,
            )
            if evidence is not None:
                yield evidence
        return

    for uploaded, directory in (
        (False, fm.base_dir),
        (True, fm.uploaded_dir),
    ):
        for filename in _iter_filenames(
            directory,
            prefix=CHANNEL_FILE_PREFIX,
            suffix=COMPRESSED_JSON_SUFFIX,
        ):
            if not _is_channel_file(filename):
                continue
            evidence = await _read_local_channel_file(
                fm, filename=filename, uploaded=uploaded,
            )
            if evidence is not None:
                yield evidence


async def _read_local_video_file(
    fm: AssetFileManagement,
    *,
    filename: str,
    uploaded: bool,
) -> VideoEvidence | None:
    try:
        data: dict = (
            await fm.read_uploaded(filename)
            if uploaded else await fm.read_file(filename)
        )
        video: YouTubeVideo = YouTubeVideo.from_dict(data)
    except Exception as exc:
        LOGGER.warning(
            'Failed to read local video file',
            extra={'filename': filename, 'error': str(exc)},
        )
        return None
    if not video.channel_id:
        return None
    return VideoEvidence(
        video=video,
        source='local',
        filename=filename,
        uploaded=uploaded,
        file_manager=fm,
    )


async def load_local_videos_for_channel(
    fm: AssetFileManagement | None,
    channel: YouTubeChannel,
    *,
    max_videos: int,
) -> list[VideoEvidence]:
    if fm is None or not channel.video_ids or max_videos <= 0:
        return []
    result: list[VideoEvidence] = []
    for video_id in sorted(channel.video_ids):
        if len(result) >= max_videos:
            break
        evidence: VideoEvidence | None = None
        for filename in _video_filenames(video_id):
            base_path: Path = fm.base_dir / filename
            uploaded_path: Path = fm.uploaded_dir / filename
            if base_path.is_file():
                evidence = await _read_local_video_file(
                    fm, filename=filename, uploaded=False,
                )
            if uploaded_path.is_file():
                uploaded_evidence: VideoEvidence | None = (
                    await _read_local_video_file(
                        fm, filename=filename, uploaded=True,
                    )
                )
                if uploaded_evidence is not None:
                    evidence = uploaded_evidence
            if evidence is not None:
                result.append(evidence)
                break
    return result


async def load_api_channel_by_id(
    client: ExchangeClient,
    *,
    username: str,
    schema_version: str,
    channel_id: str,
) -> ChannelEvidence | None:
    try:
        entry: GetDataResponseModel = await get_data_by_param(
            client,
            username=username,
            platform=Platform.YOUTUBE,
            entity='channel',
            version=schema_version,
            platform_content_id=channel_id,
        )
        channel: YouTubeChannel = await fetch_youtube_channel(
            client, entry,
        )
    except Exception as exc:
        LOGGER.debug(
            'Could not fetch API channel by id',
            extra={'channel_id': channel_id, 'error': str(exc)},
        )
        return None
    if not channel.channel_id:
        return None
    return ChannelEvidence(
        channel=channel,
        source='api',
        api_entry=entry,
    )


async def fetch_api_videos_for_channel(
    client: ExchangeClient,
    *,
    username: str,
    schema_version: str,
    channel_id: str,
    max_videos: int,
) -> ApiVideoFetchResult:
    filters: PostFilterRequestModel = PostFilterRequestModel(
        username=username,
        platform=Platform.YOUTUBE,
        entity='video',
        version=schema_version,
        platform_creator_id=channel_id,
        first=max(min(max_videos, 1000), 1),
    )
    result: ApiVideoFetchResult = ApiVideoFetchResult()
    while len(result.videos) < max_videos:
        result.api_calls += 1
        page = await filter_data(client, filters)
        for edge in page.edges:
            if len(result.videos) >= max_videos:
                break
            entry: GetDataResponseModel = edge.node
            result.api_calls += 1
            try:
                video: YouTubeVideo = await fetch_youtube_video(
                    client, entry,
                )
            except Exception as exc:
                LOGGER.warning(
                    'Failed to fetch API video payload',
                    extra={
                        'item_id': str(entry.item_id),
                        'error': str(exc),
                    },
                )
                continue
            result.videos.append(
                VideoEvidence(
                    video=video,
                    source='api',
                    api_entry=entry,
                )
            )
        if not page.page_info.has_next_page:
            break
        filters.after = page.page_info.end_cursor
    return result


async def iter_channel_evidence(
    *,
    channel_fm: AssetFileManagement | None,
    client: ExchangeClient | None,
    username: str | None,
    schema_version: str,
    include_api_channels: bool,
    channel_ids: set[str] | None,
) -> AsyncIterator[tuple[str, ChannelEvidence | None, ChannelEvidence | None]]:
    seen: set[str] = set()
    if channel_fm is not None:
        async for local in iter_local_channels(
            channel_fm,
            channel_ids=channel_ids,
        ):
            channel_id: str | None = local.channel.channel_id
            if not channel_id or channel_id in seen:
                continue
            seen.add(channel_id)
            api: ChannelEvidence | None = None
            if include_api_channels and client is not None and username:
                api = await load_api_channel_by_id(
                    client,
                    username=username,
                    schema_version=schema_version,
                    channel_id=channel_id,
                )
            yield channel_id, local, api

    if not include_api_channels or client is None or username is None:
        return

    if channel_ids is not None:
        for channel_id in sorted(channel_ids):
            if channel_id in seen:
                continue
            api = await load_api_channel_by_id(
                client,
                username=username,
                schema_version=schema_version,
                channel_id=channel_id,
            )
            if api is None:
                continue
            seen.add(channel_id)
            yield channel_id, None, api
        return

    filters: PostFilterRequestModel = PostFilterRequestModel(
        username=username,
        platform=Platform.YOUTUBE,
        entity='channel',
        version=schema_version,
        first=1000,
    )
    async for entry in iter_filter_data(client, filters):
        try:
            channel: YouTubeChannel = await fetch_youtube_channel(
                client, entry,
            )
        except Exception as exc:
            LOGGER.warning(
                'Failed to fetch API channel payload',
                extra={'item_id': str(entry.item_id), 'error': str(exc)},
            )
            continue
        channel_id = channel.channel_id
        if not channel_id or channel_id in seen:
            continue
        seen.add(channel_id)
        yield channel_id, None, ChannelEvidence(
            channel=channel,
            source='api',
            api_entry=entry,
        )


def build_channel_plan(
    *,
    local: ChannelEvidence | None,
    api: ChannelEvidence | None,
    videos: list[VideoEvidence],
    existing_counts: dict[str, int],
    reuse_existing_data: bool,
    overwrite_category: bool,
    overwrite_country: bool,
) -> ChannelPlan | None:
    evidence: ChannelEvidence | None = local or api
    if evidence is None or not evidence.channel.channel_id:
        return None
    channel: YouTubeChannel = evidence.channel
    api_country: str | None = api.channel.country if api else None
    country: str | None = channel.country or api_country
    country_source: str | None = None
    country_conflict: str | None = None
    if channel.country:
        country_source = 'local' if local else 'api'
    elif api_country:
        country_source = 'api'
    if channel.country and api_country and channel.country != api_country:
        country_conflict = f'local={channel.country} api={api_country}'
    country_update: FieldUpdatePlan = plan_channel_country_update(
        channel, api_country,
    )

    deduped: list[VideoEvidence] = dedupe_video_evidence(videos)
    observed_counts: dict[str, int] = count_video_categories(
        [item.video for item in deduped]
    )
    final_counts: dict[str, int] = merge_category_counts(
        current=existing_counts,
        observed=observed_counts,
        reuse_existing_data=reuse_existing_data,
    )
    selected: SelectionResult = select_channel_category(final_counts)
    category_update: FieldUpdatePlan = plan_channel_category_update(
        channel,
        selected.category,
        overwrite=overwrite_category,
    )
    video_updates: list[tuple[VideoEvidence, FieldUpdatePlan]] = [
        (
            item,
            plan_video_country_update(
                item.video,
                country,
                overwrite=overwrite_country,
            ),
        )
        for item in deduped
    ]
    return ChannelPlan(
        channel_id=channel.channel_id,
        source=_channel_source(local is not None, api is not None),
        country=country,
        country_source=country_source,
        country_conflict=country_conflict,
        observed_counts=observed_counts,
        existing_counts=existing_counts,
        final_counts=final_counts,
        selected_category=selected,
        category_update=category_update,
        channel_country_update=country_update,
        video_updates=video_updates,
    )


async def _load_redis_counts(
    redis: Any | None,
    channel_id: str,
) -> dict[str, int]:
    if redis is None:
        return {}
    raw: dict = await redis.hgetall(_redis_counts_key(channel_id))
    result: dict[str, int] = {}
    for key, value in raw.items():
        category: str = (
            key.decode('utf-8') if isinstance(key, bytes) else str(key)
        )
        count_text: str = (
            value.decode('utf-8') if isinstance(value, bytes)
            else str(value)
        )
        try:
            result[category] = int(count_text)
        except ValueError:
            continue
    return result


async def _write_redis_counts(
    redis: Any,
    channel_id: str,
    counts: dict[str, int],
) -> None:
    key: str = _redis_counts_key(channel_id)
    await redis.delete(key)
    if counts:
        await redis.hset(key, mapping=counts)
        await redis.sadd(CHANNELS_WITH_CATEGORY_COUNTS, channel_id)
    else:
        await redis.srem(CHANNELS_WITH_CATEGORY_COUNTS, channel_id)


async def _write_local_channel(
    evidence: ChannelEvidence,
    validator: SchemaValidator,
) -> None:
    if not evidence.filename:
        return
    data: dict = evidence.channel.to_dict()
    error: str | None = validator.validate(data)
    if error:
        raise ValueError(f'channel validation failed: {error}')
    fm_path: Path = evidence_fm_path(evidence)
    await brotli_write_async(fm_path, data)


def evidence_fm_path(evidence: ChannelEvidence | VideoEvidence) -> Path:
    if not evidence.filename:
        raise ValueError('local evidence is missing filename')
    if evidence.file_manager is None:
        raise ValueError('local evidence is missing file manager')
    fm: AssetFileManagement = evidence.file_manager
    directory: Path = fm.uploaded_dir if evidence.uploaded else fm.base_dir
    return directory / evidence.filename


async def _write_local_video(
    evidence: VideoEvidence,
    validator: SchemaValidator,
) -> None:
    if not evidence.filename:
        return
    data: dict = evidence.video.to_dict()
    error: str | None = validator.validate(data)
    if error:
        raise ValueError(f'video validation failed: {error}')
    await brotli_write_async(evidence_fm_path(evidence), data)


async def _post_record(
    client: ExchangeClient,
    *,
    settings: BackfillSettings,
    entity: str,
    source_url: str | None,
    data: dict,
) -> None:
    response = await client.post(
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': entity,
            'version': settings.schema_version,
            'source_url': source_url,
            'data': data,
        },
    )
    response.raise_for_status()


async def _preflight_remote_video_schema(
    client: ExchangeClient,
    settings: BackfillSettings,
) -> None:
    schema: dict = await fetch_schema_dict(
        client,
        settings.exchange_url,
        settings.schema_owner,
        'youtube',
        'video',
        settings.schema_version,
    )
    properties: dict = schema.get('properties', {})
    if 'channel_country' not in properties:
        raise RuntimeError(
            'Remote YouTube video schema does not allow channel_country'
        )


async def run(settings: BackfillSettings) -> RunSummary:
    summary: RunSummary = RunSummary()
    channel_fm: AssetFileManagement | None = None
    video_fm: AssetFileManagement | None = None

    if settings.channel_data_directory:
        channel_fm: AssetFileManagement = AssetFileManagement(
            settings.channel_data_directory
        )
    if settings.video_data_directory:
        video_fm: AssetFileManagement = AssetFileManagement(
            settings.video_data_directory
        )
    if settings.apply_local and not settings.redis_dsn:
        raise RuntimeError('REDIS_DSN is required when --apply-local is set')

    client: ExchangeClient | None = None
    username: str | None = None
    needs_api: bool = settings.include_api_channels or settings.apply_remote
    if needs_api:
        if not settings.api_key_id or not settings.api_key_secret:
            raise RuntimeError('API credentials are required for API reads')
        client = await ExchangeClient.setup(
            settings.api_key_id,
            settings.api_key_secret,
            settings.exchange_url,
        )
        username = client.authenticated_username
        if not username:
            raise RuntimeError('Authenticated API username is unavailable')
    if settings.apply_remote and client is not None:
        await _preflight_remote_video_schema(client, settings)

    redis: Any | None = None
    if settings.redis_dsn:
        redis = redis_from_url(
            settings.redis_dsn,
            component='yt-backfill-category-country',
        )

    channel_ids: set[str] | None = None
    if settings.channel_ids:
        channel_ids = set(settings.channel_ids)

    channel_schema: dict = await _read_schema(CHANNEL_SCHEMA_PATH)
    video_schema: dict = await _read_schema(VIDEO_SCHEMA_PATH)
    channel_validator: SchemaValidator = SchemaValidator(channel_schema)
    video_validator: SchemaValidator = SchemaValidator(video_schema)

    try:
        async for channel_id, local_channel, api_channel in (
            iter_channel_evidence(
                channel_fm=channel_fm,
                client=client,
                username=username,
                schema_version=settings.schema_version,
                include_api_channels=settings.include_api_channels,
                channel_ids=channel_ids,
            )
        ):
            if (
                settings.limit_channels is not None
                and summary.channels_seen >= settings.limit_channels
            ):
                break
            summary.channels_seen += 1
            api_result: ApiVideoFetchResult = ApiVideoFetchResult()
            channel: YouTubeChannel | None = (
                local_channel.channel if local_channel else (
                    api_channel.channel if api_channel else None
                )
            )
            local_channel_videos: list[VideoEvidence] = []
            if channel is not None:
                local_channel_videos = await load_local_videos_for_channel(
                    video_fm,
                    channel,
                    max_videos=settings.max_videos_per_channel,
                )
                country: str | None = (
                    channel.country
                    or (
                        api_channel.channel.country
                        if api_channel else None
                    )
                )
                if (
                    client is not None
                    and username is not None
                    and needs_api_video_evidence(
                        channel=channel,
                        local_videos=local_channel_videos,
                        country=country,
                        apply_remote=settings.apply_remote,
                    )
                ):
                    api_result = await fetch_api_videos_for_channel(
                        client,
                        username=username,
                        schema_version=settings.schema_version,
                        channel_id=channel_id,
                        max_videos=settings.max_videos_per_channel,
                    )
            existing_counts: dict[str, int] = await _load_redis_counts(
                redis, channel_id,
            )
            plan: ChannelPlan | None = build_channel_plan(
                local=local_channel,
                api=api_channel,
                videos=(
                    local_channel_videos + api_result.videos
                ),
                existing_counts=existing_counts,
                reuse_existing_data=settings.reuse_existing_data,
                overwrite_category=settings.overwrite_channel_category,
                overwrite_country=settings.overwrite_video_channel_country,
            )
            if plan is None:
                continue
            summary.channels_planned += 1
            log_channel_category_decision(
                plan,
                local_video_files_read=len(local_channel_videos),
                api_calls=api_result.api_calls,
            )
            print_channel_plan(plan, apply_local=settings.apply_local,
                               apply_remote=settings.apply_remote)
            if settings.apply_local and redis is not None:
                await _write_redis_counts(redis, channel_id,
                                          plan.final_counts)
                summary.redis_writes += 1
            channel_should_update: bool = (
                plan.category_update.should_update
                or plan.channel_country_update.should_update
            )
            if channel_should_update:
                evidence: ChannelEvidence | None = (
                    local_channel or api_channel
                )
                if evidence:
                    if plan.category_update.should_update:
                        evidence.channel.category = (
                            plan.category_update.value
                        )
                    if plan.channel_country_update.should_update:
                        evidence.channel.country = (
                            plan.channel_country_update.value
                        )
                    if settings.apply_local and evidence.source == 'local':
                        try:
                            await _write_local_channel(
                                evidence, channel_validator,
                            )
                            summary.local_writes += 1
                        except Exception as exc:
                            _record_failure(
                                summary,
                                f'{channel_id}: local channel write: {exc}',
                            )
                    if settings.apply_remote and client is not None:
                        try:
                            data = evidence.channel.to_dict()
                            error = channel_validator.validate(data)
                            if error:
                                raise ValueError(error)
                            await _post_record(
                                client,
                                settings=settings,
                                entity='channel',
                                source_url=evidence.channel.url,
                                data=data,
                            )
                            summary.remote_writes += 1
                        except Exception as exc:
                            _record_failure(
                                summary,
                                f'{channel_id}: remote channel write: {exc}',
                            )
            for video_evidence, update in plan.video_updates:
                if not update.should_update:
                    continue
                video_evidence.video.channel_country = update.value
                if settings.apply_local and video_evidence.source == 'local':
                    try:
                        await _write_local_video(
                            video_evidence, video_validator,
                        )
                        summary.local_writes += 1
                    except Exception as exc:
                        _record_failure(
                            summary,
                            f'{channel_id}: local video write: {exc}',
                        )
                if settings.apply_remote and client is not None:
                    try:
                        data = video_evidence.video.to_dict()
                        error = video_validator.validate(data)
                        if error:
                            raise ValueError(error)
                        await _post_record(
                            client,
                            settings=settings,
                            entity='video',
                            source_url=video_evidence.video.url,
                            data=data,
                        )
                        summary.remote_writes += 1
                    except Exception as exc:
                        _record_failure(
                            summary,
                            f'{channel_id}: remote video write: {exc}',
                        )
    finally:
        if redis is not None:
            await redis.aclose()
        if client is not None:
            await client.aclose()
    return summary


def configure_backfill_logging(settings: BackfillSettings) -> None:
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )


def log_channel_category_decision(
    plan: ChannelPlan,
    *,
    local_video_files_read: int,
    api_calls: int,
) -> None:
    category: str | None = plan.selected_category.category
    LOGGER.info(
        'Decided YouTube channel category',
        extra={
            'channel_id': plan.channel_id,
            'category': category,
            'category_decision_reason': plan.selected_category.reason,
            'category_video_count': plan.selected_category.count,
            'local_video_files_read': local_video_files_read,
            'api_calls': api_calls,
            'videos_evaluated': sum(plan.observed_counts.values()),
            'top_video_categories': top_category_counts(
                plan.observed_counts
            ),
        },
    )


def print_channel_plan(
    plan: ChannelPlan,
    *,
    apply_local: bool,
    apply_remote: bool,
) -> None:
    category: str = plan.selected_category.category or (
        f'unresolved:{plan.selected_category.reason}'
    )
    print(
        f'channel={plan.channel_id} source={plan.source} '
        f'country={plan.country or "-"} country_source='
        f'{plan.country_source or "-"} category={category} '
        f'category_update={plan.category_update.should_update} '
        f'channel_country_update='
        f'{plan.channel_country_update.should_update} '
        f'videos_to_update='
        f'{sum(1 for _, update in plan.video_updates if update.should_update)} '
        f'apply_local={apply_local} apply_remote={apply_remote}'
    )
    if plan.country_conflict:
        print(f'  country_conflict={plan.country_conflict}')
    if plan.observed_counts:
        print(f'  observed_counts={plan.observed_counts}')
    if plan.existing_counts:
        print(f'  existing_counts={plan.existing_counts}')
    if plan.final_counts:
        print(f'  final_counts={plan.final_counts}')


async def async_main() -> int:
    settings: BackfillSettings = BackfillSettings()
    configure_backfill_logging(settings)
    LOGGER.info(
        'Starting YouTube category/country backfill',
        extra={
            'apply_local': settings.apply_local,
            'apply_remote': settings.apply_remote,
            'include_api_channels': settings.include_api_channels,
            'limit_channels': settings.limit_channels,
            'max_videos_per_channel': settings.max_videos_per_channel,
            'reuse_existing_data': settings.reuse_existing_data,
        },
    )
    summary: RunSummary = await run(settings)
    LOGGER.info(
        'Finished YouTube category/country backfill',
        extra={
            'channels_seen': summary.channels_seen,
            'channels_planned': summary.channels_planned,
            'local_writes': summary.local_writes,
            'redis_writes': summary.redis_writes,
            'remote_writes': summary.remote_writes,
            'failures': len(summary.failures),
        },
    )
    print(
        f'summary channels_seen={summary.channels_seen} '
        f'channels_planned={summary.channels_planned} '
        f'local_writes={summary.local_writes} '
        f'redis_writes={summary.redis_writes} '
        f'remote_writes={summary.remote_writes} '
        f'failures={len(summary.failures)}'
    )
    for failure in summary.failures:
        print(f'failure: {failure}', file=sys.stderr)
    return 1 if summary.failed and (
        settings.apply_local or settings.apply_remote
    ) else 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == '__main__':
    main()
