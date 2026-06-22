#!/usr/bin/env python3

'''
TikTok video upload tool.

Reads ``tiktok-video-*.json.br`` files, validates them against the
TikTok video schema, bulk uploads them to Scrape Exchange, and moves
successful files to ``uploaded/``.
'''

from __future__ import annotations

import logging

from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.tiktok import TikTokVideo

from tools._tt_upload_common import (
    TikTokUploadSettings,
    RequeueCorrupt,
    run_tool,
    run_upload_worker,
    validate_common_settings,
)


VIDEO_FILE_PREFIX: str = 'tiktok-video-'
ENTITY: str = 'video'


class VideoUploadSettings(TikTokUploadSettings):
    video_upload_watch: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_UPLOAD_WATCH', 'video_upload_watch',
        ),
        description='Watch for new TikTok video files after drain.',
    )
    video_upload_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_UPLOAD_CONCURRENCY',
            'video_upload_concurrency',
        ),
        description='TikTok video upload worker concurrency.',
    )
    video_upload_metrics_port: int = Field(
        default=9701,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_UPLOAD_METRICS_PORT',
            'video_upload_metrics_port',
        ),
        description='Prometheus metrics port.',
    )
    video_upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_UPLOAD_LOG_LEVEL',
            'video_upload_log_level',
            'TIKTOK_UPLOAD_LOG_LEVEL',
            'LOG_LEVEL',
            'log_level',
        ),
        description='Logging level.',
    )
    video_upload_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_UPLOAD_LOG_FILE',
            'video_upload_log_file',
            'TIKTOK_UPLOAD_LOG_FILE',
            'LOG_FILE',
            'log_file',
        ),
        description='Log file.',
    )

    @field_validator('video_upload_log_level', mode='before')
    @classmethod
    def _normalize_video_upload_log_level(cls, value: str) -> str:
        return normalize_log_level(value)


def _load_video_record(data: dict) -> dict:
    return TikTokVideo.model_validate(data).to_dict()


async def _requeue_factory(
    settings: TikTokUploadSettings,
    fm: AssetFileManagement,
) -> RequeueCorrupt | None:
    del fm
    del settings
    # The TikTok video scrape queue currently stores full video URLs. A
    # corrupt tiktok-video-<id>.json.br filename only preserves the ID, so
    # enqueueing it would poison the scraper. Returning None keeps the file
    # in place for operator handling until queue entries support ID-only refs
    # or the uploader has a URL sidecar to recover from.
    logging.warning(
        'TikTok corrupt-video requeue disabled: filename has no URL',
    )
    return None


async def _run_worker(ctx) -> None:
    settings: VideoUploadSettings = ctx.settings
    await run_upload_worker(
        ctx,
        data_directory=settings.video_data_directory,
        entity=ENTITY,
        prefix=VIDEO_FILE_PREFIX,
        load_record=_load_video_record,
        requeue_corrupt_factory=_requeue_factory,
        watch=settings.video_upload_watch,
    )


def main() -> None:
    settings = VideoUploadSettings()
    settings.upload_log_file = settings.video_upload_log_file
    settings.upload_log_level = settings.video_upload_log_level
    validate_common_settings(
        settings,
        data_directory=settings.video_data_directory,
        entity=ENTITY,
    )
    run_tool(
        settings=settings,
        entity=ENTITY,
        concurrency=settings.video_upload_concurrency,
        metrics_port=settings.video_upload_metrics_port,
        worker=_run_worker,
    )


if __name__ == '__main__':
    main()
