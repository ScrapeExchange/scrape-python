#!/usr/bin/env python3

'''
TikTok creator upload tool.

Reads ``tiktok-creator-*.json.br`` files, validates them against the
TikTok creator schema, bulk uploads them to Scrape Exchange, and moves
successful files to ``uploaded/``.
'''

from __future__ import annotations

from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    parse_priority_queues,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.tiktok import TikTokCreator

from tools._tt_upload_common import (
    TikTokUploadSettings,
    RequeueCorrupt,
    run_tool,
    run_upload_worker,
    validate_common_settings,
)
from scrape_exchange.settings import normalize_log_level


CREATOR_FILE_PREFIX: str = 'tiktok-creator-'
ENTITY: str = 'creator'


class CreatorUploadSettings(TikTokUploadSettings):
    creator_upload_watch: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_UPLOAD_WATCH', 'creator_upload_watch',
        ),
        description='Watch for new TikTok creator files after drain.',
    )
    creator_upload_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_UPLOAD_CONCURRENCY',
            'creator_upload_concurrency',
        ),
        description='TikTok creator upload worker concurrency.',
    )
    creator_upload_metrics_port: int = Field(
        default=9301,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_UPLOAD_METRICS_PORT',
            'creator_upload_metrics_port',
        ),
        description='Prometheus metrics port.',
    )
    creator_upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_UPLOAD_LOG_LEVEL',
            'creator_upload_log_level',
            'TIKTOK_UPLOAD_LOG_LEVEL',
            'LOG_LEVEL',
            'log_level',
        ),
        description='Logging level.',
    )
    creator_upload_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_UPLOAD_LOG_FILE',
            'creator_upload_log_file',
            'TIKTOK_UPLOAD_LOG_FILE',
            'LOG_FILE',
            'log_file',
        ),
        description='Log file.',
    )
    creator_priority_queues: str = Field(
        default='4:1000000,12:100000,24:10000,48:0',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PRIORITY_QUEUES',
            'creator_priority_queues',
        ),
        description='Creator queue tier spec for corrupt-file requeue.',
    )

    @field_validator('creator_upload_log_level', mode='before')
    @classmethod
    def _normalize_creator_upload_log_level(cls, value: str) -> str:
        return normalize_log_level(value)


def _load_creator_record(data: dict) -> dict:
    record: dict = TikTokCreator.model_validate(data).to_dict()
    record['videos'] = []
    return record


async def _requeue_factory(
    settings: TikTokUploadSettings,
    fm: AssetFileManagement,
) -> RequeueCorrupt | None:
    del fm
    s: CreatorUploadSettings = settings  # type: ignore[assignment]
    if not s.redis_dsn:
        return None
    queue = RedisCreatorQueue(
        s.redis_dsn,
        platform='tiktok',
        worker_id=s.worker_id or 'tt-creator-upload',
    )
    tiers = parse_priority_queues(s.creator_priority_queues)
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)

    async def _requeue(username: str) -> None:
        await queue.schedule_if_absent(username, username, 0)

    return _requeue


async def _run_worker(ctx) -> None:
    settings: CreatorUploadSettings = ctx.settings
    await run_upload_worker(
        ctx,
        data_directory=settings.creator_data_directory,
        entity=ENTITY,
        prefix=CREATOR_FILE_PREFIX,
        load_record=_load_creator_record,
        requeue_corrupt_factory=_requeue_factory,
        watch=settings.creator_upload_watch,
    )


def main() -> None:
    settings = CreatorUploadSettings()
    settings.upload_log_file = settings.creator_upload_log_file
    settings.upload_log_level = settings.creator_upload_log_level
    validate_common_settings(
        settings,
        data_directory=settings.creator_data_directory,
        entity=ENTITY,
    )
    run_tool(
        settings=settings,
        entity=ENTITY,
        concurrency=settings.creator_upload_concurrency,
        metrics_port=settings.creator_upload_metrics_port,
        worker=_run_worker,
    )


if __name__ == '__main__':
    main()
