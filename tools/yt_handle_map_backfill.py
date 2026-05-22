#!/usr/bin/env python3
'''
One-shot operator script: mirror every entry in
``youtube:creator_map`` into ``youtube:handle_map`` via the paired
``bind()`` API.

Idempotent. Invalid handles (whitespace or ``/``) are skipped and
counted; the operator should fix the source before re-running.

Usage::

    uv run python -m tools.yt_handle_map_backfill
'''

import asyncio
import logging
from dataclasses import dataclass
from typing import Mapping

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.handle_map import RedisHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
    InconsistentIdentityError,
)
from scrape_exchange.youtube.settings import (
    YouTubeScraperSettings,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BackfillSummary:
    processed: int
    skipped_invalid: int
    skipped_inconsistent: int


async def run_backfill(
    store: ChannelIdentityStore,
) -> BackfillSummary:
    '''Iterate ``store.creator_map.get_all()`` and ``bind()`` each
    pair.

    Two classes of skip are tolerated:
    * ``ValueError`` — the creator_map entry's handle fails the
      hard rules (whitespace, slash, empty). Skipped and counted
      in ``skipped_invalid``.
    * :class:`InconsistentIdentityError` — ``handle_map`` already
      has this handle bound to a different ``channel_id``.
      Skipped (the existing binding wins) and counted in
      ``skipped_inconsistent``. The operator should review these
      manually; a non-zero count means two ``channel_id`` values
      both claim the same handle in Redis. Backfill is not the
      right place to resolve those.
    '''
    creator_entries: Mapping[str, str] = (
        await store.creator_map.get_all()
    )
    processed: int = 0
    skipped_invalid: int = 0
    skipped_inconsistent: int = 0
    for channel_id, handle in creator_entries.items():
        try:
            await store.bind(channel_id, handle)
            processed += 1
        except ValueError as exc:
            _LOGGER.warning(
                f'Skipping invalid handle during backfill: '
                f'channel_id={channel_id!r} '
                f'handle={handle!r} reason={exc}',
            )
            skipped_invalid += 1
        except InconsistentIdentityError as exc:
            _LOGGER.warning(
                f'Skipping inconsistent handle during '
                f'backfill: channel_id={channel_id!r} '
                f'handle={handle!r} reason={exc}',
            )
            skipped_inconsistent += 1
    return BackfillSummary(
        processed=processed,
        skipped_invalid=skipped_invalid,
        skipped_inconsistent=skipped_inconsistent,
    )


async def _main() -> int:
    settings: YouTubeScraperSettings = YouTubeScraperSettings()
    creator_map: RedisCreatorMap = RedisCreatorMap(
        settings.redis_dsn, 'youtube',
    )
    # Share the creator_map's decode_responses=True client so
    # values come back as str, not bytes.
    handle_map: RedisHandleMap = RedisHandleMap(
        creator_map.redis_client, platform='youtube',
    )
    store: ChannelIdentityStore = ChannelIdentityStore(
        creator_map=creator_map,
        handle_map=handle_map,
    )
    try:
        summary: BackfillSummary = await run_backfill(store)
    finally:
        await creator_map.redis_client.aclose()
    _LOGGER.info(
        f'handle_map backfill complete: '
        f'processed={summary.processed} '
        f'skipped_invalid={summary.skipped_invalid} '
        f'skipped_inconsistent={summary.skipped_inconsistent}',
    )
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    return asyncio.run(_main())


if __name__ == '__main__':
    raise SystemExit(main())
