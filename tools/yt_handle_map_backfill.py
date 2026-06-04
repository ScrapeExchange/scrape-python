#!/usr/bin/env python3
'''Backfill handle_map entries from creator_map.

The creator_map stores ``channel_id -> channel_handle``. This
operator helper mirrors those bindings into handle_map through
``ChannelIdentityStore.bind()`` so the inverse invariant is
enforced while inconsistent historical rows are skipped.
'''

from __future__ import annotations

from dataclasses import dataclass

from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
    InconsistentIdentityError,
)


@dataclass(frozen=True)
class BackfillSummary:
    processed: int = 0
    skipped_invalid: int = 0
    skipped_inconsistent: int = 0


async def run_backfill(
    store: ChannelIdentityStore,
) -> BackfillSummary:
    '''Mirror creator_map into handle_map.

    ``processed`` counts successful bindings. Invalid handles and
    inconsistent duplicate bindings are skipped and counted
    separately.
    '''
    processed: int = 0
    skipped_invalid: int = 0
    skipped_inconsistent: int = 0
    creators: dict[str, str] = await store.creator_map.get_all()
    for channel_id, handle in creators.items():
        try:
            await store.bind(channel_id, handle)
        except InconsistentIdentityError:
            skipped_inconsistent += 1
            continue
        except ValueError:
            skipped_invalid += 1
            continue
        processed += 1
    return BackfillSummary(
        processed=processed,
        skipped_invalid=skipped_invalid,
        skipped_inconsistent=skipped_inconsistent,
    )
