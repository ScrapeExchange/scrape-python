#!/usr/bin/env python3

'''
One-shot cleanup tool for the YouTube channel list file referenced
by the ``YOUTUBE_CHANNEL_LIST`` setting (default: ``channels.lst``).

What this tool does
-------------------
1. Acquires an advisory POSIX lock on ``channels.lst`` so concurrent
   runs cannot corrupt the file.
2. Parses every line with :func:`tools._channel_list_record.parse_line`
   (handles raw handles, channel IDs, YouTube URLs, legacy JSONL).
3. Deduplicates by Redis-resolved ``channel_id`` first, then by
   case-normalised handle for records not yet in Redis.  Titles are
   **never** fuzzy-matched.
4. Resolves ``channels.lst`` ↔ Redis conflicts interactively, with a
   "skip all of this type" escape hatch.
5. Classifies each record as ``scraped`` / ``new`` by probing the
   ``YOUTUBE_CHANNEL_DATA_DIR`` tree for a matching ``.json.br`` file.
6. Sorts (known ids first, then alphabetically) and atomically
   rewrites ``channels.lst`` as canonical JSONL.
7. Appends unparse-able lines to ``channels.lst.dropped``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import contextlib
import enum
import fcntl
import logging
import os
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Generator

import redis.asyncio as aioredis

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.handle_map import RedisHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from tools._channel_list_record import (
    ChannelListRecord,
    format_line,
    parse_line,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Conflict type constants
# ---------------------------------------------------------------------------

CT_LST_HANDLE_VS_REDIS: str = 'lst_handle_disagrees_with_redis'


# ---------------------------------------------------------------------------
# ConflictDecision enum
# ---------------------------------------------------------------------------

class ConflictDecision(enum.Enum):
    USE_LST = 'use_lst'
    USE_REDIS = 'use_redis'
    SKIP_ALL_OF_TYPE = 'skip_all_of_type'


PromptFn = Callable[[str, dict], ConflictDecision]


# ---------------------------------------------------------------------------
# Advisory POSIX lock
# ---------------------------------------------------------------------------

class ChannelsListLockBusy(RuntimeError):
    '''Raised when ``channels.lst`` is locked by another writer.'''


@contextlib.contextmanager
def acquire_channels_lst_lock(
    path: Path,
) -> Generator[int, None, None]:
    '''Advisory exclusive lock on a sidecar ``.lock`` file.

    Raises :class:`ChannelsListLockBusy` immediately if another
    process holds the lock — the cleanup tool is interactive, so
    blocking indefinitely is the wrong default.

    Yields the open file descriptor so callers can assert it is not
    None in tests.
    '''
    lock_path: Path = path.with_suffix(path.suffix + '.lock')
    lock_fd: int = os.open(
        str(lock_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o644,
    )
    acquired: bool = False
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            raise ChannelsListLockBusy(
                f'another writer holds {lock_path}'
            ) from exc
        yield lock_fd
    finally:
        if acquired:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        try:
            os.close(lock_fd)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------

def _casing_only_winner(a: str, b: str) -> str | None:
    '''Mirror of ``_casing_only_winner`` in
    ``tools/yt_import_channel_export.py``: return the non-lowercase
    variant when *a* and *b* differ only in casing AND exactly one of
    them is all-lowercase. Returns ``None`` for substantive
    differences or for case-ambiguous pairs (both lowercase, or both
    mixed-case but unequal).
    '''
    if a.lower() != b.lower():
        return None
    a_is_lower: bool = a == a.lower()
    b_is_lower: bool = b == b.lower()
    if a_is_lower and not b_is_lower:
        return b
    if b_is_lower and not a_is_lower:
        return a
    return None


def _join_comments(
    a: str | None, b: str | None,
) -> str | None:
    '''Concatenate two optional comment strings with ``; ``.'''
    pieces: list[str] = [c for c in (a, b) if c]
    return '; '.join(pieces) if pieces else None


def _merge_for_id(
    existing: ChannelListRecord | None,
    incoming: ChannelListRecord,
    channel_id: str,
) -> ChannelListRecord:
    '''Combine two records that map to the same channel_id.

    Prefer non-null fields; comments are concatenated.
    '''
    if existing is None:
        return ChannelListRecord(
            channel_id=channel_id,
            channel_handle=incoming.channel_handle,
            title=incoming.title,
            status=incoming.status,
            comment=incoming.comment,
        )
    return ChannelListRecord(
        channel_id=channel_id,
        channel_handle=(
            existing.channel_handle or incoming.channel_handle
        ),
        title=existing.title or incoming.title,
        status=existing.status or incoming.status,
        comment=_join_comments(
            existing.comment, incoming.comment,
        ),
    )


def _pick_canonical_casing(
    existing: ChannelListRecord | None,
    incoming: ChannelListRecord,
) -> ChannelListRecord:
    '''When two records have only-case-differing handles, prefer the
    non-lowercase variant when exactly one is all-lowercase
    (creator-intended casing). Ambiguous pairs keep the first-seen
    record. Same rule as ``yt_import_channel_export.py``.
    '''
    if existing is None:
        return incoming
    winner: str | None = _casing_only_winner(
        existing.channel_handle or '',
        incoming.channel_handle or '',
    )
    if winner == incoming.channel_handle:
        return incoming
    return existing


async def dedup_records(
    records: list[ChannelListRecord],
    *,
    store: ChannelIdentityStore,
) -> list[ChannelListRecord]:
    '''Two-pass dedup: by Redis-resolved channel_id, then by
    normalised handle.  Per CONTEXT.md: titles are never used for
    fuzzy matching.

    Pass 1 — resolve every record's channel_id from Redis (via
    handle_map for handle-only records) then group by id.

    Pass 2 — records still without a resolvable id are deduped by
    case-insensitive handle, keeping the mixed-case variant.

    Title-only records pass through unchanged.
    '''
    by_id: dict[str, ChannelListRecord] = {}
    unresolved: list[ChannelListRecord] = []

    handles_to_lookup: list[str] = [
        r.channel_handle for r in records
        if r.channel_id is None and r.channel_handle
    ]
    handle_to_id: Mapping[str, str | None] = (
        await store.handle_map.get_many(handles_to_lookup)
    )

    for record in records:
        resolved_id: str | None = (
            record.channel_id
            or (
                handle_to_id.get(record.channel_handle)
                if record.channel_handle else None
            )
        )
        if resolved_id:
            existing: ChannelListRecord | None = by_id.get(
                resolved_id,
            )
            by_id[resolved_id] = _merge_for_id(
                existing, record, resolved_id,
            )
        else:
            unresolved.append(record)

    # Second pass: dedup unresolved by normalised handle.
    by_norm: dict[str, ChannelListRecord] = {}
    title_only: list[ChannelListRecord] = []
    for record in unresolved:
        if record.channel_handle:
            norm: str = record.channel_handle.lower()
            norm_existing: ChannelListRecord | None = by_norm.get(
                norm,
            )
            by_norm[norm] = _pick_canonical_casing(
                norm_existing, record,
            )
        else:
            title_only.append(record)

    return (
        list(by_id.values())
        + list(by_norm.values())
        + title_only
    )


# ---------------------------------------------------------------------------
# Conflict resolution
# ---------------------------------------------------------------------------

async def _resolve_one(
    record: ChannelListRecord,
    *,
    store: ChannelIdentityStore,
    prompt: PromptFn,
    suppressed: set[str],
) -> ChannelListRecord:
    '''Resolve conflicts for a single record.

    Casing-only differences where exactly one side is all-lowercase
    are auto-resolved to the non-lowercase variant without a prompt
    (same rule as ``yt_import_channel_export.py``). Substantive
    handle disagreements still prompt.
    '''
    if not record.channel_id:
        return record

    redis_handle: str | None = await store.creator_map.get(
        record.channel_id,
    )
    if (
        redis_handle is None
        or record.channel_handle is None
        or redis_handle == record.channel_handle
    ):
        return record

    auto_winner: str | None = _casing_only_winner(
        redis_handle, record.channel_handle,
    )
    if auto_winner is not None:
        if auto_winner == record.channel_handle:
            return record
        return ChannelListRecord(
            channel_id=record.channel_id,
            channel_handle=auto_winner,
            title=record.title,
            status=record.status,
            comment=record.comment,
        )

    if CT_LST_HANDLE_VS_REDIS in suppressed:
        return record

    decision: ConflictDecision = prompt(
        CT_LST_HANDLE_VS_REDIS,
        {
            'channel_id': record.channel_id,
            'lst_handle': record.channel_handle,
            'redis_handle': redis_handle,
        },
    )
    if decision is ConflictDecision.USE_REDIS:
        return ChannelListRecord(
            channel_id=record.channel_id,
            channel_handle=redis_handle,
            title=record.title,
            status=record.status,
            comment=record.comment,
        )
    if decision is ConflictDecision.SKIP_ALL_OF_TYPE:
        suppressed.add(CT_LST_HANDLE_VS_REDIS)
        return record
    # ConflictDecision.USE_LST — keep record as-is.
    return record


async def resolve_conflicts(
    records: list[ChannelListRecord],
    *,
    store: ChannelIdentityStore,
    prompt: PromptFn,
) -> list[ChannelListRecord]:
    '''Walk records and resolve channels.lst ↔ Redis conflicts.

    Levels 1–4 of the source precedence are already applied before
    this point (the rehabilitator in Phase 6 cleans scraped files).
    Here we only mediate level-4 (Redis) vs level-5 (channels.lst).
    '''
    suppressed: set[str] = set()
    out: list[ChannelListRecord] = []
    for record in records:
        out.append(
            await _resolve_one(
                record,
                store=store,
                prompt=prompt,
                suppressed=suppressed,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Status classification
# ---------------------------------------------------------------------------

async def _classify_status(
    record: ChannelListRecord,
    *,
    base_dir: Path,
    uploaded_dir: Path,
) -> ChannelListRecord:
    '''Set ``record.status`` based on whether a scraped file exists.

    Probes ``base_dir`` and ``base_dir/uploaded/`` for
    ``channel-{id}.json.br`` and ``channel-{handle}.json.br``.
    '''
    candidates: list[str] = []
    if record.channel_id:
        candidates.append(
            f'channel-{record.channel_id}.json.br'
        )
    if record.channel_handle:
        candidates.append(
            f'channel-{record.channel_handle}.json.br'
        )
    for name in candidates:
        if (
            (base_dir / name).exists()
            or (uploaded_dir / name).exists()
        ):
            record.status = 'scraped'
            return record
    record.status = 'new'
    return record


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------

def _atomic_write_lines(
    path: Path, lines: list[str],
) -> None:
    '''Write *lines* to *path* atomically via mkstemp + rename.'''
    tmp_fd: int
    tmp_path_str: str
    tmp_fd, tmp_path_str = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=path.name + '.',
        suffix='.tmp',
    )
    try:
        with open(tmp_fd, 'w', encoding='utf-8') as fh:
            fh.write('\n'.join(lines) + '\n')
        Path(tmp_path_str).replace(path)
    except Exception:
        Path(tmp_path_str).unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Interactive CLI prompt
# ---------------------------------------------------------------------------

def _interactive_cli_prompt(
    conflict_type: str, payload: dict,
) -> ConflictDecision:
    '''Print conflict details and ask the operator for a decision.'''
    print(
        f'\nConflict ({conflict_type}):\n'
        f'  payload={payload}\n'
        '  [1] use channels.lst value\n'
        '  [2] use Redis value\n'
        '  [3] skip ALL conflicts of this type\n',
    )
    choice: str = input('Choose 1/2/3: ').strip()
    return {
        '1': ConflictDecision.USE_LST,
        '2': ConflictDecision.USE_REDIS,
        '3': ConflictDecision.SKIP_ALL_OF_TYPE,
    }.get(choice, ConflictDecision.USE_LST)


# ---------------------------------------------------------------------------
# Core _run (factored for testability — no pydantic-settings coupling)
# ---------------------------------------------------------------------------

async def _run(
    path: Path,
    base_dir: Path,
    store: ChannelIdentityStore,
    prompt: PromptFn = _interactive_cli_prompt,
) -> int:
    '''Execute the cleanup pipeline.

    Parameters
    ----------
    path:
        Path to ``channels.lst``.
    base_dir:
        Root channel data directory (``YOUTUBE_CHANNEL_DATA_DIR``).
    store:
        Pre-constructed :class:`ChannelIdentityStore`.
    prompt:
        Conflict resolution callback.  Defaults to the interactive
        CLI prompt; tests pass a deterministic stub.
    '''
    if not path.is_file():
        _LOGGER.error(
            'Channel list file not found',
            extra={'channel_list': str(path)},
        )
        return 1

    uploaded_dir: Path = base_dir / 'uploaded'

    with acquire_channels_lst_lock(path):
        raw_lines: list[str] = path.read_text(
            encoding='utf-8',
        ).splitlines()

        records: list[ChannelListRecord] = []
        dropped: list[tuple[str, str]] = []

        for raw in raw_lines:
            try:
                rec: ChannelListRecord | None = parse_line(raw)
            except Exception as exc:
                dropped.append((raw, f'parse_error: {exc}'))
                continue
            if rec is None:
                continue
            records.append(rec)

        deduped: list[ChannelListRecord] = await dedup_records(
            records, store=store,
        )
        resolved: list[ChannelListRecord] = await resolve_conflicts(
            deduped, store=store, prompt=prompt,
        )

        with_status: list[ChannelListRecord] = [
            await _classify_status(
                r,
                base_dir=base_dir,
                uploaded_dir=uploaded_dir,
            )
            for r in resolved
        ]
        with_status.sort(
            key=lambda r: (
                r.channel_id is None,
                r.channel_id or '',
                r.channel_handle or '',
            ),
        )

        _atomic_write_lines(
            path,
            [format_line(r) for r in with_status],
        )
        _LOGGER.info(
            'Wrote canonical JSONL',
            extra={
                'path': str(path),
                'records': len(with_status),
            },
        )

        if dropped:
            dropped_path: Path = path.with_suffix(
                path.suffix + '.dropped',
            )
            with dropped_path.open('a', encoding='utf-8') as fh:
                for raw_line, reason in dropped:
                    fh.write(f'{reason}\t{raw_line}\n')
            _LOGGER.info(
                'Appended unparseable lines to dropped sidecar',
                extra={
                    'dropped_path': str(dropped_path),
                    'count': len(dropped),
                },
            )

    return 0


# ---------------------------------------------------------------------------
# main() — constructs settings + Redis client, then delegates to _run
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    '''Entry point.  Reads settings from env / .env, then runs the
    cleanup pipeline.
    '''
    logging.basicConfig(level=logging.INFO)
    settings: YouTubeScraperSettings = YouTubeScraperSettings()
    path: Path = Path(settings.channel_list)

    if not settings.channel_data_directory:
        print(
            'YOUTUBE_CHANNEL_DATA_DIR is not set; '
            'status classification will mark all records as "new".',
            file=sys.stderr,
        )
        base_dir: Path = path.parent
    else:
        base_dir = Path(settings.channel_data_directory)

    if not settings.redis_dsn:
        print(
            'REDIS_DSN is not set; '
            'dedup will use handle-only matching only.',
            file=sys.stderr,
        )

    from scrape_exchange.handle_map import NullHandleMap
    from scrape_exchange.creator_map import NullCreatorMap

    if settings.redis_dsn:
        client: aioredis.Redis = aioredis.from_url(
            settings.redis_dsn,
            decode_responses=True,
        )
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=RedisCreatorMap(
                settings.redis_dsn, platform='youtube',
            ),
            handle_map=RedisHandleMap(
                client, platform='youtube',
            ),
        )

        async def _run_with_redis() -> int:
            try:
                return await _run(
                    path, base_dir, store,
                )
            finally:
                await client.aclose()

        return asyncio.run(_run_with_redis())
    else:
        store_null: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=NullCreatorMap(),
            handle_map=NullHandleMap(),
        )
        return asyncio.run(_run(path, base_dir, store_null))


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
