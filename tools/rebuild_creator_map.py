#!/usr/bin/env python3

'''
One-shot tool that rebuilds the Redis creator map (hash
``youtube:creator_map``) from the locally stored channel scrape
files. Use this after a Redis wipe or when
``yt_rss_channel_map_size`` has dropped far below the on-disk
channel count.

For every ``channel-<handle>.json.br`` file found in the
``channel_data_directory`` (and its ``uploaded/`` sub-directory),
the tool extracts ``channel_id`` and derives the handle via
``fallback_handle(channel.channel_handle)`` — the same helper
``resolve_channel_upload_handle`` uses when no canonical handle is
available. Mappings are batched and flushed to Redis in groups of
500.

Read-only on disk. Defaults to dry-run: nothing is written to
Redis unless ``--no-dry-run`` is passed.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import concurrent.futures
import logging
import os
import sys

from pathlib import Path

from httpx import Response
from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import (
    ScraperSettings,
    normalize_log_level,
)
from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
)

from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
)

BATCH_SIZE: int = 50


class RebuildSettings(ScraperSettings):
    '''
    Settings for :mod:`tools.rebuild_creator_map`.

    Inherits ``redis_dsn`` and common fields from
    :class:`ScraperSettings`, reads CLI flags + env + .env in
    priority order.
    '''

    channel_data_directory: str = Field(
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR',
            'channel_data_directory',
        ),
        description=(
            'Root directory containing channel data files. '
            'The tool walks this directory recursively.'
        ),
    )
    file_glob: str = Field(
        default='*.br',
        validation_alias=AliasChoices('FILE_GLOB', 'file_glob'),
        description=(
            'rglob pattern for channel data files. Default '
            '"channel-*.json.br" matches the scraper\'s own '
            'output layout. For server-side asset storage '
            '(e.g. /mnt/scrape/data/v1/<owner>-<platform>-'
            '<entity>-<version>/<handle>/<file>.json.br) pass '
            '"*.json.br".'
        ),
    )
    platform: str = Field(
        default='youtube',
        validation_alias=AliasChoices('PLATFORM', 'platform'),
        description=(
            'Platform key used as the prefix of the Redis '
            'creator_map hash (e.g. "youtube" → '
            'youtube:creator_map).'
        ),
    )
    processes: int = Field(
        default=os.cpu_count() or 1,
        validation_alias=AliasChoices('NUM_PROCESSES', 'num_processes'),
        description=(
            'Number of worker processes to spawn. Defaults to the '
            'number of CPUs. Each worker gets its own Redis '
            'connection and processes a disjoint subset of the '
            'files.'
        ),
    )
    dry_run: bool = Field(
        default=True,
        validation_alias=AliasChoices('DRY_RUN', 'dry_run'),
        description=(
            'When True (the default), report what would be '
            'written but do not touch Redis. Pass '
            '--no-dry-run to actually commit.'
        ),
    )
    resolve_missing: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'RESOLVE_MISSING', 'resolve_missing',
        ),
        description=(
            'When True (the default), files that have a '
            'handle/name but no channel_id trigger a YouTube '
            'about-page fetch (via the proxy pool and the '
            'shared YouTubeRateLimiter) to resolve the '
            'channel_id. Results are cached per-worker so the '
            'same handle is only fetched once. Pass '
            '--no-resolve-missing to rely strictly on the '
            'channel_id stored on disk.'
        ),
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
        description='Log level for the tool.',
    )

    @field_validator('log_level', mode='before')
    @classmethod
    def _normalize_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


def _validate_settings(settings: RebuildSettings) -> None:
    if not settings.redis_dsn:
        print(
            'Error: REDIS_DSN must be provided via --redis-dsn, '
            'environment variable REDIS_DSN, or a .env file.'
        )
        sys.exit(1)
    if not Path(settings.channel_data_directory).is_dir():
        print(
            f'Error: channel_data_directory '
            f'{settings.channel_data_directory!r} is not a '
            'directory.'
        )
        sys.exit(1)


async def _scrape_channel(
    channel: YouTubeChannel, proxies: str | None,
) -> dict | None:
    '''
    Scrape YouTube's about page for *handle* and return the
    resulting channel data via ``YouTubeChannel.to_dict(
    with_video_ids=False)``. Uses :class:`AsyncYouTubeClient`,
    which automatically selects a proxy from the pool and
    honours :class:`YouTubeRateLimiter` per-proxy rate limits.

    :returns: A dict with the full about-page payload
        (``channel_id``, ``channel``, ``title``, ``description``,
        thumbnails, banners, subscriber counts, etc.) or ``None``
        on any failure.
    '''

    try:
        await channel.scrape_about_page(proxies=proxies)
    except ValueError:
        logging.warning(
            'Channel not found on YouTube',
            extra={'handle': channel.channel_handle},
        )
        return None
    except Exception as exc:
        logging.warning(
            'Failed to scrape YouTube about page for handle',
            exc=exc, extra={'handle': channel.channel_handle},
        )
        return None
    finally:
        if channel.browse_client is not None:
            await channel.browse_client.aclose()

    if not channel.channel_id:
        return None
    try:
        return channel.to_dict(with_video_ids=False)
    except Exception as exc:
        logging.warning(
            'Failed to serialize scraped channel',
            exc=exc,
            extra={'handle': channel.channel_handle},
        )
        return None


async def _extract_mapping(
    path: Path,
    known_ids: set[str],
    proxies: str | None,
    handle_cache: dict[str, dict | None],
) -> tuple[str | None, str | None, str, dict | None]:
    '''
    Read one channel file at *path* and derive a
    ``(channel_id, handle)`` pair from it.

    Resolution order:
    1. If the file's ``channel_id`` is already in *known_ids*,
       return ``'already_known'``.
    2. If the file carries a ``channel_id``, use it → ``'ok'``.
    3. Otherwise, if *proxies* is not ``None``, scrape the
       channel's about page from YouTube (cached per-worker via
       *handle_cache*). On success → ``'resolved'``, or
       ``'already_known'`` if the resolved id was already known.
    4. If resolution fails or is disabled, ``'skipped'``.

    :returns: ``(channel_id, handle, status, update_payload)``
        with status in ``{'ok', 'resolved', 'already_known',
        'skipped', 'error'}``. ``update_payload`` is non-``None``
        only for ``'resolved'`` and only when the file was in
        upload-envelope shape (top-level has a ``data`` dict);
        it holds the envelope with the freshly **scraped**
        channel data swapped into its ``data`` field, ready to
        POST back to ``/api/v1/data``.
    '''

    try:
        data: dict = await AssetFileManagement._read_path(path)
    except Exception as exc:
        logging.warning(
            'Failed to read channel file',
            exc=exc,
            extra={'path': str(path)},
        )
        return None, None, 'error', None

    # Two supported on-disk shapes:
    # 1. Channel fields at top level — the output of
    #    ``YouTubeChannel.to_dict()`` as written by the scraper.
    # 2. Upload-payload shape — the enqueue_upload_channel body,
    #    where the channel fields live under the ``data`` key.
    channel_payload: dict[str, any] = data.get('data', data)
    is_wrapped: bool = channel_payload is not data
    channel_id: str | None = channel_payload.get('channel_id')
    channel_handle: str | None = channel_payload.get(
        'channel', channel_payload.get('channel_handle')
    )

    if not channel_id:
        platform_content_id: str | None = data.get('platform_content_id')
        platform_creator_id: str | None = data.get('platform_creator_id')
        if YouTubeChannel.is_channel_id(platform_content_id):
            channel_id = platform_content_id
        else:
            if not channel_handle:
                channel_handle = platform_content_id
            if YouTubeChannel.is_channel_id(platform_creator_id):
                channel_id = platform_creator_id
                if not channel_handle:
                    channel_handle = platform_creator_id

    if not channel_handle:
        channel_handle = data.get(
            'platform_creator_id', data.get('platform_content_id')
        )
        if YouTubeChannel.is_channel_id(channel_handle):
            channel_handle = None

    if not channel_handle and not channel_id:
        logging.warning(f'Channel file missing handle and id: {path}')
        return None, None, 'skipped', None

    if ' ' in channel_handle:
        channel_handle = None

    try:
        channel: YouTubeChannel = (
            YouTubeChannel.from_dict(channel_payload)
        )
        if not channel.channel_id and channel_id:
            channel.channel_id = channel_id
        if not channel.channel_handle and channel_handle:
            channel.channel_handle = channel_handle
    except Exception as exc:
        logging.warning(
            'Failed to parse channel payload',
            exc=exc, extra={'path': str(path)},
        )
        return None, None, 'error', None

    if channel.channel_id and channel_id:
        return channel_id, channel_handle, 'ok', None

    return await _resolve_via_youtube(
        channel, data, is_wrapped, known_ids, proxies, handle_cache,
    )


async def _resolve_via_youtube(
    channel: YouTubeChannel,
    envelope: dict,
    is_wrapped: bool,
    known_ids: set[str],
    proxies: str | None,
    handle_cache: dict[str, dict | None],
) -> tuple[str | None, str | None, str, dict | None]:
    '''
    Handle the "channel_id missing on disk" path of
    :func:`_extract_mapping`: consult / populate the per-worker
    *handle_cache*, call the YouTube scraper when needed, and
    build the POST update payload for wrapped envelopes.
    '''
    if proxies is None:
        logging.warning(
            'Channel file missing channel_id and resolution '
            'disabled',
            extra={'handle': channel.channel_handle},
        )
        return None, None, 'skipped', None

    if channel.channel_handle and channel.channel_handle in handle_cache:
        scraped: dict | None = handle_cache[channel.channel_handle]
    else:
        scraped = await _scrape_channel(channel, proxies)
        handle_cache[channel.channel_handle] = scraped

    if not scraped:
        return None, None, 'skipped', None

    resolved_cid_raw = scraped.get('channel_id')
    if not resolved_cid_raw:
        return None, None, 'skipped', None
    resolved_cid: str = str(resolved_cid_raw)
    if resolved_cid in known_ids:
        return None, None, 'already_known', None
    known_ids.add(resolved_cid)

    # Build the Scrape-Exchange update payload only for wrapped
    # files — for flat files we don't have the envelope fields
    # (username, version, platform_content_id, ...) needed to
    # round-trip through POST /api/v1/data. For wrapped files,
    # swap the (possibly stale) ``data`` in the envelope for the
    # freshly scraped payload so the server record is refreshed
    # with current YouTube metadata, not just the channel_id.
    update_payload: dict | None = None
    if is_wrapped:
        envelope['data'] = scraped
        update_payload = envelope
    return resolved_cid, channel.channel_handle, 'resolved', update_payload


async def _post_channel_id_update(
    client: ExchangeClient,
    exchange_url: str,
    payload: dict,
) -> bool:
    '''
    POST *payload* (an upload envelope whose ``data.channel_id``
    has been set by the caller) back to ``/api/v1/data`` so the
    server record gets the freshly discovered channel_id.

    :returns: ``True`` on 2xx, ``False`` on any other outcome.
    '''
    url: str = f'{exchange_url}{ExchangeClient.POST_DATA_API}'
    try:
        resp: Response = await client.post(url, json=payload)
    except Exception as exc:
        logging.warning(
            'POST channel-id update failed',
            exc=exc,
            extra={
                'platform_content_id': payload.get(
                    'platform_content_id',
                ),
            },
        )
        return False
    if 200 <= resp.status_code < 300:
        return True
    logging.warning(
        'POST channel-id update returned non-2xx',
        extra={
            'status_code': resp.status_code,
            'platform_content_id': payload.get(
                'platform_content_id',
            ),
            'response_text': resp.text[:200],
        },
    )
    return False


async def _load_known_ids(
    redis_dsn: str, platform: str,
) -> set[str]:
    '''
    Snapshot the existing channel_ids in the creator_map hash
    so workers can short-circuit files whose channel is already
    known. One ``HKEYS`` per worker; cost is linear in map size.
    '''
    creator_map: RedisCreatorMap = RedisCreatorMap(
        redis_dsn, platform=platform,
    )
    try:
        keys: list[str] = (
            await creator_map._redis.hkeys(creator_map._key)
        )
    finally:
        await creator_map._redis.aclose()
    return set(keys)


async def _setup_exchange_client(
    exchange_url: str,
    api_key_id: str | None,
    api_key_secret: str | None,
) -> ExchangeClient | None:
    '''
    Build a fully-authenticated :class:`ExchangeClient` for
    posting channel-id updates. Returns ``None`` if credentials
    are missing or setup fails; the worker treats that as
    "don't post".
    '''
    if not (api_key_id and api_key_secret):
        return None
    try:
        return await ExchangeClient.setup(
            api_key_id, api_key_secret, exchange_url,
        )
    except Exception as exc:
        logging.warning(
            'ExchangeClient setup failed; channel-id '
            'updates will not be posted',
            exc=exc,
        )
        return None


async def _flush_to_redis(
    redis_dsn: str, platform: str,
    mappings: dict[str, str],
) -> None:
    '''Flush *mappings* to the Redis creator_map hash.'''
    if not mappings:
        return
    creator_map: RedisCreatorMap = RedisCreatorMap(
        redis_dsn, platform=platform,
    )
    try:
        await _flush_mappings(creator_map, mappings)
    finally:
        await creator_map._redis.aclose()


async def _process_file(
    path_str: str,
    known_ids: set[str],
    resolve_proxies: str | None,
    handle_cache: dict[str, str | None],
    mappings: dict[str, str],
    exchange_client: ExchangeClient | None,
    exchange_url: str,
    counters: dict[str, int],
) -> None:
    '''
    Dispatch one channel file: extract the mapping, record it
    in *mappings* if new, post a channel-id update when a
    resolve succeeded on a wrapped file, and bump the right
    counter in *counters*.
    '''
    cid: str | None
    handle: str | None
    status: str
    update_payload: dict | None
    (
        cid, handle, status, update_payload,
    ) = await _extract_mapping(
        Path(path_str), known_ids, resolve_proxies, handle_cache,
    )

    if status in ('ok', 'resolved') and cid and handle:
        mappings[cid] = handle
        if status == 'resolved':
            counters['resolved'] += 1
            if exchange_client and update_payload:
                ok: bool = await _post_channel_id_update(
                    exchange_client, exchange_url, update_payload,
                )
                counters[
                    'posts_ok' if ok else 'posts_failed'
                ] += 1
        return

    counters[
        status if status in ('already_known', 'skipped') else 'errors'
    ] += 1


async def _worker_run(
    work_items: list[str],
    redis_dsn: str,
    platform: str,
    dry_run: bool,
    proxies: str | None,
    resolve_missing: bool,
    exchange_url: str,
    api_key_id: str | None,
    api_key_secret: str | None,
) -> tuple[int, int, int, int, int, int, int]:
    '''
    Async body of one worker process. Reads its share of the
    channel files, builds a ``channel_id -> handle`` dict of
    only the *new* channels (those not already in the Redis
    creator_map at worker startup), and — unless dry-run —
    flushes it to Redis with its own connection.

    When *resolve_missing* is True and *proxies* is set, files
    that carry a handle but no ``channel_id`` trigger a YouTube
    about-page fetch through the shared
    :class:`YouTubeRateLimiter` (bound to *redis_dsn*) using the
    configured proxy pool. For each such resolution against an
    upload-shaped file, the envelope is POSTed back to
    ``/api/v1/data`` so the server record also picks up the new
    channel_id (skipped in dry-run).

    :returns: ``(harvested, resolved, posts_ok, posts_failed,
        already_known, skipped, errors)``. Summing across
        workers may double-count channels whose data appears in
        multiple files.
    '''

    # Bind the rate-limiter singleton to Redis and announce the
    # proxy pool so _resolve_channel_id_from_handle's internal
    # AsyncYouTubeClient can round-robin over it and honour
    # per-proxy token buckets.
    limiter: YouTubeRateLimiter = YouTubeRateLimiter.get(
        redis_dsn=redis_dsn,
    )
    if proxies:
        limiter.set_proxies(proxies)

    known_ids: set[str] = await _load_known_ids(
        redis_dsn, platform,
    )
    handle_cache: dict[str, str | None] = {}
    resolve_proxies: str | None = (
        proxies if (resolve_missing and proxies) else None
    )

    exchange_client: ExchangeClient | None = None
    if resolve_proxies is not None and not dry_run:
        exchange_client = await _setup_exchange_client(
            exchange_url, api_key_id, api_key_secret,
        )

    mappings: dict[str, str] = {}
    counters: dict[str, int] = {
        'resolved': 0, 'posts_ok': 0, 'posts_failed': 0,
        'already_known': 0, 'skipped': 0, 'errors': 0,
    }

    try:
        for path_str in work_items:
            await _process_file(
                path_str, known_ids, resolve_proxies,
                handle_cache, mappings, exchange_client,
                exchange_url, counters,
            )
        if not dry_run:
            await _flush_to_redis(
                redis_dsn, platform, mappings,
            )
    finally:
        if exchange_client is not None:
            await exchange_client.aclose()

    return (
        len(mappings),
        counters['resolved'],
        counters['posts_ok'],
        counters['posts_failed'],
        counters['already_known'],
        counters['skipped'],
        counters['errors'],
    )


def _worker_entrypoint(
    worker_id: int,
    work_items: list[str],
    redis_dsn: str,
    platform: str,
    dry_run: bool,
    log_level: str,
    log_file: str,
    log_format: str,
    proxies: str | None,
    resolve_missing: bool,
    exchange_url: str,
    api_key_id: str | None,
    api_key_secret: str | None,
) -> tuple[int, int, int, int, int, int, int]:
    '''
    Top-level (pickleable) entry point passed to
    :class:`ProcessPoolExecutor`. Configures logging for the
    child process with the same formatter the parent uses, tags
    every record with ``worker_id``, and runs :func:`_worker_run`
    in a fresh event loop.
    '''

    configure_logging(
        level=log_level,
        filename=log_file,
        log_format=log_format,
    )

    class _WorkerIdFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.worker_id = f'w{worker_id}'
            return True

    worker_filter: logging.Filter = _WorkerIdFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(worker_filter)

    return asyncio.run(_worker_run(
        work_items, redis_dsn, platform, dry_run,
        proxies, resolve_missing,
        exchange_url, api_key_id, api_key_secret,
    ))


async def _flush_mappings(
    creator_map: RedisCreatorMap, mappings: dict[str, str],
) -> int:
    '''
    Write mappings to Redis in batches. Returns the number of
    fields written (may exceed the number of entries that
    actually changed, since HSET returns only the number of new
    fields). Each batch is one ``HSET`` with a mapping.
    '''

    written: int = 0
    batch: dict[str, str] = {}
    for creator_id, handle in mappings.items():
        batch[creator_id] = handle
        if len(batch) >= BATCH_SIZE:
            await creator_map.put_many(batch)
            written += len(batch)
            logging.debug(
                'Flushed batch',
                extra={
                    'batch_size': len(batch),
                    'written_total': written,
                },
            )
            batch = {}
    if batch:
        await creator_map.put_many(batch)
        written += len(batch)
        logging.debug(
            'Flushed final batch',
            extra={
                'batch_size': len(batch),
                'written_total': written,
            },
        )
    return written


def _enumerate_work(root: Path, pattern: str) -> list[str]:
    '''
    Recursively find every file matching *pattern* under *root*
    and return their absolute paths as strings (strings pickle
    cheaper than ``Path`` objects across processes).
    '''
    return [str(p) for p in root.rglob(pattern) if p.is_file()]


async def _hash_size(dsn: str, platform: str) -> int:
    '''Return the current ``hlen`` of the creator_map hash.'''
    cm: RedisCreatorMap = RedisCreatorMap(
        dsn, platform=platform,
    )
    try:
        return await cm.size()
    finally:
        await cm._redis.aclose()


def _prefetch_exchange_jwt(
    exchange_url: str,
    api_key_id: str,
    api_key_secret: str,
) -> None:
    '''
    Fetch a JWT once in the parent process and export it into
    ``EXCHANGE_JWT`` so that every forked worker's
    :meth:`ExchangeClient.setup` returns immediately with the
    inherited token instead of issuing its own auth request.

    Mirrors the supervisor pattern in
    ``scrape_exchange.scraper_supervisor`` but for the one-shot
    rebuild tool. On failure the env var is left unset; each
    worker will then retry auth on its own (with reduced
    concurrency, since failures here usually indicate a wider
    outage rather than a stampede).
    '''
    try:
        client: ExchangeClient = asyncio.run(
            ExchangeClient.setup(
                api_key_id, api_key_secret, exchange_url,
            )
        )
    except Exception as exc:
        logging.warning(
            'JWT prefetch failed; workers will authenticate '
            'individually',
            exc=exc,
            extra={'exchange_url': exchange_url},
        )
        return
    os.environ['EXCHANGE_JWT'] = client.jwt_header
    logging.info(
        'JWT pre-fetched for workers',
        extra={'exchange_url': exchange_url},
    )


def _run(settings: RebuildSettings) -> None:
    root: Path = Path(settings.channel_data_directory)

    work_items: list[str] = _enumerate_work(
        root, settings.file_glob,
    )

    chunks: list[list[str]] = [
        work_items[i::settings.processes] for i in range(settings.processes)
    ]
    chunks = [c for c in chunks if c]
    num_workers: int = len(chunks) or 1

    logging.info(
        'Dispatching workers',
        extra={
            'num_workers': num_workers,
            'total_work_items': len(work_items),
            'dry_run': settings.dry_run,
        },
    )

    size_before: int = (
        asyncio.run(_hash_size(
            settings.redis_dsn, settings.platform,
        ))
        if not settings.dry_run else 0
    )

    # Pre-fetch a single JWT in the parent process so the forked
    # workers inherit it via EXCHANGE_JWT and each skip their own
    # auth call. Without this, N workers stampede the auth endpoint
    # concurrently and under load the default httpx connect timeout
    # can time out even after the 3 retries inside
    # ``ExchangeClient.setup``.
    if (
        settings.resolve_missing
        and not settings.dry_run
        and settings.api_key_id
        and settings.api_key_secret
    ):
        _prefetch_exchange_jwt(
            settings.exchange_url,
            settings.api_key_id,
            settings.api_key_secret,
        )

    harvested: int = 0
    resolved: int = 0
    posts_ok: int = 0
    posts_failed: int = 0
    already_known: int = 0
    skipped: int = 0
    errors: int = 0

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_workers,
    ) as pool:
        futures: list[concurrent.futures.Future] = [
            pool.submit(
                _worker_entrypoint,
                idx,
                chunk,
                settings.redis_dsn,
                settings.platform,
                settings.dry_run,
                settings.log_level,
                settings.log_file,
                settings.log_format,
                settings.proxies,
                settings.resolve_missing,
                settings.exchange_url,
                settings.api_key_id,
                settings.api_key_secret,
            )
            for idx, chunk in enumerate(chunks)
        ]
        for fut in concurrent.futures.as_completed(futures):
            h: int
            r: int
            pok: int
            pfail: int
            a: int
            s: int
            e: int
            h, r, pok, pfail, a, s, e = fut.result()
            harvested += h
            resolved += r
            posts_ok += pok
            posts_failed += pfail
            already_known += a
            skipped += s
            errors += e

    print(
        f'{num_workers} worker(s) processed '
        f'{len(work_items)} files under {root}: '
        f'{harvested} harvested (of which {resolved} '
        f'resolved via YouTube; may double-count channels '
        f'whose data appears in multiple files), '
        f'{posts_ok} channel-id updates POSTed '
        f'({posts_failed} failed), '
        f'{already_known} already in creator_map, '
        f'{skipped} skipped, {errors} errors.'
    )

    if settings.dry_run:
        print(
            f'DRY RUN: would write up to {harvested} '
            f'(channel_id, handle) pairs to Redis hash '
            f'{settings.platform}:creator_map at '
            f'{settings.redis_dsn}.'
        )
        print('Re-run with --no-dry-run to commit.')
        return

    size_after: int = asyncio.run(_hash_size(
        settings.redis_dsn, settings.platform,
    ))
    print(
        f'Hash size {size_before} -> {size_after} '
        f'({size_after - size_before:+d}).'
    )


def main() -> None:
    settings: RebuildSettings = RebuildSettings()
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )
    _validate_settings(settings)
    _run(settings)


if __name__ == '__main__':
    main()
