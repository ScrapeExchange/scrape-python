'''Video scrape queue backed by backlog-server.

backlog-server (``backlog-server/`` in this repository) keeps every
known video ID on the local filesystem of one host and speaks the
Redis protocol with its own ``BACKLOG.*`` commands, so this client
talks to it through a normal ``redis-py`` connection.

Compared with :class:`RedisVideoScrapeQueue` the server keeps no
per-video context (source, channel fields) and no free-text
diagnostics: a terminal record holds a state and a reason code only.
Popped IDs are leased; ``complete`` or ``mark`` resolves the lease and
``bump_attempts`` extends it while the scraper retries.
'''

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.scraper_metrics import (
    METRIC_BACKLOG_UPLOADED_API_FALLBACK,
)
from scrape_exchange.video_scrape_queue import (
    ENQUEUE_MANY_CHUNK_SIZE,
    VideoQueueChannelContext,
    VideoScrapeQueue,
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
)
from scrape_exchange.youtube.exchange_video_exists import (
    exchange_video_exists,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

DEFAULT_QUEUE_NAME: str = 'youtube:video'
DEFAULT_EXCHANGE_URL: str = 'https://scrape.exchange'

# Reason codes the server stores; anything else is stored as 'other'.
REASON_CODES: frozenset[str] = frozenset({
    'none', 'rate_limit', 'transient', 'unavailable', 'premiere',
    'bind_failed', 'bot_detection', 'rate_limit_wait',
    'retries_exhausted', 'operator', 'other',
})

_RETRIES_EXHAUSTED_PREFIX: str = 'transient retries exhausted'


class BacklogServerUnsupported(NotImplementedError):
    '''The operation needs per-video metadata the server does not keep.'''


def normalize_reason(last_error: str | None) -> str:
    '''Map a scraper error string onto a backlog-server reason code.'''
    if not last_error:
        return 'none'
    if last_error.startswith(_RETRIES_EXHAUSTED_PREFIX):
        return 'retries_exhausted'
    if last_error in REASON_CODES:
        return last_error
    return 'other'


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


_DONE_STATES: frozenset[str] = frozenset({'scraped', 'uploaded'})


def _state(value: Any) -> VideoState | None:
    '''Server state name -> VideoState.

    ``scraped`` and ``uploaded`` are the done states and map to None,
    as a completed video is unknown to the Redis queue too; use
    :meth:`BacklogServerVideoScrapeQueue.get_state_names` when the
    difference matters. Any other unrecognised name is a deploy-order
    mistake or a typo: it raises, so it fails loudly instead of
    looking like an unknown video and turning into duplicate work.
    '''
    if value is None:
        return None
    name: str = _text(value)
    if name in _DONE_STATES:
        return None
    try:
        return VideoState(name)
    except ValueError:
        _LOGGER.error(
            'unknown video queue state name from backlog-server',
            extra={'state': name},
        )
        raise ValueError(
            f'unknown video queue state: {name!r}',
        ) from None


class BacklogServerVideoScrapeQueue(VideoScrapeQueue):
    ''':class:`VideoScrapeQueue` on top of backlog-server.

    :param redis: a ``redis.asyncio`` client connected to the server
    :param settings: queue settings (batch sizes, retry timing)
    :param queue_name: the server-side queue, e.g. ``youtube:video``
    '''

    def __init__(
        self,
        redis: aioredis.Redis,
        settings: VideoScrapeQueueSettings,
        queue_name: str = DEFAULT_QUEUE_NAME,
        *,
        supports_uploaded: bool = False,
        exchange_client: ExchangeClient | None = None,
        exchange_url: str = DEFAULT_EXCHANGE_URL,
    ) -> None:
        self._redis: aioredis.Redis = redis
        self._settings: VideoScrapeQueueSettings = settings
        self._queue: str = queue_name
        self.supports_uploaded: bool = supports_uploaded
        self._exchange_client: ExchangeClient | None = exchange_client
        self._exchange_url: str = exchange_url
        self._fallback_logged: bool = False
        # IDs handed out by pop_entries with the force flag set; read
        # once by consume_force.
        self._forced: set[str] = set()

    async def _cmd(self, command: str, *args: Any) -> Any:
        # The client retries connection failures on the command itself
        # (see backlog_redis_from_url): every BACKLOG.* command is
        # idempotent, so a replayed command leaves the server in the
        # same state as a single delivery. The reply can differ (a
        # replayed ADD answers 0, FORCE 'forced_pending', REQUEUE
        # False), so counts derived from replies are a lower bound
        # across a reconnect. That is the one place this client differs
        # from the Redis queue, whose Lua mutations may only be sent
        # once.
        return await self._redis.execute_command(
            f'BACKLOG.{command}', self._queue, *args,
        )

    # -- producers ----------------------------------------------------

    async def enqueue(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_url: str | None = None,
        channel_is_verified: bool | None = None,
    ) -> bool:
        '''Queue *video_id* unless it is already known. Channel and
        source context are accepted for compatibility but not stored.'''
        if not video_id:
            raise ValueError('empty video_id')
        reply: list[Any] = await self._cmd('ADD', video_id)
        result: int = int(reply[0])
        if result < 0:
            raise ValueError(f'invalid video_id: {video_id}')
        return result == 1

    async def enqueue_many(
        self,
        video_ids: list[str],
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_is_verified: bool | None = None,
        chunk_size: int = ENQUEUE_MANY_CHUNK_SIZE,
    ) -> int:
        '''Queue every ID that is not already known, *chunk_size* per
        command. Malformed IDs are skipped and logged.

        :returns: number of IDs newly queued.
        '''
        if any(not video_id for video_id in video_ids):
            raise ValueError('empty video_id')
        if chunk_size < 1:
            raise ValueError('chunk_size must be positive')
        added: int = 0
        invalid: int = 0
        start: int
        for start in range(0, len(video_ids), chunk_size):
            chunk: list[str] = video_ids[start:start + chunk_size]
            reply: list[Any] = await self._cmd('ADD', *chunk)
            value: Any
            for value in reply:
                result: int = int(value)
                if result == 1:
                    added += 1
                elif result < 0:
                    invalid += 1
        if invalid:
            _LOGGER.warning(
                'Backlog server rejected malformed video IDs',
                extra={'invalid': invalid, 'source': source},
            )
        return added

    async def force_enqueue(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_url: str | None = None,
        channel_is_verified: bool | None = None,
    ) -> str:
        '''Queue *video_id* at the front with the one-shot force flag.

        :returns: ``'added'``, ``'revived'`` or ``'forced_pending'``.
        '''
        if not video_id:
            raise ValueError('empty video_id')
        return _text(await self._cmd('FORCE', video_id))

    # -- consumers ----------------------------------------------------

    async def _pop_raw(self, batch: int) -> list[tuple[str, bool]]:
        reply: list[Any] = await self._cmd('POP', batch)
        return [(_text(item[0]), bool(int(item[1]))) for item in reply]

    async def pop(self, batch: int) -> list[str]:
        return [video_id for video_id, _ in await self._pop_raw(batch)]

    async def pop_entries(
        self, batch: int,
    ) -> list[VideoScrapeQueueEntry]:
        entries: list[VideoScrapeQueueEntry] = []
        video_id: str
        force: bool
        for video_id, force in await self._pop_raw(batch):
            meta: dict[str, str] = {}
            if force:
                meta['force'] = '1'
                self._forced.add(video_id)
            entries.append(VideoScrapeQueueEntry(
                video_id=video_id,
                channel=VideoQueueChannelContext(),
                source=None,
                meta=meta,
            ))
        return entries

    async def consume_force(self, video_id: str) -> bool:
        '''True once for an ID popped with the force flag. The server
        clears the flag itself on complete or mark.'''
        if video_id in self._forced:
            self._forced.discard(video_id)
            return True
        return False

    async def complete(self, video_id: str) -> None:
        self._forced.discard(video_id)
        await self._cmd('COMPLETE', video_id)

    async def flush(self) -> None:
        '''Nothing is buffered client-side.'''

    async def mark(
        self, video_id: str, *,
        state: VideoState,
        last_error: str | None = None,
        note: str | None = None,
    ) -> None:
        if state not in VideoState.terminal_states():
            raise ValueError(f'not a terminal state: {state!r}')
        self._forced.discard(video_id)
        await self._cmd(
            'MARK', video_id, state.value, normalize_reason(last_error),
        )

    async def unmark(self, video_id: str) -> None:
        await self._cmd('REQUEUE', video_id)

    async def bump_attempts(
        self, video_id: str, *, last_error: str,
    ) -> int:
        '''Extend the lease while the scraper retries. Attempts are
        counted by the scraper, so this always returns 0.'''
        await self._cmd('TOUCH', video_id)
        return 0

    # -- lookups ------------------------------------------------------

    async def get_state(
        self, video_id: str,
    ) -> VideoState | None:
        reply: list[Any] = await self._cmd('STATE', video_id)
        return _state(reply[0])

    async def get_states(
        self, video_ids: list[str],
    ) -> dict[str, VideoState | None]:
        ids: list[str] = list(video_ids)
        if not ids:
            return {}
        reply: list[Any] = await self._cmd('STATE', *ids)
        return {
            video_id: _state(value)
            for video_id, value in zip(ids, reply, strict=True)
        }

    async def get_state_names(
        self, video_ids: list[str],
    ) -> dict[str, str | None]:
        '''Verbatim server state names, ``scraped`` included.

        :meth:`get_states` hides ``scraped`` (a completed video is
        unknown to the Redis queue, and callers must not see two
        different worlds); the server keeps it, so this reports what
        it actually holds.
        '''
        ids: list[str] = list(video_ids)
        if not ids:
            return {}
        reply: list[Any] = await self._cmd('STATE', *ids)
        return {
            video_id: None if value is None else _text(value)
            for video_id, value in zip(ids, reply, strict=True)
        }

    def state_names(self) -> list[str]:
        '''Verbatim state names, the done ones included.

        A queue that does not track upload state has no ``uploaded``.
        '''
        names: list[str] = [state.value for state in VideoState]
        names.append('scraped')
        if self.supports_uploaded:
            names.append('uploaded')
        return names

    # -- upload state -------------------------------------------------

    async def mark_uploaded(self, video_ids: list[str]) -> None:
        '''Record *video_ids* as accepted by the bulk upload API.'''
        if not self.supports_uploaded:
            return
        ids: list[str] = [
            video_id for video_id in video_ids if video_id
        ]
        start: int
        for start in range(0, len(ids), ENQUEUE_MANY_CHUNK_SIZE):
            await self._cmd(
                'UPLOADED',
                *ids[start:start + ENQUEUE_MANY_CHUNK_SIZE],
            )

    async def uploaded_many(
        self, video_ids: list[str], *, api_fallback: bool = True,
    ) -> dict[str, bool]:
        '''True for IDs the server holds as ``uploaded``.

        With the migration fallback on, every ID the server does not
        hold as ``uploaded`` is asked about on scrape.exchange:
        ``scraped`` is ambiguous until the backfill, because it means
        "on the exchange" for the original import and "waiting for
        upload" for a video scraped since.
        '''
        if not self.supports_uploaded:
            return {video_id: False for video_id in video_ids}
        names: dict[str, str | None] = await self.get_state_names(
            list(video_ids),
        )
        out: dict[str, bool] = {
            video_id: names.get(video_id) == 'uploaded'
            for video_id in video_ids
        }
        if not (api_fallback and self._fallback_on()):
            return out
        ask: list[str] = [
            video_id for video_id in video_ids if not out.get(video_id)
        ]
        if not ask:
            return out
        out.update(await self._ask_api(ask))
        return out

    async def done_many(
        self, video_ids: list[str], *, api_fallback: bool = True,
    ) -> dict[str, bool]:
        '''True for IDs that need no more work.

        ``scraped`` is done even before its file is uploaded: it must
        not be scraped again, and the uploader reads ``uploaded``
        itself. With the migration fallback on, only IDs the server
        does not know are asked about: every known ID is already
        answered correctly, and the original import never queued an
        ID that was on scrape.exchange.
        '''
        if not self.supports_uploaded:
            return {video_id: False for video_id in video_ids}
        names: dict[str, str | None] = await self.get_state_names(
            list(video_ids),
        )
        out: dict[str, bool] = {
            video_id: names.get(video_id) in _DONE_STATES
            for video_id in video_ids
        }
        if not (api_fallback and self._fallback_on()):
            return out
        unknown: list[str] = [
            video_id for video_id in video_ids
            if names.get(video_id) is None
        ]
        if not unknown:
            return out
        out.update(await self._ask_api(unknown))
        return out

    def _fallback_on(self) -> bool:
        '''Whether the migration fallback may ask the API at all.'''
        if not self._settings.backlog_uploaded_api_fallback:
            return False
        if self._exchange_client is not None:
            return True
        if not self._fallback_logged:
            _LOGGER.warning(
                'BACKLOG_UPLOADED_API_FALLBACK is on but the queue '
                'has no ExchangeClient; skipping the API fallback',
            )
            self._fallback_logged = True
        return False

    async def _ask_api(self, video_ids: list[str]) -> dict[str, bool]:
        '''Ask scrape.exchange about *video_ids*.

        Answers are bounded by ``exchange_existence_concurrency``;
        confirmed IDs are marked ``uploaded``, so each one costs at
        most one lookup during the migration. An error answers False:
        the cost is a duplicate upload, which the bulk API reports as
        a duplicate.
        '''
        client: ExchangeClient | None = self._exchange_client
        if client is None:
            return {}
        gate: asyncio.Semaphore = asyncio.Semaphore(
            max(1, self._settings.exchange_existence_concurrency),
        )

        async def one(video_id: str) -> tuple[str, bool | None]:
            try:
                async with gate:
                    exists: bool = await exchange_video_exists(
                        client, self._exchange_url, video_id,
                    )
            # Fail open: an error answers "not uploaded", which
            # costs a duplicate upload at worst (design 5.4).
            except Exception:  # noqa: BLE001
                METRIC_BACKLOG_UPLOADED_API_FALLBACK.labels(
                    outcome='error',
                ).inc()
                return (video_id, None)
            return (video_id, exists)

        answers: list[tuple[str, bool | None]] = list(
            await asyncio.gather(*(one(video_id) for video_id in video_ids)),
        )
        out: dict[str, bool] = {}
        confirmed: list[str] = []
        for video_id, exists in answers:
            if exists is None:
                out[video_id] = False
                continue
            out[video_id] = exists
            METRIC_BACKLOG_UPLOADED_API_FALLBACK.labels(
                outcome='uploaded' if exists else 'not_uploaded',
            ).inc()
            if exists:
                confirmed.append(video_id)
        if confirmed:
            await self.mark_uploaded(confirmed)
        return out

    async def get_meta(
        self, video_id: str,
    ) -> dict[str, str]:
        state: VideoState | None = await self.get_state(video_id)
        return {'state': state.value} if state is not None else {}

    async def set_meta(
        self, video_id: str, **fields: str,
    ) -> None:
        raise BacklogServerUnsupported(
            'set_meta is not supported by the backlog server',
        )

    async def _info(self) -> dict[str, int]:
        reply: list[Any] = await self._cmd('INFO')
        return {
            _text(reply[i]): int(reply[i + 1])
            for i in range(0, len(reply) - 1, 2)
        }

    async def count_by_state(
        self,
    ) -> dict[VideoState, int]:
        info: dict[str, int] = await self._info()
        return {state: info.get(state.value, 0) for state in VideoState}

    async def count_by_state_name(self) -> dict[str, int]:
        '''Counts for every state the server stores, ``scraped`` and
        (when upload state is tracked) ``uploaded`` included.'''
        info: dict[str, int] = await self._info()
        return {name: info.get(name, 0) for name in self.state_names()}

    async def search_meta(
        self,
        pattern: str,
        fields: tuple[str, ...] = (
            'last_error', 'source',
        ),
    ) -> list[str]:
        raise BacklogServerUnsupported(
            'search_meta is not supported by the backlog server',
        )

    async def iter_members(self) -> AsyncIterator[dict]:
        raise BacklogServerUnsupported(
            'iter_members is not supported by the backlog server',
        )
        yield {}  # pragma: no cover - makes this an async generator
