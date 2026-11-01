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

import logging
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis

from scrape_exchange.video_scrape_queue import (
    ENQUEUE_MANY_CHUNK_SIZE,
    VideoQueueChannelContext,
    VideoScrapeQueue,
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

DEFAULT_QUEUE_NAME: str = 'youtube:video'

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


def _state(value: Any) -> VideoState | None:
    '''Server state name -> VideoState. ``scraped`` maps to None, as a
    completed video is unknown to the Redis queue too. Use
    :meth:`BacklogServerVideoScrapeQueue.get_state_names` when the
    difference between scraped and unknown matters.'''
    if value is None:
        return None
    name: str = _text(value)
    if name == 'scraped':
        return None
    try:
        return VideoState(name)
    except ValueError:
        return None


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
    ) -> None:
        self._redis: aioredis.Redis = redis
        self._settings: VideoScrapeQueueSettings = settings
        self._queue: str = queue_name
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

    async def count_by_state(
        self,
    ) -> dict[VideoState, int]:
        reply: list[Any] = await self._cmd('INFO')
        info: dict[str, int] = {
            _text(reply[i]): int(reply[i + 1])
            for i in range(0, len(reply) - 1, 2)
        }
        return {state: info.get(state.value, 0) for state in VideoState}

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
