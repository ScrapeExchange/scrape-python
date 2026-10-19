'''MongoDB-backed backlog for the video scrape queue.

MongoDB is the source of truth for every video the queue knows: one
document per video in the ``<platform>_videos`` collection. Redis only
holds the *hot window*, the oldest queued videos that the scrapers pop
from (see :class:`scrape_exchange.video_scrape_queue.RedisVideoScrapeQueue`
and ``tools/yt_video_queue_refill.py``).

Document layout::

    {
        _id: <video_id>,
        state: 'queued' | 'hot' | 'unavailable' | 'failed' | 'removed',
        enqueued_at: <unix seconds>,
        source, channel_id, channel_handle,   # optional strings
        channel_is_verified,                  # optional bool
        record: {ts, last_error, note},       # terminal states only
    }

``queued`` documents wait in MongoDB; ``hot`` documents have been
copied into the Redis hot window (queued or being scraped). A
completed video's document is deleted; the uploaded-videos Bloom
filter keeps it from being queued again. Deletes are buffered and
sent in batches (see :meth:`MongoVideoBacklog.delete`).
'''

import asyncio
import time
from collections.abc import AsyncIterator, Iterable
from typing import Any

from pymongo import ASCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import BulkWriteError, DuplicateKeyError

STATE_QUEUED: str = 'queued'
STATE_HOT: str = 'hot'
TERMINAL_STATES: tuple[str, ...] = ('unavailable', 'failed', 'removed')

# count_by_state() runs every 30 s on several hosts; counting 100M+
# documents exactly is expensive, so counts are cached.
COUNTS_CACHE_SECONDS: float = 1800.0

# Completed videos are deleted in batches: one delete_many per
# DELETE_BATCH_SIZE IDs, or per DELETE_FLUSH_SECONDS, whichever comes
# first.
DELETE_BATCH_SIZE: int = 500
DELETE_FLUSH_SECONDS: float = 5.0

DEFAULT_DATABASE: str = 'scraper'

STATE_ENQUEUED_AT_INDEX: str = 'state_enqueued_at'
ID_INDEX: str = '_id_'

# One backlog (and so one MongoDB client and connection pool) per DSN,
# platform and event loop: callers such as the channel scraper build a
# queue object per scrape, and a client per object would exhaust the
# server's connections.
_SHARED: dict[tuple[str, str, int], MongoVideoBacklog] = {}

_CONTEXT_FIELDS: tuple[str, ...] = (
    'source', 'channel_id', 'channel_handle', 'channel_is_verified',
)


def backlog_document(
    video_id: str,
    *,
    state: str,
    enqueued_at: float,
    source: str | None = None,
    channel_id: str | None = None,
    channel_handle: str | None = None,
    channel_is_verified: bool | None = None,
) -> dict[str, Any]:
    '''Build a backlog document, omitting empty optional fields.'''
    doc: dict[str, Any] = {
        '_id': video_id, 'state': state,
        'enqueued_at': int(enqueued_at),
    }
    if source:
        doc['source'] = source
    if channel_id:
        doc['channel_id'] = channel_id
    if channel_handle:
        doc['channel_handle'] = channel_handle
    if channel_is_verified is not None:
        doc['channel_is_verified'] = channel_is_verified
    return doc


class MongoVideoBacklog:
    '''Async access to one platform's video backlog collection.

    :param collection: a PyMongo ``AsyncCollection`` (or a compatible
        test double).
    :param delete_batch_size: buffered deletes that trigger a flush
    :param delete_flush_seconds: flush buffered deletes at least this
        often while deletes keep arriving
    '''

    def __init__(
        self,
        collection: Any,
        *,
        delete_batch_size: int = DELETE_BATCH_SIZE,
        delete_flush_seconds: float = DELETE_FLUSH_SECONDS,
    ) -> None:
        self._coll: Any = collection
        self._indexes_ready: bool = False
        self._counts: dict[str, int] | None = None
        self._counts_at: float = 0.0
        self._delete_batch: int = delete_batch_size
        self._delete_interval: float = delete_flush_seconds
        self._pending_deletes: set[str] = set()
        self._deletes_flushed_at: float = time.monotonic()

    @classmethod
    def from_dsn(
        cls, mongo_dsn: str, *, platform: str = 'youtube',
    ) -> MongoVideoBacklog:
        '''Shared backlog on the DSN's default database (``scraper``
        when the DSN names none). The client connects lazily.'''
        loop_id: int
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            loop_id = 0
        key: tuple[str, str, int] = (mongo_dsn, platform, loop_id)
        shared: MongoVideoBacklog | None = _SHARED.get(key)
        if shared is not None:
            return shared
        client: AsyncMongoClient = AsyncMongoClient(
            mongo_dsn, appname=f'{platform}-video-backlog',
        )
        database: Any = client.get_default_database(
            default=DEFAULT_DATABASE,
        )
        backlog: MongoVideoBacklog = cls(database[f'{platform}_videos'])
        _SHARED[key] = backlog
        return backlog

    async def ensure_indexes(self) -> None:
        '''Create the backlog's index once per process: the refill
        reads the oldest queued documents and the reconcile streams
        the hot ones, both through ``state_enqueued_at``.

        There is deliberately no ``(state, _id)`` index. Its keys are
        spread randomly over the whole collection, so every insert,
        state change and delete updated it at a random position.'''
        if self._indexes_ready:
            return
        await self._coll.create_index(
            [('state', ASCENDING), ('enqueued_at', ASCENDING)],
            name=STATE_ENQUEUED_AT_INDEX,
        )
        self._indexes_ready = True

    # -- producers ----------------------------------------------------

    async def add(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_is_verified: bool | None = None,
        enqueued_at: float | None = None,
    ) -> bool:
        '''Insert *video_id* as queued unless it is already known.

        :returns: True when the video was added.
        '''
        await self.ensure_indexes()
        try:
            await self._coll.insert_one(backlog_document(
                video_id, state=STATE_QUEUED,
                enqueued_at=time.time() if enqueued_at is None
                else enqueued_at,
                source=source, channel_id=channel_id,
                channel_handle=channel_handle,
                channel_is_verified=channel_is_verified,
            ))
        except DuplicateKeyError:
            return False
        return True

    async def add_many(self, docs: list[dict[str, Any]]) -> int:
        '''Insert *docs* (see :func:`backlog_document`), skipping IDs
        that already exist.

        :returns: number of documents inserted.
        '''
        if not docs:
            return 0
        await self.ensure_indexes()
        try:
            result: Any = await self._coll.insert_many(
                docs, ordered=False,
            )
        except BulkWriteError as exc:
            errors: list[dict[str, Any]] = exc.details.get(
                'writeErrors', [],
            )
            if any(err.get('code') != 11000 for err in errors):
                raise
            return int(exc.details.get('nInserted', 0))
        return len(result.inserted_ids)

    # -- lookups ------------------------------------------------------

    async def states(
        self, video_ids: Iterable[str],
    ) -> dict[str, str | None]:
        '''Backlog state per video ID; None when unknown.'''
        ids: list[str] = list(video_ids)
        out: dict[str, str | None] = dict.fromkeys(ids)
        if not ids:
            return out
        doc: dict[str, Any]
        async for doc in self._coll.find(
            {'_id': {'$in': ids}}, {'state': 1},
        ):
            out[doc['_id']] = doc.get('state')
        return out

    async def get(self, video_id: str) -> dict[str, Any] | None:
        '''The video's document, or None.'''
        return await self._coll.find_one({'_id': video_id})

    async def counts(self) -> dict[str, int]:
        '''Documents per state, cached for COUNTS_CACHE_SECONDS.
        ``queued`` includes hot documents and is derived from the
        collection's estimated size minus the terminal counts.'''
        now: float = time.monotonic()
        if (
            self._counts is not None
            and now - self._counts_at < COUNTS_CACHE_SECONDS
        ):
            return dict(self._counts)
        total: int = int(await self._coll.estimated_document_count())
        counts: dict[str, int] = {}
        state: str
        for state in TERMINAL_STATES:
            counts[state] = int(
                await self._coll.count_documents({'state': state}),
            )
        counts[STATE_QUEUED] = max(total - sum(counts.values()), 0)
        self._counts = counts
        self._counts_at = now
        return dict(counts)

    async def iter_documents(
        self, *, exclude_state: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        '''Stream documents, optionally skipping one state.'''
        query: dict[str, Any] = (
            {'state': {'$ne': exclude_state}} if exclude_state else {}
        )
        doc: dict[str, Any]
        async for doc in self._coll.find(query):
            yield doc

    # -- state transitions --------------------------------------------

    async def force(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_is_verified: bool | None = None,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        '''Mark *video_id* hot (it is going straight into Redis),
        reviving a terminal record and merging channel context. The
        existing source wins.

        :returns: the document as it was before, or None if new.
        '''
        await self.ensure_indexes()
        self._pending_deletes.discard(video_id)
        update_set: dict[str, Any] = {'state': STATE_HOT}
        if channel_id:
            update_set['channel_id'] = channel_id
        if channel_handle:
            update_set['channel_handle'] = channel_handle
        if channel_is_verified is not None:
            update_set['channel_is_verified'] = channel_is_verified
        before: dict[str, Any] | None = (
            await self._coll.find_one_and_update(
                {'_id': video_id},
                {
                    '$set': update_set,
                    '$unset': {'record': ''},
                    '$setOnInsert': {
                        'enqueued_at': int(
                            time.time() if now is None else now,
                        ),
                    },
                },
                upsert=True,
                return_document=ReturnDocument.BEFORE,
            )
        )
        if source and not (before or {}).get('source'):
            await self._coll.update_one(
                {'_id': video_id}, {'$set': {'source': source}},
            )
        return before

    async def mark(
        self,
        video_id: str,
        *,
        state: str,
        record: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> None:
        '''Record a terminal *state* with its diagnostic *record*.
        *context* (source / channel fields) is stored so a later
        revive keeps it.'''
        if state not in TERMINAL_STATES:
            raise ValueError(f'not a terminal state: {state!r}')
        self._pending_deletes.discard(video_id)
        update_set: dict[str, Any] = {'state': state, 'record': record}
        key: str
        for key in _CONTEXT_FIELDS:
            value: Any = (context or {}).get(key)
            if value not in (None, ''):
                update_set[key] = value
        await self._coll.update_one(
            {'_id': video_id},
            {
                '$set': update_set,
                '$setOnInsert': {'enqueued_at': int(time.time())},
            },
            upsert=True,
        )

    async def requeue(
        self, video_id: str, *, now: float | None = None,
    ) -> None:
        '''Return *video_id* to the backlog as queued (operator
        revive of a terminal record).'''
        self._pending_deletes.discard(video_id)
        await self._coll.update_one(
            {'_id': video_id},
            {
                '$set': {
                    'state': STATE_QUEUED,
                    'enqueued_at': int(time.time() if now is None else now),
                },
                '$unset': {'record': ''},
            },
            upsert=True,
        )

    async def delete(self, video_id: str) -> None:
        '''Forget *video_id* (scraped successfully).

        The delete is buffered and sent with others in one
        ``delete_many`` once the buffer holds ``delete_batch_size`` IDs
        or ``delete_flush_seconds`` have passed since the last flush.
        Call :meth:`flush_deletes` before shutting down. A buffered
        delete lost in a crash leaves the document ``hot``; the refill
        reconcile returns it to the backlog and it is scraped again.
        '''
        self._pending_deletes.add(video_id)
        if (
            len(self._pending_deletes) >= self._delete_batch
            or time.monotonic() - self._deletes_flushed_at
            >= self._delete_interval
        ):
            await self.flush_deletes()

    async def flush_deletes(self) -> int:
        '''Send the buffered deletes. On failure the IDs go back into
        the buffer for the next flush and the error propagates.

        :returns: number of documents deleted.
        '''
        self._deletes_flushed_at = time.monotonic()
        if not self._pending_deletes:
            return 0
        ids: list[str] = list(self._pending_deletes)
        self._pending_deletes = set()
        try:
            result: Any = await self._coll.delete_many(
                {'_id': {'$in': ids}},
            )
        except Exception:
            self._pending_deletes.update(ids)
            raise
        return int(result.deleted_count)

    # -- refill -------------------------------------------------------

    async def oldest_queued(self, limit: int) -> list[dict[str, Any]]:
        '''The *limit* oldest queued documents.'''
        await self.ensure_indexes()
        return await self._coll.find(
            {'state': STATE_QUEUED},
        ).sort('enqueued_at', ASCENDING).limit(limit).to_list(None)

    async def set_state(
        self, video_ids: list[str], *, from_state: str, to_state: str,
    ) -> int:
        '''Move *video_ids* that are still in *from_state* to
        *to_state*. Documents that changed state meanwhile (completed,
        marked terminal) are left alone.

        :returns: number of documents changed.
        '''
        if not video_ids:
            return 0
        # Without the hint the planner may pick state_enqueued_at: the
        # refill marks the oldest queued documents, which sit at the
        # start of that index, so the scan wins the planner's trial and
        # then walks every queued document (hours on 600M+).
        result: Any = await self._coll.update_many(
            {'_id': {'$in': video_ids}, 'state': from_state},
            {'$set': {'state': to_state}},
            hint=ID_INDEX,
        )
        return int(result.modified_count)

    async def iter_hot_ids(
        self, *, batch_size: int = 10_000,
    ) -> AsyncIterator[str]:
        '''Stream the IDs of all hot documents, for reconciliation
        with Redis. The hint keeps MongoDB on the ``state`` range of
        ``state_enqueued_at``, which only reads the hot documents.'''
        await self.ensure_indexes()
        doc: dict[str, Any]
        async for doc in self._coll.find(
            {'state': STATE_HOT}, {'_id': 1},
        ).hint(STATE_ENQUEUED_AT_INDEX).batch_size(batch_size):
            yield doc['_id']
