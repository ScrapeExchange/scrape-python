'''
Platform/entity-agnostic operator interface over scrape queues.

``OperatorQueue`` is the small command surface the
``scrape-queue`` CLI speaks to. Each ``(platform, entity)`` pair
registers an adapter that maps those commands onto its backing queue
and defines its own small state set. Only ``('tiktok', 'creator')`` is
registered today; adding a platform/entity later is one registry entry
plus an adapter.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import contextlib
import dataclasses
import json
import re
import sys
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.tiktok.short_url import (
    normalize_tiktok_short_url,
)
from scrape_exchange.twitch.normalization import normalize_creator
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


_TIKTOK_VIDEO_ID_RE: re.Pattern[str] = re.compile(r'^\d{5,}$')
_TIKTOK_VIDEO_URL_RE: re.Pattern[str] = re.compile(
    r'(?:^|/)video/(\d{5,})(?:\D|$)',
)


@dataclasses.dataclass
class ImportReport:
    '''Line-level outcomes from a queue import file.'''

    total_lines: int = 0
    added: int = 0
    duplicates: int = 0
    invalid: int = 0
    blank: int = 0
    comments: int = 0


def normalize_tiktok_video_id(value: str) -> str | None:
    '''Extract a bare TikTok video/item id from *value*.

    Accepts either a bare numeric id or a canonical-ish TikTok video
    URL containing ``/video/<id>``.
    '''
    candidate: str = value.strip()
    if not candidate:
        return None
    if _TIKTOK_VIDEO_ID_RE.fullmatch(candidate):
        return candidate
    match: re.Match[str] | None = _TIKTOK_VIDEO_URL_RE.search(
        candidate,
    )
    if match is not None:
        return match.group(1)
    return None


def _parse_tiktok_video_import_line(
    path: str,
    line_no: int,
    stripped: str,
) -> str | None:
    '''Parse one non-blank, non-comment TikTok video import line.'''
    raw: object = stripped
    if stripped.startswith('{'):
        try:
            obj: object = json.loads(stripped)
        except json.JSONDecodeError as exc:
            print(
                f'{path}:{line_no}: skipping malformed JSON: {exc}',
                file=sys.stderr,
            )
            return None
        if not isinstance(obj, dict) or 'video_id' not in obj:
            print(
                f'{path}:{line_no}: skipping JSON line without '
                'video_id',
                file=sys.stderr,
            )
            return None
        raw = obj['video_id']
    if raw is None:
        print(
            f'{path}:{line_no}: skipping empty video_id',
            file=sys.stderr,
        )
        return None
    video_id: str | None = normalize_tiktok_video_id(str(raw))
    if video_id is None:
        print(
            f'{path}:{line_no}: skipping invalid TikTok '
            f'video id: {raw!r}',
            file=sys.stderr,
        )
    return video_id


def normalize_tiktok_creator_handle(value: str) -> str | None:
    '''Extract a bare TikTok creator handle from *value*.

    Accepts three input shapes:

    1. Bare handle: ``charlidamelio``
    2. Handle with ``@`` prefix: ``@charlidamelio``
    3. Full TikTok profile URL: ``https://www.tiktok.com/@charlidamelio?x=y``

    Returns the handle without ``@``, ``https://www.tiktok.com/`` prefix,
    and query parameters.  Returns ``None`` when *value* does not match
    any recognised shape.
    '''
    candidate: str = value.strip()
    if not candidate:
        return None

    # Case 3: full URL like https://www.tiktok.com/@handle[?x=y]
    url_match: re.Match[str] | None = (
        _TIKTOK_CREATOR_URL_RE.fullmatch(candidate)
    )
    if url_match is not None:
        handle: str = url_match.group(1)
        return handle if handle else None

    # Strip leading '@' if present (cases 1 and 2).
    handle: str = candidate
    if handle.startswith('@'):
        handle = handle[1:]

    if not handle:
        return None

    return handle if _TIKTOK_CREATOR_HANDLE_RE.fullmatch(handle) else None


_TIKTOK_CREATOR_HANDLE_RE: re.Pattern[str] = re.compile(
    r'^[a-zA-Z0-9_.-]{2,24}$',
)
_TIKTOK_CREATOR_URL_RE: re.Pattern[str] = re.compile(
    r'https?://(?:www\.)?tiktok\.com/@([a-zA-Z0-9_.-]{2,24})'
    r'(?:/)?(?:\?.*)?$',
)
_INSTAGRAM_RESERVED_PATHS: set[str] = {
    'about',
    'accounts',
    'api',
    'developer',
    'direct',
    'explore',
    'graphql',
    'p',
    'reel',
    'reels',
    'stories',
}
_INSTAGRAM_CREATOR_HANDLE_RE: re.Pattern[str] = re.compile(
    r'^[a-zA-Z0-9_.]{1,30}$',
)
_INSTAGRAM_CREATOR_URL_RE: re.Pattern[str] = re.compile(
    r'https?://(?:www\.)?instagram\.com/([^/?#]+)'
    r'(?:/)?(?:[?#].*)?$',
)


def normalize_instagram_creator_handle(value: str) -> str | None:
    '''Extract a lowercase Instagram username from a submission.'''
    candidate: str = value.strip()
    if not candidate:
        return None
    url_match: re.Match[str] | None = (
        _INSTAGRAM_CREATOR_URL_RE.fullmatch(candidate)
    )
    if url_match is not None:
        candidate = url_match.group(1)
    if candidate.startswith('@'):
        candidate = candidate[1:]
    handle: str = candidate.lower()
    if handle in _INSTAGRAM_RESERVED_PATHS:
        return None
    if (
        handle.startswith('.')
        or handle.endswith('.')
        or '..' in handle
    ):
        return None
    if not _INSTAGRAM_CREATOR_HANDLE_RE.fullmatch(handle):
        return None
    return handle


def normalize_tiktok_creator_submission(value: str) -> str | None:
    '''Normalise an operator-submitted creator reference to a queue
    member: a bare handle, or a canonical vm/vt short URL. Returns
    ``None`` when *value* is neither.'''
    handle: str | None = normalize_tiktok_creator_handle(value)
    if handle is not None:
        return handle
    return normalize_tiktok_short_url(value)


def _parse_tiktok_creator_import_line(
    path: str,
    line_no: int,
    stripped: str,
) -> str | None:
    '''Parse one non-blank, non-comment TikTok creator import line.'''
    raw: object = stripped
    if stripped.startswith('{'):
        try:
            obj: object = json.loads(stripped)
        except json.JSONDecodeError as exc:
            print(
                f'{path}:{line_no}: skipping malformed JSON: {exc}',
                file=sys.stderr,
            )
            return None
        if not isinstance(obj, dict):
            print(
                f'{path}:{line_no}: skipping non-object JSON line',
                file=sys.stderr,
            )
            return None
        raw = (
            obj.get('username')
            or obj.get('creator_id')
            or obj.get('handle')
        )
        if raw is None:
            print(
                f'{path}:{line_no}: skipping JSON line without '
                'username',
                file=sys.stderr,
            )
            return None
    if raw is None:
        print(
            f'{path}:{line_no}: skipping empty username',
            file=sys.stderr,
        )
        return None
    handle: str | None = normalize_tiktok_creator_submission(str(raw))
    if handle is None:
        print(
            f'{path}:{line_no}: skipping invalid TikTok '
            f'creator: {raw!r}',
            file=sys.stderr,
        )
    return handle


class OperatorQueue(ABC):
    '''Agnostic operator command surface for one scrape queue.'''

    platform: str = ''
    entity: str = ''
    member_label: str = 'member'

    @abstractmethod
    def states(self) -> list[str]:
        '''The (small) state set this queue exposes.'''

    @abstractmethod
    async def count_by_state(self) -> dict[str, int]:
        '''Member counts keyed by state.'''

    @abstractmethod
    async def show(self, member_id: str) -> dict | None:
        '''Member detail, or ``None`` if unknown.'''

    @abstractmethod
    async def search(
        self, term: str, limit: int,
    ) -> list[dict]:
        '''Members whose id or name matches *term*.'''

    @abstractmethod
    async def add(
        self, members: list[tuple[str, int]],
    ) -> int:
        '''Add ``(member_id, weight)`` pairs; return count added.'''

    @abstractmethod
    async def remove(self, member_id: str) -> bool:
        '''Remove a member (durably). Return whether it acted.'''

    @abstractmethod
    async def rescrape(self, member_ids: list[str]) -> int:
        '''Make members due now; return count rescheduled.'''

    @abstractmethod
    async def import_members(self, path: str) -> ImportReport:
        '''Bulk-add members and report every input-line outcome.'''

    @abstractmethod
    def export(self) -> AsyncIterator[dict]:
        '''Async-iterate every member as a record dict. Each record
        contains at least a ``state`` key.'''

    @abstractmethod
    async def close(self) -> None:
        '''Release backend resources.'''


class TikTokCreatorQueueAdapter(OperatorQueue):
    '''Operator adapter over ``RedisCreatorQueue`` for TikTok
    creators. States: queued | claimed | removed.'''

    platform: str = 'tiktok'
    entity: str = 'creator'
    member_label: str = 'username'

    def __init__(
        self,
        queue: RedisCreatorQueue,
        tiers: list[TierConfig],
    ) -> None:
        self._queue: RedisCreatorQueue = queue
        self._tiers: list[TierConfig] = tiers

    def states(self) -> list[str]:
        return ['queued', 'claimed', 'removed']

    def _fallback_weight(self) -> int:
        '''Follower count that routes a member to the next-to-lowest
        tier (the scraper's new-creator rule).'''
        tier: TierConfig = (
            self._tiers[-2] if len(self._tiers) >= 2
            else self._tiers[-1]
        )
        return max(tier.min_subscribers, 0)

    async def count_by_state(self) -> dict[str, int]:
        return await self._queue.count_by_state()

    async def show(self, member_id: str) -> dict | None:
        return await self._queue.show_member(member_id)

    async def search(
        self, term: str, limit: int,
    ) -> list[dict]:
        return await self._queue.search_members(term, limit)

    async def add(
        self, members: list[tuple[str, int]],
    ) -> int:
        added: int = 0
        for member_id, weight in members:
            if await self._queue.add_member(
                member_id, member_id, weight,
            ):
                added += 1
        return added

    async def remove(self, member_id: str) -> bool:
        await self._queue.exclude(member_id)
        return True

    async def rescrape(self, member_ids: list[str]) -> int:
        count: int = 0
        for member_id in member_ids:
            if await self._queue.reschedule(member_id):
                count += 1
        return count

    async def import_members(self, path: str) -> ImportReport:
        weight: int = self._fallback_weight()
        report = ImportReport()
        with open(path, 'r') as fd:
            for line_no, line in enumerate(fd, start=1):
                report.total_lines += 1
                name: str = line.strip()
                if not name:
                    report.blank += 1
                    continue
                if name.startswith('#'):
                    report.comments += 1
                    continue
                handle: str | None = (
                    _parse_tiktok_creator_import_line(
                        path, line_no, name,
                    )
                )
                if handle is None:
                    report.invalid += 1
                    continue
                if await self.add([(handle, weight)]):
                    report.added += 1
                else:
                    report.duplicates += 1
        return report

    async def export(self) -> AsyncIterator[dict]:
        async for rec in self._queue.iter_members():
            yield rec

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._queue._redis.aclose()


class TikTokVideoQueueAdapter(OperatorQueue):
    '''Operator adapter over ``RedisVideoScrapeQueue`` for TikTok
    videos. States: queued | unavailable | failed | removed.'''

    platform: str = 'tiktok'
    entity: str = 'video'
    member_label: str = 'video_id'

    def __init__(self, queue: RedisVideoScrapeQueue) -> None:
        self._queue: RedisVideoScrapeQueue = queue

    def states(self) -> list[str]:
        return [
            VideoState.QUEUED.value,
            VideoState.UNAVAILABLE.value,
            VideoState.FAILED.value,
            VideoState.REMOVED.value,
        ]

    async def count_by_state(self) -> dict[str, int]:
        counts: dict[VideoState, int] = (
            await self._queue.count_by_state()
        )
        return {state.value: count for state, count in counts.items()}

    async def show(self, member_id: str) -> dict | None:
        video_id: str | None = normalize_tiktok_video_id(member_id)
        if video_id is None:
            return None
        meta: dict[str, str] = await self._queue.get_meta(video_id)
        if not meta:
            return None
        return {'video_id': video_id, **meta}

    async def search(
        self, term: str, limit: int,
    ) -> list[dict]:
        hits: list[dict] = []
        direct: dict | None = await self.show(term)
        if direct is not None:
            hits.append(direct)
        ids: list[str] = await self._queue.search_meta(f'*{term}*')
        for video_id in ids:
            if len(hits) >= limit:
                break
            if any(h.get('video_id') == video_id for h in hits):
                continue
            rec: dict | None = await self.show(video_id)
            if rec is not None:
                hits.append(rec)
        return hits[:limit]

    async def add(
        self, members: list[tuple[str, int]],
    ) -> int:
        added: int = 0
        for member_id, _weight in members:
            video_id: str | None = normalize_tiktok_video_id(
                member_id,
            )
            if video_id is None:
                continue
            if await self._queue.enqueue(video_id, source='cli'):
                added += 1
        return added

    async def remove(self, member_id: str) -> bool:
        video_id: str | None = normalize_tiktok_video_id(member_id)
        if video_id is None:
            return False
        await self._queue.mark(
            video_id, state=VideoState.REMOVED, note='removed by cli',
        )
        return True

    async def rescrape(self, member_ids: list[str]) -> int:
        count: int = 0
        for member_id in member_ids:
            video_id: str | None = normalize_tiktok_video_id(
                member_id,
            )
            if video_id is None:
                continue
            await self._queue.force_enqueue(
                video_id, source='cli-rescrape',
            )
            count += 1
        return count

    async def import_members(self, path: str) -> ImportReport:
        report = ImportReport()
        with open(path, 'r') as fd:
            for line_no, line in enumerate(fd, start=1):
                report.total_lines += 1
                stripped: str = line.strip()
                if not stripped:
                    report.blank += 1
                    continue
                if stripped.startswith('#'):
                    report.comments += 1
                    continue
                video_id: str | None = (
                    _parse_tiktok_video_import_line(
                        path, line_no, stripped,
                    )
                )
                if video_id is None:
                    report.invalid += 1
                    continue
                if await self.add([(video_id, 0)]):
                    report.added += 1
                else:
                    report.duplicates += 1
        return report

    async def export(self) -> AsyncIterator[dict]:
        async for rec in self._queue.iter_members():
            yield rec

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._queue._redis.aclose()


class InstagramCreatorQueueAdapter(TikTokCreatorQueueAdapter):
    '''Operator adapter over ``RedisCreatorQueue`` for Instagram
    creators. States: queued | claimed | removed.'''

    platform: str = 'instagram'
    entity: str = 'creator'
    member_label: str = 'username'

    def normalize(self, value: str) -> str | None:
        return normalize_instagram_creator_handle(value)

    async def add(
        self, members: list[tuple[str, int]],
    ) -> int:
        weight: int = self._fallback_weight()
        added: int = 0
        for member_id, requested_weight in members:
            handle: str | None = self.normalize(member_id)
            if handle is None:
                continue
            member_weight: int = requested_weight or weight
            if await self._queue.add_member(
                handle, handle, member_weight,
            ):
                added += 1
        return added

    async def import_members(self, path: str) -> ImportReport:
        weight: int = self._fallback_weight()
        report = ImportReport()
        with open(path, 'r') as fd:
            for line_no, line in enumerate(fd, start=1):
                report.total_lines += 1
                stripped: str = line.strip()
                if not stripped:
                    report.blank += 1
                    continue
                if stripped.startswith('#'):
                    report.comments += 1
                    continue
                raw: object = stripped
                if stripped.startswith('{'):
                    try:
                        obj: object = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        print(
                            f'{path}:{line_no}: skipping malformed '
                            f'JSON: {exc}',
                            file=sys.stderr,
                        )
                        report.invalid += 1
                        continue
                    if not isinstance(obj, dict):
                        report.invalid += 1
                        continue
                    raw = (
                        obj.get('username')
                        or obj.get('creator_id')
                        or obj.get('handle')
                    )
                handle: str | None = (
                    self.normalize(raw) if isinstance(raw, str) else None
                )
                if handle is None:
                    report.invalid += 1
                    continue
                if await self.add([(handle, weight)]):
                    report.added += 1
                else:
                    report.duplicates += 1
        return report


class TwitchCreatorQueueAdapter(InstagramCreatorQueueAdapter):
    '''Reuse handle-based creator operations with Twitch normalization.'''

    platform: str = 'twitch'

    def normalize(self, value: str) -> str | None:
        return normalize_creator(value)

    async def show(self, member_id: str) -> dict | None:
        handle: str | None = self.normalize(member_id)
        return await super().show(handle) if handle else None

    async def remove(self, member_id: str) -> bool:
        handle: str | None = self.normalize(member_id)
        return await super().remove(handle) if handle else False

    async def rescrape(self, member_ids: list[str]) -> int:
        handles: list[str] = [
            handle for value in member_ids
            if (handle := self.normalize(value)) is not None
        ]
        return await super().rescrape(handles)


def _build_twitch_creator_adapter(settings: Any) -> OperatorQueue:
    tiers: list[TierConfig] = parse_priority_queues(
        getattr(settings, 'twitch_creator_priority_queues',
                '24:1000000,72:100000,168:10000,336:0'),
    )
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn, getattr(settings, 'worker_id', '0'),
        'twitch', key_namespace='scrape',
    )
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    return TwitchCreatorQueueAdapter(queue, tiers)


def _build_tiktok_creator_adapter(
    settings: Any,
) -> OperatorQueue:
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn,
        getattr(settings, 'worker_id', '0'),
        'tiktok',
        key_namespace='scrape',
    )
    # The operator tool runs without populate(), which is what
    # normally sets these; wire them so the new helpers know the
    # tier queue keys.
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    return TikTokCreatorQueueAdapter(queue, tiers)


def _build_tiktok_video_adapter(settings: Any) -> OperatorQueue:
    redis = redis_from_url(
        settings.redis_dsn,
        component='tiktok-video-queue',
        decode_responses=True,
    )
    queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
        redis,
        VideoScrapeQueueSettings(),
        platform='tiktok',
    )
    return TikTokVideoQueueAdapter(queue)


def _build_instagram_creator_adapter(settings: Any) -> OperatorQueue:
    tiers: list[TierConfig] = parse_priority_queues(
        getattr(
            settings,
            'instagram_creator_priority_queues',
            '72:10000000,168:1000000,336:100000,720:10000,4320:0',
        ),
    )
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn,
        getattr(settings, 'worker_id', '0'),
        'instagram',
        key_namespace='scrape',
    )
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    return InstagramCreatorQueueAdapter(queue, tiers)


# Registry: (platform, entity) -> adapter factory.
ADAPTERS: dict[
    tuple[str, str], Callable[[Any], OperatorQueue]
] = {
    ('twitch', 'creator'): _build_twitch_creator_adapter,
    ('instagram', 'creator'): _build_instagram_creator_adapter,
    ('tiktok', 'creator'): _build_tiktok_creator_adapter,
    ('tiktok', 'video'): _build_tiktok_video_adapter,
}


def get_adapter(
    platform: str, entity: str, settings: Any,
) -> OperatorQueue:
    '''Resolve the adapter for *platform*/*entity*, or raise with the
    registered pairs.'''
    key: tuple[str, str] = (platform, entity)
    factory: Callable[[Any], OperatorQueue] | None = (
        ADAPTERS.get(key)
    )
    if factory is None:
        registered: str = ', '.join(
            f'{p}/{e}' for p, e in sorted(ADAPTERS)
        )
        raise ValueError(
            f'no queue adapter for {platform}/{entity}; '
            f'registered: {registered}',
        )
    return factory(settings)
