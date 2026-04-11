'''
Generic token-bucket rate limiter for async HTTP scrapers.

Provides :class:`RateLimiter`, an abstract base class that manages per-proxy
token buckets for rate-limiting requests to external platforms.  Each concrete
subclass supplies its own call-type enum and bucket configurations.

A global (cross-type) bucket is checked *in addition* to the per-type bucket
so that concurrent traffic from different coroutines cannot collectively exceed
a safe aggregate rate.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import sys
import time
import json
import random
import fcntl
import hashlib
import asyncio
import logging

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, ClassVar, Generic, Self, TypeVar
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge, Histogram

_LOGGER: logging.Logger = logging.getLogger(__name__)
_SCRIPT_NAME: str = os.path.basename(sys.argv[0]) if sys.argv else 'unknown'

_METRIC_LABELS: list[str] = ['call_type', 'proxy', 'script', 'platform']

METRIC_REQUESTS_ACQUIRED: Counter = Counter(
    'rate_limiter_requests_acquired_total',
    'Total requests successfully acquired per call type',
    _METRIC_LABELS,
)
METRIC_WAIT_EVENTS: Counter = Counter(
    'rate_limiter_wait_events_total',
    'Total number of rate-limit sleep waits',
    _METRIC_LABELS,
)
METRIC_SLEEP_SECONDS: Histogram = Histogram(
    'rate_limiter_sleep_seconds',
    'Duration of rate-limit sleep waits in seconds',
    _METRIC_LABELS,
    buckets=(.1, .25, .5, 1.0, 2.0, 3.0, 5.0, 10.0, 30.0, 60.0),
)
METRIC_BUCKET_TOKENS: Gauge = Gauge(
    'rate_limiter_bucket_tokens',
    'Current token level in the per-type rate-limit bucket',
    _METRIC_LABELS,
)
METRIC_GLOBAL_BUCKET_TOKENS: Gauge = Gauge(
    'rate_limiter_global_bucket_tokens',
    'Current token level in the global rate-limit bucket',
    ['proxy', 'script', 'platform'],
)


@dataclass
class _Bucket:
    '''
    A simple token-bucket that refills at a constant rate.

    :param tokens: current number of available tokens
    :param burst: maximum tokens the bucket can hold
    :param refill_rate: tokens added per second
    :param last_refill: wall-clock timestamp of last refill

    Wall-clock (``time.time()``) is used rather than monotonic time so
    that bucket state can be persisted across processes and compared
    to values written by a different process on the same host. On a
    server that never suspends, this is operationally equivalent to
    monotonic time.
    '''
    tokens: float
    burst: int
    refill_rate: float
    last_refill: float = field(default_factory=time.time)

    def _refill(self) -> None:
        now: float = time.time()
        elapsed: float = max(0.0, now - self.last_refill)
        self.tokens = min(self.burst, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def time_until_available(self) -> float:
        '''Seconds until at least one token is available (0 if ready now).'''
        self._refill()
        if self.tokens >= 1.0:
            return 0.0
        return (1.0 - self.tokens) / self.refill_rate

    def try_acquire(self) -> bool:
        '''Consume one token if available. Returns True on success.'''
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


@dataclass
class _BucketConfig:
    '''Immutable configuration for one token bucket.'''
    burst: int
    refill_rate: float          # tokens per second
    jitter_min: float           # seconds
    jitter_max: float           # seconds


@dataclass
class _ProxyBuckets:
    '''All buckets and the acquire lock for a single proxy (or no-proxy).'''
    buckets: dict[Any, _Bucket]
    global_bucket: _Bucket


# Filesystems for which fcntl.flock() does not reliably serialise
# writers across processes. Refuse to use the shared backend on them.
_REJECTED_FS_TYPES: frozenset[str] = frozenset({
    'nfs', 'nfs4', 'cifs', 'smbfs', 'smb2', 'smb3',
    'fuse.sshfs', 'fuse.rclone', 'fuse.s3fs', 'fuse.gcsfuse',
})


def _detect_fs_type(path: str) -> str | None:
    '''
    Return the mount filesystem type for the mount point that
    *path* lives on, or ``None`` when it can't be determined.
    Resolves symlinks and walks up to the deepest existing
    ancestor so that the path does not need to already exist.
    '''

    target: str = os.path.realpath(path)
    while target and not os.path.exists(target):
        parent: str = os.path.dirname(target)
        if parent == target:
            break
        target = parent
    if not target:
        return None

    try:
        with open('/proc/mounts', 'r') as mounts:
            candidates: list[tuple[int, str]] = []
            for line in mounts:
                parts: list[str] = line.split()
                if len(parts) < 3:
                    continue
                mount_point: str = parts[1]
                fs_type: str = parts[2]
                if target == mount_point or target.startswith(
                    mount_point.rstrip('/') + '/'
                ):
                    candidates.append((len(mount_point), fs_type))
    except OSError:
        return None

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _assert_local_filesystem(path: str) -> None:
    '''
    Raise :class:`RuntimeError` when *path* resolves to a mount of
    a filesystem type that can't be trusted for cross-process
    ``fcntl.flock``-based coordination.
    '''

    fs_type: str | None = _detect_fs_type(path)
    if fs_type is None:
        _LOGGER.warning(
            'Could not determine filesystem type for %s; proceeding',
            path,
        )
        return
    if fs_type in _REJECTED_FS_TYPES:
        raise RuntimeError(
            f'Rate limiter state dir {path!r} is on a '
            f'{fs_type!r} filesystem; flock-based coordination '
            f'across processes is unreliable on that fs. Use a '
            f'local filesystem (ext4, xfs, btrfs, tmpfs) instead.'
        )


CallTypeT = TypeVar('CallTypeT', bound=Enum)


class _Backend(ABC, Generic[CallTypeT]):
    '''
    Abstract per-proxy bucket backend. Implementations provide the
    atomic refill + (maybe) consume operation that the rate limiter
    needs and the synchronous peek used by :meth:`RateLimiter.select_proxy`.
    '''

    def __init__(
        self,
        default_configs: dict[CallTypeT, _BucketConfig],
        global_config: _BucketConfig,
    ) -> None:
        self._default_configs: dict[CallTypeT, _BucketConfig] = (
            default_configs
        )
        self._global_config: _BucketConfig = global_config

    @abstractmethod
    def peek_tokens(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> float:
        '''
        Return the current token count for ``(proxy, call_type)``
        after a synthetic refill. Must be non-blocking and sync.
        '''

    @abstractmethod
    async def try_acquire(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> tuple[float, float, float]:
        '''
        Atomic refill + check + (when tokens permit) consume.

        :returns: ``(wait_seconds, bucket_tokens, global_tokens)``.
            ``wait_seconds == 0`` means a token was successfully
            consumed and the caller should proceed. Otherwise the
            caller should sleep that many seconds and retry. The
            two token counts reflect post-operation state for
            gauge metrics.
        '''

    @abstractmethod
    async def penalise(
        self, call_type: CallTypeT, proxy: str | None,
        penalty_seconds: float,
    ) -> None:
        '''
        Drain tokens from the per-type bucket for *proxy* by
        ``penalty_seconds * refill_rate``, floored at ``-burst``.
        Fleet-wide under the shared-file backend.
        '''


class _InProcessBackend(_Backend[CallTypeT]):
    '''
    In-memory, per-process backend. State is held in a dict keyed
    by proxy URL; no cross-process coordination. Matches the
    original rate-limiter behaviour.
    '''

    def __init__(
        self,
        default_configs: dict[CallTypeT, _BucketConfig],
        global_config: _BucketConfig,
    ) -> None:
        super().__init__(default_configs, global_config)
        self._proxy_buckets: dict[str | None, _ProxyBuckets] = {}

    def _create_proxy_buckets(self) -> _ProxyBuckets:
        now: float = time.time()
        gc: _BucketConfig = self._global_config
        return _ProxyBuckets(
            buckets={
                call_type: _Bucket(
                    tokens=cfg.burst,
                    burst=cfg.burst,
                    refill_rate=cfg.refill_rate,
                    last_refill=now,
                )
                for call_type, cfg in self._default_configs.items()
            },
            global_bucket=_Bucket(
                tokens=gc.burst,
                burst=gc.burst,
                refill_rate=gc.refill_rate,
                last_refill=now,
            ),
        )

    def _get_or_create(self, proxy: str | None) -> _ProxyBuckets:
        pb: _ProxyBuckets | None = self._proxy_buckets.get(proxy)
        if pb is None:
            pb = self._create_proxy_buckets()
            self._proxy_buckets[proxy] = pb
        return pb

    def peek_tokens(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> float:
        pb: _ProxyBuckets = self._get_or_create(proxy)
        bucket: _Bucket = pb.buckets[call_type]
        bucket._refill()
        return bucket.tokens

    async def try_acquire(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> tuple[float, float, float]:
        pb: _ProxyBuckets = self._get_or_create(proxy)
        bucket: _Bucket = pb.buckets[call_type]
        wait_type: float = bucket.time_until_available()
        wait_global: float = pb.global_bucket.time_until_available()
        wait: float = max(wait_type, wait_global)
        if wait <= 0:
            bucket.try_acquire()
            pb.global_bucket.try_acquire()
            return 0.0, bucket.tokens, pb.global_bucket.tokens
        return wait, bucket.tokens, pb.global_bucket.tokens

    async def penalise(
        self, call_type: CallTypeT, proxy: str | None,
        penalty_seconds: float,
    ) -> None:
        pb: _ProxyBuckets = self._get_or_create(proxy)
        bucket: _Bucket = pb.buckets[call_type]
        bucket.tokens -= penalty_seconds * bucket.refill_rate
        bucket.tokens = max(bucket.tokens, -float(bucket.burst))


_STATE_VERSION: int = 1


def _proxy_filename(proxy: str | None) -> str:
    '''Hash a proxy URL into a stable, credential-safe filename.'''
    key: str = proxy if proxy is not None else '__no_proxy__'
    digest: str = hashlib.sha1(key.encode('utf-8')).hexdigest()[:16]
    return f'{digest}.state'


class _SharedFileBackend(_Backend[CallTypeT]):
    '''
    Filesystem-backed backend. Bucket state for each proxy lives
    in a JSON file under ``state_dir``; every read-modify-write
    cycle is serialised across processes on the host via
    ``fcntl.flock`` with ``LOCK_EX``.

    Blocking file I/O (``open``/``flock``/``read``/``write``/
    ``close``) runs inside the default ``asyncio`` executor so that
    the event loop is never blocked waiting on lock contention or
    disk. A per-process in-memory cache of the last observed token
    counts is kept so that :meth:`peek_tokens` (used by
    :meth:`RateLimiter.select_proxy`) stays non-blocking; the
    cache is refreshed on every successful ``try_acquire``.
    '''

    def __init__(
        self,
        default_configs: dict[CallTypeT, _BucketConfig],
        global_config: _BucketConfig,
        state_dir: str,
    ) -> None:
        super().__init__(default_configs, global_config)
        _assert_local_filesystem(state_dir)
        os.makedirs(state_dir, exist_ok=True)
        self._state_dir: str = state_dir
        self._cache: dict[
            str | None,
            tuple[dict[CallTypeT, _Bucket], _Bucket],
        ] = {}

    def _path(self, proxy: str | None) -> str:
        return os.path.join(self._state_dir, _proxy_filename(proxy))

    def _fresh_buckets(
        self,
    ) -> tuple[dict[CallTypeT, _Bucket], _Bucket]:
        now: float = time.time()
        gc: _BucketConfig = self._global_config
        buckets: dict[CallTypeT, _Bucket] = {
            call_type: _Bucket(
                tokens=cfg.burst,
                burst=cfg.burst,
                refill_rate=cfg.refill_rate,
                last_refill=now,
            )
            for call_type, cfg in self._default_configs.items()
        }
        global_bucket: _Bucket = _Bucket(
            tokens=gc.burst,
            burst=gc.burst,
            refill_rate=gc.refill_rate,
            last_refill=now,
        )
        return buckets, global_bucket

    def _deserialise(
        self, raw: str,
    ) -> tuple[dict[CallTypeT, _Bucket], _Bucket]:
        '''
        Parse the on-disk state. Missing / malformed data yields a
        fresh full-burst bucket set so partial writes and schema
        changes never poison the limiter.
        '''

        buckets: dict[CallTypeT, _Bucket]
        global_bucket: _Bucket
        buckets, global_bucket = self._fresh_buckets()

        if not raw.strip():
            return buckets, global_bucket
        try:
            doc: dict[str, Any] = json.loads(raw)
        except (ValueError, TypeError):
            _LOGGER.warning(
                'Rate limiter state file corrupt; reinitialising',
            )
            return buckets, global_bucket

        persisted_buckets: dict[str, Any] = doc.get('buckets', {})
        for call_type, cfg in self._default_configs.items():
            entry: dict[str, Any] | None = persisted_buckets.get(
                call_type.value
            )
            if not entry:
                continue
            try:
                tokens: float = float(entry['tokens'])
                last_refill: float = float(entry['last_refill'])
            except (KeyError, ValueError, TypeError):
                continue
            buckets[call_type] = _Bucket(
                tokens=min(tokens, float(cfg.burst)),
                burst=cfg.burst,
                refill_rate=cfg.refill_rate,
                last_refill=last_refill,
            )

        persisted_global: dict[str, Any] = doc.get('global', {})
        try:
            gt: float = float(persisted_global['tokens'])
            glr: float = float(persisted_global['last_refill'])
            gc: _BucketConfig = self._global_config
            global_bucket = _Bucket(
                tokens=min(gt, float(gc.burst)),
                burst=gc.burst,
                refill_rate=gc.refill_rate,
                last_refill=glr,
            )
        except (KeyError, ValueError, TypeError):
            pass

        return buckets, global_bucket

    def _serialise(
        self,
        proxy: str | None,
        buckets: dict[CallTypeT, _Bucket],
        global_bucket: _Bucket,
    ) -> str:
        doc: dict[str, Any] = {
            'version': _STATE_VERSION,
            'proxy': proxy,
            'buckets': {
                ct.value: {
                    'tokens': b.tokens,
                    'last_refill': b.last_refill,
                }
                for ct, b in buckets.items()
            },
            'global': {
                'tokens': global_bucket.tokens,
                'last_refill': global_bucket.last_refill,
            },
        }
        return json.dumps(doc)

    def _update_cache(
        self, proxy: str | None,
        buckets: dict[CallTypeT, _Bucket],
        global_bucket: _Bucket,
    ) -> None:
        self._cache[proxy] = (dict(buckets), global_bucket)

    def peek_tokens(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> float:
        cached: tuple[dict[CallTypeT, _Bucket], _Bucket] | None = (
            self._cache.get(proxy)
        )
        if cached is None:
            # Not yet touched in this process. Optimistic: assume
            # the bucket sits at full burst. Authoritative state
            # will be seen on the next try_acquire.
            return float(self._default_configs[call_type].burst)
        buckets: dict[CallTypeT, _Bucket] = cached[0]
        bucket: _Bucket | None = buckets.get(call_type)
        if bucket is None:
            return float(self._default_configs[call_type].burst)
        bucket._refill()
        return bucket.tokens

    def _rmw_try_acquire(
        self, path: str, proxy: str | None, call_type: CallTypeT,
    ) -> tuple[
        float, float, float,
        dict[CallTypeT, _Bucket], _Bucket,
    ]:
        '''
        Blocking read-modify-write for ``try_acquire``. Opens the
        proxy's state file with ``O_RDWR|O_CREAT``, takes
        ``LOCK_EX``, reads current state, refills, consumes when
        possible, writes back, and returns the post-op numbers
        plus the fresh in-memory bucket view for the cache.
        '''

        fd: int = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw_bytes: bytes = b''
            while True:
                chunk: bytes = os.read(fd, 4096)
                if not chunk:
                    break
                raw_bytes += chunk
            buckets: dict[CallTypeT, _Bucket]
            global_bucket: _Bucket
            buckets, global_bucket = self._deserialise(
                raw_bytes.decode('utf-8', errors='replace')
            )
            bucket: _Bucket = buckets[call_type]
            wait_type: float = bucket.time_until_available()
            wait_global: float = (
                global_bucket.time_until_available()
            )
            wait: float = max(wait_type, wait_global)
            if wait <= 0:
                bucket.try_acquire()
                global_bucket.try_acquire()
                payload: str = self._serialise(
                    proxy, buckets, global_bucket
                )
                os.lseek(fd, 0, os.SEEK_SET)
                os.ftruncate(fd, 0)
                os.write(fd, payload.encode('utf-8'))
                return (
                    0.0,
                    bucket.tokens,
                    global_bucket.tokens,
                    buckets,
                    global_bucket,
                )
            # Not acquired: still persist the refill so the
            # next reader doesn't double-count elapsed time.
            payload2: str = self._serialise(
                proxy, buckets, global_bucket
            )
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, payload2.encode('utf-8'))
            return (
                wait,
                bucket.tokens,
                global_bucket.tokens,
                buckets,
                global_bucket,
            )
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    async def try_acquire(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> tuple[float, float, float]:
        loop: asyncio.AbstractEventLoop = (
            asyncio.get_running_loop()
        )
        path: str = self._path(proxy)
        wait: float
        bucket_tokens: float
        global_tokens: float
        buckets: dict[CallTypeT, _Bucket]
        global_bucket: _Bucket
        (
            wait,
            bucket_tokens,
            global_tokens,
            buckets,
            global_bucket,
        ) = await loop.run_in_executor(
            None, self._rmw_try_acquire, path, proxy, call_type,
        )
        self._update_cache(proxy, buckets, global_bucket)
        return wait, bucket_tokens, global_tokens

    def _rmw_penalise(
        self, path: str, proxy: str | None,
        call_type: CallTypeT, penalty_seconds: float,
    ) -> tuple[dict[CallTypeT, _Bucket], _Bucket]:
        fd: int = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw_bytes: bytes = b''
            while True:
                chunk: bytes = os.read(fd, 4096)
                if not chunk:
                    break
                raw_bytes += chunk
            buckets: dict[CallTypeT, _Bucket]
            global_bucket: _Bucket
            buckets, global_bucket = self._deserialise(
                raw_bytes.decode('utf-8', errors='replace')
            )
            bucket: _Bucket = buckets[call_type]
            bucket.tokens -= penalty_seconds * bucket.refill_rate
            bucket.tokens = max(
                bucket.tokens, -float(bucket.burst)
            )
            payload: str = self._serialise(
                proxy, buckets, global_bucket
            )
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, payload.encode('utf-8'))
            return buckets, global_bucket
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    async def penalise(
        self, call_type: CallTypeT, proxy: str | None,
        penalty_seconds: float,
    ) -> None:
        loop: asyncio.AbstractEventLoop = (
            asyncio.get_running_loop()
        )
        path: str = self._path(proxy)
        buckets: dict[CallTypeT, _Bucket]
        global_bucket: _Bucket
        buckets, global_bucket = await loop.run_in_executor(
            None,
            self._rmw_penalise,
            path, proxy, call_type, penalty_seconds,
        )
        self._update_cache(proxy, buckets, global_bucket)


class RateLimiter(ABC, Generic[CallTypeT]):
    '''
    Abstract async-safe, singleton rate limiter for platform HTTP
    scrapers.

    Subclasses must implement :attr:`default_configs` and
    :attr:`global_config` to supply per-call-type bucket parameters
    and a global aggregate bucket. The singleton, proxy management,
    and acquire loop are provided here.

    Backend selection: when *state_dir* is provided (or the env var
    ``RATE_LIMITER_STATE_DIR`` is set) the limiter uses a file-backed
    backend that shares state across every process on the host that
    points at the same directory, so that running multiple scraper
    tools (video / rss / channel) or multiple scraper processes
    against the same proxy pool no longer multiplies the configured
    per-proxy rate. Set *state_dir* to an empty string to force the
    in-process backend explicitly (useful in tests).

    Usage (from a concrete subclass)::

        limiter = MyRateLimiter.get()
        await limiter.acquire(
            MyCallType.FETCH, proxy='http://proxy:8080',
        )
    '''

    _instance: ClassVar['RateLimiter[Any] | None'] = None

    def __init__(
        self, platform: str, state_dir: str | None = None,
    ) -> None:
        self._platform: str = platform
        self._proxies: list[str] | None = None
        self._proxy_locks: dict[str | None, asyncio.Lock] = {}

        if state_dir is None:
            state_dir = os.environ.get('RATE_LIMITER_STATE_DIR')
        resolved_state_dir: str | None = state_dir or None

        self._state_dir: str | None = resolved_state_dir
        backend: _Backend[CallTypeT]
        if resolved_state_dir:
            backend = _SharedFileBackend(
                self.default_configs,
                self.global_config,
                resolved_state_dir,
            )
            _LOGGER.info(
                'Rate limiter using shared-file backend at %s',
                resolved_state_dir,
            )
        else:
            backend = _InProcessBackend(
                self.default_configs, self.global_config,
            )
            _LOGGER.info(
                'Rate limiter using in-process backend',
            )
        self._backend: _Backend[CallTypeT] = backend

    @property
    @abstractmethod
    def default_configs(self) -> dict[CallTypeT, _BucketConfig]:
        '''Per-call-type bucket configurations.'''

    @property
    @abstractmethod
    def global_config(self) -> _BucketConfig:
        '''Global (cross-type) aggregate bucket configuration.'''

    def set_proxies(
        self, proxies: list[str] | str | None,
    ) -> None:
        '''Register the proxy pool used by :meth:`acquire` when no
        explicit *proxy* is given.'''
        if isinstance(proxies, str):
            proxies = [
                p.strip() for p in proxies.split(',') if p.strip()
            ]
        self._proxies = proxies or None

    def _get_lock(self, proxy: str | None) -> asyncio.Lock:
        '''Per-proxy in-process lock coalescing concurrent acquires
        from the same process before they reach the backend.'''
        lock: asyncio.Lock | None = self._proxy_locks.get(proxy)
        if lock is None:
            lock = asyncio.Lock()
            self._proxy_locks[proxy] = lock
        return lock

    @classmethod
    def get(cls, state_dir: str | None = None) -> Self:
        '''
        Return the process-wide singleton, creating it on first
        call. *state_dir* is only honoured on the very first call;
        subsequent calls ignore it and return the existing instance.
        Call :meth:`reset` first to rebind a backend in tests.
        '''
        if cls._instance is None:
            cls._instance = cls(state_dir=state_dir)
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        '''Discard the singleton (useful in tests).'''
        cls._instance = None

    def select_proxy(self, call_type: CallTypeT) -> str | None:
        '''
        Return the proxy with the most tokens currently available
        for *call_type*, without consuming any tokens.

        This is a non-blocking peek. Under the shared-file backend
        the value may be slightly stale relative to other processes
        — it's advisory; :meth:`acquire` is still authoritative.
        '''
        if not self._proxies:
            return None
        best_tokens: float = -1.0
        best_proxies: list[str] = []
        for p in self._proxies:
            tokens: float = self._backend.peek_tokens(call_type, p)
            if tokens > best_tokens:
                best_tokens = tokens
                best_proxies = [p]
            elif tokens == best_tokens:
                best_proxies.append(p)
        return random.choice(best_proxies)

    def _labels(
        self, call_type: CallTypeT, proxy: str | None,
    ) -> dict[str, str]:
        return {
            'call_type': call_type.value,
            'proxy': proxy or 'none',
            'script': _SCRIPT_NAME,
            'platform': self._platform,
        }

    def _global_labels(
        self, proxy: str | None,
    ) -> dict[str, str]:
        return {
            'proxy': proxy or 'none',
            'script': _SCRIPT_NAME,
            'platform': self._platform,
        }

    async def acquire(
        self, call_type: CallTypeT, proxy: str | None = None,
    ) -> str | None:
        '''
        Wait until a request of *call_type* is permitted, then
        consume one token from both the per-type and global
        buckets.

        :returns: the proxy that was used (useful when
            auto-selected).
        '''
        if proxy is None:
            proxy = self.select_proxy(call_type)

        cfg: _BucketConfig = self.default_configs[call_type]
        labels: dict[str, str] = self._labels(call_type, proxy)
        global_labels: dict[str, str] = self._global_labels(proxy)
        lock: asyncio.Lock = self._get_lock(proxy)

        async with lock:
            while True:
                wait: float
                bucket_tokens: float
                global_tokens: float
                (
                    wait,
                    bucket_tokens,
                    global_tokens,
                ) = await self._backend.try_acquire(
                    call_type, proxy,
                )
                METRIC_BUCKET_TOKENS.labels(**labels).set(
                    bucket_tokens
                )
                METRIC_GLOBAL_BUCKET_TOKENS.labels(
                    **global_labels
                ).set(global_tokens)
                if wait <= 0:
                    break
                METRIC_WAIT_EVENTS.labels(**labels).inc()
                _LOGGER.debug(
                    'Rate limiter: %s (proxy=%s) waiting %.2fs',
                    call_type.value, proxy, wait,
                )
                lock.release()
                try:
                    METRIC_SLEEP_SECONDS.labels(**labels).observe(
                        wait
                    )
                    await asyncio.sleep(wait)
                finally:
                    await lock.acquire()

        METRIC_REQUESTS_ACQUIRED.labels(**labels).inc()

        jitter: float = random.uniform(cfg.jitter_min, cfg.jitter_max)
        if jitter > 0:
            _LOGGER.debug(
                'Rate limiter: %s (proxy=%s) jitter %.2fs',
                call_type.value, proxy, jitter,
            )
            await asyncio.sleep(jitter)

        return proxy

    async def penalise(
        self, call_type: CallTypeT, proxy: str | None,
        penalty_seconds: float,
    ) -> None:
        '''
        Drain tokens from the per-type bucket for *proxy* so that
        all coroutines sharing that bucket are forced to wait
        approximately *penalty_seconds* before their next
        ``acquire()``.

        Under the shared-file backend the penalty is **fleet-wide**:
        every process on the host that shares the state dir will
        see the drained tokens on its next acquire.
        '''
        await self._backend.penalise(
            call_type, proxy, penalty_seconds,
        )
        _LOGGER.warning(
            'Rate limiter: penalised %s (proxy=%s) by %.1fs',
            call_type.value, proxy, penalty_seconds,
        )
