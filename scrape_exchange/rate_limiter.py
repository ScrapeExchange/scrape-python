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

from scrape_exchange.worker_id import get_worker_id

_LOGGER: logging.Logger = logging.getLogger(__name__)
_SCRIPT_NAME: str = os.path.basename(sys.argv[0]) if sys.argv else 'unknown'

_METRIC_LABELS: list[str] = [
    'call_type', 'proxy', 'script', 'platform',
    'worker_id',
]

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
    ['proxy', 'script', 'platform', 'worker_id'],
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
            'Could not determine filesystem type; proceeding',
            extra={'path': str(path)},
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


# ------------------------------------------------------------------
# Redis backend
# ------------------------------------------------------------------

METRIC_REDIS_OPS: Counter = Counter(
    'rate_limiter_redis_ops_total',
    'Redis operations executed by the rate limiter backend.',
    ['operation', 'result', 'platform', 'worker_id'],
)

# Lua script: atomic refill + conditional consume.
# Uses redis TIME for server-side clock (avoids cross-host skew).
# Returns {wait_seconds, bucket_tokens, global_tokens} as strings.
_LUA_TRY_ACQUIRE: str = '''\
local key = KEYS[1]
local ct = ARGV[1]
local b_burst = tonumber(ARGV[2])
local b_rate = tonumber(ARGV[3])
local g_burst = tonumber(ARGV[4])
local g_rate = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local function read_field(f)
    local raw = redis.call('HGET', key, f)
    if raw then
        local sep = string.find(raw, ':')
        return tonumber(string.sub(raw, 1, sep - 1)),
               tonumber(string.sub(raw, sep + 1))
    end
    return nil, nil
end

local function refill(tokens, lr, burst, rate)
    if tokens == nil then
        return burst, now
    end
    local elapsed = math.max(0, now - lr)
    return math.min(burst, tokens + elapsed * rate), now
end

local b_tok, b_lr = read_field(ct)
b_tok, b_lr = refill(b_tok, b_lr, b_burst, b_rate)

local g_tok, g_lr = read_field('__global__')
g_tok, g_lr = refill(g_tok, g_lr, g_burst, g_rate)

local b_wait = 0
if b_tok < 1.0 then
    b_wait = (1.0 - b_tok) / b_rate
end
local g_wait = 0
if g_tok < 1.0 then
    g_wait = (1.0 - g_tok) / g_rate
end
local wait = math.max(b_wait, g_wait)

if wait <= 0 then
    b_tok = b_tok - 1.0
    g_tok = g_tok - 1.0
end

redis.call('HSET', key, ct,
    string.format('%.6f:%.6f', b_tok, b_lr))
redis.call('HSET', key, '__global__',
    string.format('%.6f:%.6f', g_tok, g_lr))
redis.call('EXPIRE', key, ttl)

return {tostring(wait), tostring(b_tok), tostring(g_tok)}
'''

# Lua script: drain tokens from a per-type bucket.
_LUA_PENALISE: str = '''\
local key = KEYS[1]
local ct = ARGV[1]
local b_burst = tonumber(ARGV[2])
local b_rate = tonumber(ARGV[3])
local penalty = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local raw = redis.call('HGET', key, ct)
local b_tok, b_lr
if raw then
    local sep = string.find(raw, ':')
    b_tok = tonumber(string.sub(raw, 1, sep - 1))
    b_lr = tonumber(string.sub(raw, sep + 1))
else
    b_tok = b_burst
    b_lr = now
end

local elapsed = math.max(0, now - b_lr)
b_tok = math.min(b_burst, b_tok + elapsed * b_rate)
b_lr = now

b_tok = b_tok - penalty * b_rate
b_tok = math.max(b_tok, -b_burst)

redis.call('HSET', key, ct,
    string.format('%.6f:%.6f', b_tok, b_lr))
redis.call('EXPIRE', key, ttl)

return tostring(b_tok)
'''


class _RedisBackend(_Backend[CallTypeT]):
    '''
    Redis-backed backend. Bucket state for each proxy lives
    in a Redis hash; every read-modify-write cycle is
    serialised by the Redis server via Lua scripts, enabling
    cross-host coordination.

    Requires the ``redis`` package (``pip install
    redis[hiredis]>=5.0.0``).
    '''

    _KEY_TTL: int = 3600  # 1 hour

    def __init__(
        self,
        default_configs: dict[CallTypeT, _BucketConfig],
        global_config: _BucketConfig,
        redis_dsn: str,
        platform: str,
    ) -> None:
        super().__init__(default_configs, global_config)
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                'redis package is required for the Redis '
                'rate-limiter backend. Install it with: '
                'pip install "redis[hiredis]>=5.0.0"'
            ) from None
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._platform: str = platform
        self._cache: dict[
            str | None,
            dict[str, tuple[float, float]],
        ] = {}
        self._try_acquire_sha: str | None = None
        self._penalise_sha: str | None = None
        self._scripts_loaded: bool = False

    def _key(self, proxy: str | None) -> str:
        h: str = _proxy_filename(proxy).removesuffix(
            '.state'
        )
        return f'rl:{self._platform}:{h}'

    def _metric_labels(
        self, operation: str, result: str,
    ) -> dict[str, str]:
        return {
            'operation': operation,
            'result': result,
            'platform': self._platform,
            'worker_id': get_worker_id(),
        }

    async def _ensure_scripts(self) -> None:
        '''Load Lua scripts into Redis on first use.'''
        if self._scripts_loaded:
            return
        self._try_acquire_sha = (
            await self._redis.script_load(
                _LUA_TRY_ACQUIRE
            )
        )
        self._penalise_sha = (
            await self._redis.script_load(
                _LUA_PENALISE
            )
        )
        self._scripts_loaded = True
        METRIC_REDIS_OPS.labels(
            **self._metric_labels(
                'script_load', 'success',
            )
        ).inc()

    async def _evalsha_with_reload(
        self, sha: str, script: str,
        num_keys: int, *args: str,
    ) -> list[str]:
        '''
        Run ``EVALSHA``. On ``NOSCRIPT`` (Redis restarted),
        reload all scripts and retry once.
        '''
        import redis.exceptions as rexc
        try:
            return await self._redis.evalsha(
                sha, num_keys, *args,
            )
        except rexc.NoScriptError:
            _LOGGER.warning(
                'Redis NOSCRIPT — reloading Lua scripts',
            )
            self._scripts_loaded = False
            await self._ensure_scripts()
            # Re-resolve SHA after reload
            if script is _LUA_TRY_ACQUIRE:
                sha = self._try_acquire_sha
            else:
                sha = self._penalise_sha
            return await self._redis.evalsha(
                sha, num_keys, *args,
            )

    def peek_tokens(
        self, call_type: CallTypeT,
        proxy: str | None,
    ) -> float:
        cached: (
            dict[str, tuple[float, float]] | None
        ) = self._cache.get(proxy)
        if cached is None:
            return float(
                self._default_configs[call_type].burst
            )
        entry: tuple[float, float] | None = (
            cached.get(call_type.value)
        )
        if entry is None:
            return float(
                self._default_configs[call_type].burst
            )
        tokens: float = entry[0]
        last_refill: float = entry[1]
        cfg: _BucketConfig = (
            self._default_configs[call_type]
        )
        now: float = time.time()
        elapsed: float = max(0.0, now - last_refill)
        return min(
            cfg.burst,
            tokens + elapsed * cfg.refill_rate,
        )

    async def try_acquire(
        self, call_type: CallTypeT,
        proxy: str | None,
    ) -> tuple[float, float, float]:
        await self._ensure_scripts()
        cfg: _BucketConfig = (
            self._default_configs[call_type]
        )
        gc: _BucketConfig = self._global_config
        try:
            result: list[str] = (
                await self._evalsha_with_reload(
                    self._try_acquire_sha,
                    _LUA_TRY_ACQUIRE,
                    1,
                    self._key(proxy),
                    call_type.value,
                    str(cfg.burst),
                    str(cfg.refill_rate),
                    str(gc.burst),
                    str(gc.refill_rate),
                    str(self._KEY_TTL),
                )
            )
        except Exception:
            _LOGGER.warning(
                'Redis unavailable; allowing request',
                exc_info=True,
            )
            METRIC_REDIS_OPS.labels(
                **self._metric_labels(
                    'try_acquire', 'error',
                )
            ).inc()
            return 0.0, 0.0, 0.0

        METRIC_REDIS_OPS.labels(
            **self._metric_labels(
                'try_acquire', 'success',
            )
        ).inc()

        wait: float = float(result[0])
        bt: float = float(result[1])
        gt: float = float(result[2])
        now: float = time.time()

        proxy_cache: dict[str, tuple[float, float]] = (
            self._cache.setdefault(proxy, {})
        )
        proxy_cache[call_type.value] = (bt, now)
        proxy_cache['__global__'] = (gt, now)

        return wait, bt, gt

    async def penalise(
        self, call_type: CallTypeT,
        proxy: str | None,
        penalty_seconds: float,
    ) -> None:
        await self._ensure_scripts()
        cfg: _BucketConfig = (
            self._default_configs[call_type]
        )
        try:
            bt_str: str = (
                await self._evalsha_with_reload(
                    self._penalise_sha,
                    _LUA_PENALISE,
                    1,
                    self._key(proxy),
                    call_type.value,
                    str(cfg.burst),
                    str(cfg.refill_rate),
                    str(penalty_seconds),
                    str(self._KEY_TTL),
                )
            )
        except Exception:
            _LOGGER.warning(
                'Redis unavailable during penalise',
                exc_info=True,
            )
            METRIC_REDIS_OPS.labels(
                **self._metric_labels(
                    'penalise', 'error',
                )
            ).inc()
            return

        METRIC_REDIS_OPS.labels(
            **self._metric_labels(
                'penalise', 'success',
            )
        ).inc()

        now: float = time.time()
        self._cache.setdefault(proxy, {})[
            call_type.value
        ] = (float(bt_str), now)


class RateLimiter(ABC, Generic[CallTypeT]):
    '''
    Abstract async-safe, singleton rate limiter for platform HTTP
    scrapers.

    Subclasses must implement :attr:`default_configs` and
    :attr:`global_config` to supply per-call-type bucket parameters
    and a global aggregate bucket. The singleton, proxy management,
    and acquire loop are provided here.

    Backend selection (first match wins):

    1. ``redis_dsn`` (or env ``REDIS_DSN``) — Redis backend for
       cross-host coordination.
    2. ``state_dir`` (or env ``RATE_LIMITER_STATE_DIR``) — shared-
       file backend for cross-process coordination on a single
       host.
    3. Neither — in-process backend (no coordination).

    Usage (from a concrete subclass)::

        limiter = MyRateLimiter.get()
        await limiter.acquire(
            MyCallType.FETCH, proxy='http://proxy:8080',
        )
    '''

    _instance: ClassVar['RateLimiter[Any] | None'] = None

    def __init__(
        self, platform: str,
        state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        self._platform: str = platform
        self._proxies: list[str] | None = None
        self._proxy_locks: dict[
            str | None, asyncio.Lock
        ] = {}

        if redis_dsn is None:
            redis_dsn = os.environ.get('REDIS_DSN')
        resolved_redis_dsn: str | None = (
            redis_dsn or None
        )

        if state_dir is None:
            state_dir = os.environ.get(
                'RATE_LIMITER_STATE_DIR'
            )
        resolved_state_dir: str | None = (
            state_dir or None
        )

        self._state_dir: str | None = resolved_state_dir
        backend: _Backend[CallTypeT]
        if resolved_redis_dsn:
            backend = _RedisBackend(
                self.default_configs,
                self.global_config,
                resolved_redis_dsn,
                platform,
            )
            _LOGGER.info(
                'Rate limiter using Redis backend',
            )
        elif resolved_state_dir:
            backend = _SharedFileBackend(
                self.default_configs,
                self.global_config,
                resolved_state_dir,
            )
            _LOGGER.info(
                'Rate limiter using shared-file backend',
                extra={'state_dir': str(resolved_state_dir)},
            )
        else:
            backend = _InProcessBackend(
                self.default_configs,
                self.global_config,
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
    def get(
        cls, state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> Self:
        '''
        Return the process-wide singleton, creating it on
        first call.  *state_dir* and *redis_dsn* are only
        honoured on the very first call; subsequent calls
        ignore them and return the existing instance.
        Call :meth:`reset` first to rebind a backend in tests.
        '''
        if cls._instance is None:
            cls._instance = cls(
                state_dir=state_dir,
                redis_dsn=redis_dsn,
            )
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
            'worker_id': get_worker_id(),
        }

    def _global_labels(
        self, proxy: str | None,
    ) -> dict[str, str]:
        return {
            'proxy': proxy or 'none',
            'script': _SCRIPT_NAME,
            'platform': self._platform,
            'worker_id': get_worker_id(),
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
                    'Rate limiter waiting',
                    extra={
                        'call_type': call_type.value,
                        'proxy': proxy,
                        'wait_seconds': wait,
                    },
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
                'Rate limiter jitter',
                extra={
                    'call_type': call_type.value,
                    'proxy': proxy,
                    'jitter_seconds': jitter,
                },
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
            'Rate limiter penalised',
            extra={
                'call_type': call_type.value,
                'proxy': proxy,
                'penalty_seconds': penalty_seconds,
            },
        )
