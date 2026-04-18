'''
Cross-process claim lock for content scraping.

Before a worker spends a scrape slot on a content item,
it attempts to *claim* the content ID.  If another process
already holds the claim the worker skips the item,
avoiding duplicate scraping across ``NUM_PROCESSES > 1``
child processes.

Two interchangeable backends are provided:

* :class:`FileContentClaim` — atomic ``O_CREAT|O_EXCL``
  lock files in a shared directory.  Zero external
  dependencies; requires all processes to share a
  filesystem.
* :class:`RedisContentClaim` — ``SET NX EX`` in Redis.
  Automatic TTL-based expiry; works across hosts.

Both expose the same async context-manager interface via
the abstract :class:`ContentClaim` base class so the
worker code is backend-agnostic.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import os
import time

from abc import ABC, abstractmethod
from pathlib import Path

_LOGGER: logging.Logger = logging.getLogger(__name__)

# Default TTL for claims (1 hour).  Filesystem claims
# that are older than this are treated as stale and
# forcibly reclaimed.  Redis claims use this as the key
# EX value so they expire automatically.
DEFAULT_CLAIM_TTL: int = 3600


class ContentClaim(ABC):
    '''
    Abstract base for cross-process content claim locks.

    Usage::

        claim: ContentClaim = FileContentClaim(...)
        acquired: bool = await claim.acquire(content_id)
        if not acquired:
            # another process owns this content item
            ...
        # ... scrape ...
        await claim.release(content_id)
    '''

    @abstractmethod
    async def acquire(self, content_id: str) -> bool:
        '''
        Attempt to claim *content_id*.

        :returns: ``True`` if the claim was acquired,
            ``False`` if another process already holds it.
        '''

    @abstractmethod
    async def release(self, content_id: str) -> None:
        '''
        Release the claim on *content_id*.  Safe to call
        even if the claim was not held (idempotent).
        '''

    @abstractmethod
    async def cleanup_stale(self) -> int:
        '''
        Remove stale claims left by crashed processes.

        :returns: Number of stale claims removed.
        '''


class FileContentClaim(ContentClaim):
    '''
    Filesystem-backed claim lock using atomic file
    creation (``O_CREAT | O_EXCL``).

    Claim files are named ``.claim-{content_id}`` inside
    *claim_dir*.  Each file contains the PID and a
    monotonic timestamp so :meth:`cleanup_stale` can
    detect and remove locks left by dead processes.

    :param claim_dir: Directory for claim files.  Created
        automatically if it does not exist.
    :param ttl: Seconds after which a claim is considered
        stale (used by :meth:`cleanup_stale`).
    '''

    _PREFIX: str = '.claim-'

    def __init__(
        self, claim_dir: str, ttl: int = DEFAULT_CLAIM_TTL,
    ) -> None:
        self._dir: Path = Path(claim_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._ttl: int = ttl

    def _path(self, content_id: str) -> Path:
        return self._dir / f'{self._PREFIX}{content_id}'

    async def acquire(self, content_id: str) -> bool:
        path: Path = self._path(content_id)
        try:
            fd: int = os.open(
                str(path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError:
            # Fail-safe: if the claim file is older than
            # the TTL, treat it as stale (the owning
            # process likely crashed) and reclaim it.
            try:
                age: float = (
                    time.time() - path.stat().st_mtime
                )
            except OSError:
                return False
            if age < self._ttl:
                return False
            _LOGGER.warning(
                'Reclaiming expired filesystem claim',
                extra={
                    'content_id': content_id,
                    'age_seconds': age,
                    'ttl': self._ttl,
                },
            )
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            # Retry the atomic create — another process
            # may have grabbed it between our unlink and
            # this open.
            try:
                fd = os.open(
                    str(path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                return False
        try:
            content: str = (
                f'{os.getpid()} {time.monotonic()}'
            )
            os.write(fd, content.encode())
        finally:
            os.close(fd)
        return True

    async def release(self, content_id: str) -> None:
        path: Path = self._path(content_id)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    async def cleanup_stale(self) -> int:
        '''
        Remove all claim files in the claim directory.

        Intended to be called once by the supervisor (or
        the single-process entry point) before spawning
        workers.  Any leftover claim file is from a
        previous run that did not shut down cleanly.
        '''

        removed: int = 0
        for entry in self._dir.iterdir():
            if not entry.name.startswith(self._PREFIX):
                continue
            try:
                entry.unlink()
                removed += 1
            except OSError:
                pass
        if removed:
            _LOGGER.info(
                'Cleaned up stale filesystem claims',
                extra={'removed': removed},
            )
        return removed


class RedisContentClaim(ContentClaim):
    '''
    Redis-backed claim lock using ``SET NX EX``.

    Claims are stored as keys
    ``{platform}:claim:{content_id}`` with a TTL that
    acts as automatic stale-lock cleanup.

    :param redis_dsn: Redis connection string.
    :param platform: Platform identifier used as key
        prefix (e.g. ``'youtube'``, ``'tiktok'``).
    :param ttl: Seconds for the claim key TTL.
    '''

    def __init__(
        self,
        redis_dsn: str,
        platform: str,
        ttl: int = DEFAULT_CLAIM_TTL,
    ) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                'redis package is required for '
                'RedisContentClaim. Install with: '
                'pip install "redis[hiredis]>=5.0.0"'
            ) from None
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._ttl: int = ttl
        self._pid: str = str(os.getpid())
        self._key_prefix: str = f'{platform}:claim:'

    def _key(self, content_id: str) -> str:
        return f'{self._key_prefix}{content_id}'

    async def acquire(self, content_id: str) -> bool:
        result: bool | None = await self._redis.set(
            self._key(content_id),
            self._pid,
            nx=True,
            ex=self._ttl,
        )
        return result is True

    async def release(self, content_id: str) -> None:
        await self._redis.delete(self._key(content_id))

    async def cleanup_stale(self) -> int:
        '''
        Scan and delete all claim keys.

        With Redis TTL-based expiry this is rarely needed
        but is provided for symmetry and for use at
        supervisor startup to clear any claims from a
        previous unclean shutdown that have not yet
        expired.
        '''

        removed: int = 0
        cursor: int = 0
        pattern: str = f'{self._key_prefix}*'
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor, match=pattern, count=100,
            )
            if keys:
                await self._redis.delete(*keys)
                removed += len(keys)
            if cursor == 0:
                break
        if removed:
            _LOGGER.info(
                'Cleaned up stale Redis claims',
                extra={'removed': removed},
            )
        return removed


class NullContentClaim(ContentClaim):
    '''
    No-op claim lock for single-process mode or when
    cross-process coordination is disabled.  Every
    :meth:`acquire` succeeds unconditionally.
    '''

    async def acquire(self, content_id: str) -> bool:
        return True

    async def release(self, content_id: str) -> None:
        pass

    async def cleanup_stale(self) -> int:
        return 0
