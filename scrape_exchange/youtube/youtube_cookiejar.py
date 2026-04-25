'''
Per-proxy YouTube cookie jar for unauthenticated browser-cookie acquisition.

Fetches a real YouTube session via :class:`AsyncYouTubeClient` for each proxy
and persists the resulting cookies to a Netscape-format file that yt-dlp can
consume via its ``--cookiefile`` option.

Each proxy gets its own cookie file so that per-proxy session state stays
consistent with the proxy's identity from YouTube's perspective.  Cookie
files are stored in a deterministic path under a shared temp directory and
coordinated with ``fcntl`` file locks so that sibling processes (e.g. the
worker pool used by ``rebuild_creator_map``) reuse the same acquired
cookies instead of each re-fetching 58-way in parallel at startup.

Cookies are refreshed automatically when the TTL expires.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import fcntl
import hashlib
import os
import time
import logging
import tempfile
import http.cookiejar

from typing import Any, ClassVar, Self
from dataclasses import dataclass, field

import httpx

from .youtube_client import AsyncYouTubeClient, CONSENT_COOKIES

_LOGGER: logging.Logger = logging.getLogger(__name__)

# YouTube session cookies (YSC, PREF, GPS) last roughly 6 hours for
# unauthenticated sessions.  Refresh at 4 hours to stay well within that.
COOKIE_TTL: float = 4 * 3600.0

# Cookie acquisition runs at worker startup with many processes racing
# through the same proxy pool, creating a stampede that saturates proxy
# CONNECT handling. A single command-line curl through one of these
# proxies completes in ~1s, but httpx's 5s default leaves no margin
# when dozens of fresh TLS handshakes are in flight simultaneously.
# Use a longer timeout for this path; cookies are acquired once per
# proxy fleet-wide, so the extra latency cost is bounded.
_COOKIE_FETCH_TIMEOUT: httpx.Timeout = httpx.Timeout(
    5.0, connect=5.0,
)

# Cap the number of in-flight cookie fetches per process. With shared
# files most proxies adopt from disk after the first process warms them;
# this semaphore bounds the stampede for whichever process is the
# fleet-wide fetcher.
_WARM_CONCURRENCY: int = 8

# Shared cookie directory. All processes on the host cooperate here.
_SHARED_DIR_NAME: str = 'scrape_exchange_yt_cookies'


def _shared_cookie_dir() -> str:
    path: str = os.path.join(tempfile.gettempdir(), _SHARED_DIR_NAME)
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def _cookie_path_for_proxy(proxy: str | None) -> str:
    '''
    Return the deterministic cookie file path for *proxy*. All processes
    on the host that call this with the same proxy URL agree on the same
    path, which lets them share the cookies one of them acquired.
    '''
    key: str = proxy if proxy is not None else '__direct__'
    digest: str = hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]
    return os.path.join(
        _shared_cookie_dir(), f'cookies_{digest}.txt',
    )


def _file_age_seconds(path: str) -> float | None:
    '''Return the age of *path* in wall-clock seconds, or None if missing.'''
    try:
        return time.time() - os.path.getmtime(path)
    except OSError:
        return None


def _write_cookie_file(path: str, cookies: list) -> None:
    '''
    Serialise *cookies* to a Netscape cookie file at *path*. Writes to a
    per-pid temp sibling and ``os.replace``s onto the final path so other
    processes see either the old file or the new one, never a partial
    write. Intended to be run via :func:`asyncio.to_thread`.
    '''
    tmp_path: str = f'{path}.tmp.{os.getpid()}'
    fd: int = os.open(
        tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600,
    )
    try:
        with os.fdopen(fd, 'w') as f:
            f.write('# Netscape HTTP Cookie File\n')
            for cookie in cookies:
                domain: str = cookie.domain or '.youtube.com'
                if not domain.startswith('.'):
                    domain = '.' + domain
                include_sub: str = 'TRUE'
                secure: str = 'TRUE' if cookie.secure else 'FALSE'
                expires: int = int(cookie.expires or 0)
                f.write(
                    f'{domain}\t{include_sub}\t'
                    f'{cookie.path or "/"}\t'
                    f'{secure}\t{expires}\t'
                    f'{cookie.name}\t{cookie.value}\n'
                )
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    os.replace(tmp_path, path)


@dataclass
class _CookieEntry:
    '''Cached cookie file path and its (wall-clock) acquisition timestamp.

    ``created_at`` is wall-clock (``time.time()``) rather than monotonic so
    that entries adopted from another process's cookie file can be assigned
    a sensible age based on the file's mtime.
    '''
    path: str
    created_at: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > COOKIE_TTL

    def delete(self) -> None:
        '''Forget this entry. The underlying file is left on disk so other
        processes sharing it don't lose their cookies mid-request; the next
        acquire in this or any process atomically overwrites it.'''
        return None


class YouTubeCookieJar:
    '''
    Singleton that manages per-proxy Netscape cookie files for YouTube.

    On the first call for a given proxy a real browser-like session is
    opened through that proxy using :class:`AsyncYouTubeClient`.  YouTube's
    response cookies (``YSC``, ``PREF``, ``GPS``, etc.) are combined with
    the consent cookies already set by the client and written to a file at
    a deterministic path so every process on the host reuses the same
    cookies.  Subsequent calls return the cached path until the TTL
    expires, at which point the file is re-acquired and atomically
    replaced.

    Usage::

        file_path = await YouTubeCookieJar.get().get_cookie_file(proxy)
        download_client.params['cookiefile'] = file_path
    '''

    _instance: ClassVar['YouTubeCookieJar | None'] = None

    def __init__(self) -> None:
        self._entries: dict[str | None, _CookieEntry] = {}
        # Per-proxy locks prevent two coroutines from concurrently
        # acquiring cookies for the same proxy within this process
        # (e.g. warm_cookie_jar vs first worker).
        self._acquire_locks: dict[str | None, asyncio.Lock] = {}
        # Bounds concurrent network fetches per process.
        self._fetch_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            _WARM_CONCURRENCY,
        )

    @classmethod
    def get(cls) -> Self:
        '''Return the process-wide singleton, creating it on first call.'''
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        '''Discard the singleton. Shared cookie files are left on disk.'''
        cls._instance = None

    def _lock(self, proxy: str | None) -> asyncio.Lock:
        lock = self._acquire_locks.get(proxy)
        if lock is None:
            lock = asyncio.Lock()
            self._acquire_locks[proxy] = lock
        return lock

    async def get_cookie_file(self, proxy: str | None) -> str | None:
        '''
        Return the path to a valid Netscape cookie file for *proxy*,
        acquiring fresh cookies if none exist or the TTL has expired.

        Concurrent calls for the same proxy are serialised so that only
        one acquisition request is made even if multiple coroutines race
        here simultaneously (e.g. warm-up task vs first worker). The
        ``_acquire`` path additionally holds an ``fcntl`` advisory lock
        on the cookie file to serialise across processes.

        :param proxy: Proxy URL the cookie file was acquired through, or
            ``None`` for a direct (no-proxy) session.
        :returns: Filesystem path to the cookie file, or ``None`` if
            acquisition failed.
        '''
        entry: _CookieEntry | None = self._entries.get(proxy)
        if entry is not None and not entry.is_expired():
            return entry.path

        async with self._lock(proxy):
            entry = self._entries.get(proxy)
            if entry is not None and not entry.is_expired():
                return entry.path
            if entry is not None:
                _LOGGER.debug(
                    f'Cookie TTL expired for proxy={proxy}, refreshing'
                )
            entry = await self._acquire(proxy, force=False)
            if entry is not None:
                self._entries[proxy] = entry
        return entry.path if entry is not None else None

    async def force_renew(self, proxy: str | None) -> str | None:
        '''
        Re-acquire cookies for *proxy* regardless of the current TTL.
        Used by the proactive renewal loop.  The on-disk file is
        atomically replaced; other processes either pick up the refresh
        on their next ``get_cookie_file`` call or keep using the
        previous file until their own entry expires.
        '''
        async with self._lock(proxy):
            self._entries.pop(proxy, None)
            entry: _CookieEntry | None = await self._acquire(
                proxy, force=True,
            )
            if entry is not None:
                self._entries[proxy] = entry
        return entry.path if entry is not None else None

    def load_into_session(self, session: Any, proxy: str | None) -> None:
        '''
        Load cached cookies for *proxy* into a ``requests.Session`` cookie
        jar.

        Called synchronously from InnerTube client constructors after the
        session is created.  No-op if no valid cached entry exists for the
        proxy (e.g. warm-up has not yet run).

        :param session: A ``requests.Session`` instance
        (``InnerTube.adaptor.session``).
        :param proxy: Proxy the cookies were acquired through, or ``None``.
        '''
        entry: _CookieEntry | None = self._entries.get(proxy)
        if entry is None or entry.is_expired():
            return
        jar: http.cookiejar.MozillaCookieJar = (
            http.cookiejar.MozillaCookieJar()
        )
        try:
            jar.load(
                entry.path, ignore_discard=True, ignore_expires=True,
            )
            session.cookies.update(jar)
        except OSError as exc:
            _LOGGER.warning(
                'Failed to load cookie file',
                exc=exc,
                extra={'cookie_file': entry.path, 'proxy': proxy},
            )

    async def _acquire(
        self, proxy: str | None, force: bool,
    ) -> _CookieEntry | None:
        '''
        Acquire cookies for *proxy* with cross-process coordination.

        Holds an advisory ``fcntl.LOCK_EX`` lock on ``<path>.lock`` for
        the duration of the operation so only one process on the host
        fetches cookies for a given proxy at a time.  After taking the
        lock, re-checks the file's mtime: if another process just wrote
        fresh cookies we adopt them instead of re-fetching (unless
        ``force`` is set, which only the renewal loop uses).

        :param proxy: Proxy URL, or ``None`` for a direct session.
        :param force: If True, always fetch — skip the adopt-from-disk
            check.  Used by :meth:`force_renew`.
        :returns: A :class:`_CookieEntry` on success, ``None`` on failure.
        '''
        path: str = _cookie_path_for_proxy(proxy)
        lock_path: str = path + '.lock'
        lock_fd: int = await asyncio.to_thread(
            os.open, lock_path, os.O_CREAT | os.O_RDWR, 0o600,
        )
        try:
            await asyncio.to_thread(
                fcntl.flock, lock_fd, fcntl.LOCK_EX,
            )

            if not force:
                age: float | None = _file_age_seconds(path)
                if age is not None and age < COOKIE_TTL:
                    _LOGGER.debug(
                        'Adopted cookie file from another process',
                        extra={
                            'proxy': proxy,
                            'cookie_file': path,
                            'age_s': round(age, 2),
                        },
                    )
                    return _CookieEntry(
                        path=path,
                        created_at=time.time() - age,
                    )

            return await self._fetch_and_write(proxy, path)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    async def _fetch_and_write(
        self, proxy: str | None, path: str,
    ) -> _CookieEntry | None:
        '''
        Open a fresh YouTube session through *proxy*, serialise the
        resulting cookies to ``<path>.tmp``, and ``os.replace`` onto
        *path* so other processes see a valid file at all times.
        Concurrent fetches across the process are bounded by
        :attr:`_fetch_semaphore`.
        '''
        async with self._fetch_semaphore:
            try:
                client: AsyncYouTubeClient = AsyncYouTubeClient(
                    proxy=proxy, timeout=_COOKIE_FETCH_TIMEOUT,
                )
                async with client:
                    await client.get(AsyncYouTubeClient.SCRAPE_URL)
                    cookies = list(client.cookies.jar)

                await asyncio.to_thread(
                    _write_cookie_file, path, cookies,
                )

                _LOGGER.debug(
                    'Acquired cookies for proxy',
                    extra={
                        'cookie_count': len(cookies),
                        'proxy': proxy,
                        'cookie_file': path,
                    },
                )
                return _CookieEntry(path=path)

            except Exception as exc:
                cause: BaseException = exc.__cause__ or exc
                _LOGGER.warning(
                    'Failed to acquire cookies for proxy',
                    exc=exc,
                    extra={
                        'proxy': proxy,
                        'exc_cause_type': type(cause).__name__,
                        'exc_cause_message': str(cause),
                    },
                )
                return None
