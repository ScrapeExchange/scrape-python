'''
Per-proxy YouTube cookie jar for unauthenticated browser-cookie acquisition.

Fetches a real YouTube session via :class:`AsyncYouTubeClient` for each proxy
and persists the resulting cookies to a Netscape-format temp file that yt-dlp
can consume via its ``--cookiefile`` option.

Each proxy gets its own cookie file so that per-proxy session state stays
consistent with the proxy's identity from YouTube's perspective.  Cookies are
refreshed automatically when the TTL expires.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import os
import time
import logging
import tempfile
import http.cookiejar

from typing import Any, ClassVar, Self
from dataclasses import dataclass, field

from .youtube_client import AsyncYouTubeClient, CONSENT_COOKIES

_LOGGER: logging.Logger = logging.getLogger(__name__)

# YouTube session cookies (YSC, PREF, GPS) last roughly 6 hours for
# unauthenticated sessions.  Refresh at 4 hours to stay well within that.
COOKIE_TTL: float = 4 * 3600.0


@dataclass
class _CookieEntry:
    '''Cached cookie file path and its acquisition timestamp.'''
    path: str
    created_at: float = field(default_factory=time.monotonic)

    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_at) > COOKIE_TTL

    def delete(self) -> None:
        try:
            os.unlink(self.path)
        except OSError:
            pass


class YouTubeCookieJar:
    '''
    Singleton that manages per-proxy Netscape cookie files for YouTube.

    On the first call for a given proxy a real browser-like session is opened
    through that proxy using :class:`AsyncYouTubeClient`.  YouTube's response
    cookies (``YSC``, ``PREF``, ``GPS``, etc.) are combined with the consent
    cookies already set by the client and written to a temp file.  Subsequent
    calls return the cached path until the TTL expires, at which point the
    file is deleted and re-acquired.

    Usage::

        file_path = await YouTubeCookieJar.get().get_cookie_file(proxy)
        download_client.params['cookiefile'] = file_path
    '''

    _instance: ClassVar['YouTubeCookieJar | None'] = None

    def __init__(self) -> None:
        self._entries: dict[str | None, _CookieEntry] = {}
        # Per-proxy locks prevent two coroutines from concurrently acquiring
        # cookies for the same proxy (e.g. warm_cookie_jar vs first worker).
        self._acquire_locks: dict[str | None, asyncio.Lock] = {}

    @classmethod
    def get(cls) -> Self:
        '''Return the process-wide singleton, creating it on first call.'''
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        '''Discard the singleton and delete all temp cookie files.'''
        if cls._instance is not None:
            for entry in cls._instance._entries.values():
                entry.delete()
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

        Concurrent calls for the same proxy are serialised so that only one
        acquisition request is made even if multiple coroutines race here
        simultaneously (e.g. warm-up task vs first worker).

        :param proxy: Proxy URL the cookie file was acquired through, or
            ``None`` for a direct (no-proxy) session.
        :returns: Filesystem path to the temp cookie file, or ``None`` if
            acquisition failed.
        '''
        # Fast path: valid entry already cached.
        entry: _CookieEntry | None = self._entries.get(proxy)
        if entry is not None and not entry.is_expired():
            return entry.path

        async with self._lock(proxy):
            # Re-check inside the lock; another coroutine may have acquired.
            entry = self._entries.get(proxy)
            if entry is not None and not entry.is_expired():
                return entry.path
            if entry is not None:
                _LOGGER.debug(
                    f'Cookie TTL expired for proxy={proxy}, refreshing'
                )
                entry.delete()
            entry = await self._acquire(proxy)
            if entry is not None:
                self._entries[proxy] = entry
        return entry.path if entry is not None else None

    async def force_renew(self, proxy: str | None) -> str | None:
        '''
        Discard the cached entry for *proxy* and acquire fresh cookies,
        regardless of TTL.  Used by the proactive renewal loop to replace
        cookies before they expire.

        :param proxy: Proxy to renew, or ``None`` for direct connection.
        :returns: Filesystem path to the new cookie file, or ``None`` on
        failure.
        '''

        async with self._lock(proxy):
            entry: _CookieEntry | None = self._entries.pop(proxy, None)
            if entry is not None:
                entry.delete()
            entry = await self._acquire(proxy)
            if entry is not None:
                self._entries[proxy] = entry
        return entry.path if entry is not None else None

    def load_into_session(self, session: Any, proxy: str | None) -> None:
        '''
        Load cached cookies for *proxy* into a ``requests.Session`` cookie jar.

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
        jar: http.cookiejar.MozillaCookieJar = http.cookiejar.MozillaCookieJar()
        try:
            jar.load(entry.path, ignore_discard=True, ignore_expires=True)
            session.cookies.update(jar)
        except OSError as exc:
            _LOGGER.warning(
                'Failed to load cookie file %s for proxy=%s: %s',
                entry.path, proxy, exc,
            )

    async def _acquire(self, proxy: str | None) -> _CookieEntry | None:
        '''
        Open a fresh :class:`AsyncYouTubeClient` session through *proxy*,
        fetch the YouTube homepage so that YouTube sets session cookies, then
        serialise all cookies to a Netscape-format temp file.

        :param proxy: Proxy URL to use, or ``None`` for a direct connection.
        :returns: A :class:`_CookieEntry` on success, ``None`` on failure.
        '''
        try:
            client: AsyncYouTubeClient = AsyncYouTubeClient(
                consent_cookies=CONSENT_COOKIES,
                proxy=proxy,
            )
            async with client:
                await client.get(AsyncYouTubeClient.SCRAPE_URL)
                # Snapshot cookies while the client is still open.
                cookies = list(client.cookies.jar)

            tmp: tempfile._TemporaryFileWrapper[str] = \
                tempfile.NamedTemporaryFile(
                    mode='w', suffix='.txt', prefix='yt_cookies_',
                    delete=False,
                )
            tmp.write('# Netscape HTTP Cookie File\n')
            for cookie in cookies:
                domain: str = cookie.domain or '.youtube.com'
                if not domain.startswith('.'):
                    domain = '.' + domain
                include_sub: str = 'TRUE'
                secure: str = 'TRUE' if cookie.secure else 'FALSE'
                expires: int = int(cookie.expires or 0)
                tmp.write(
                    f'{domain}\t{include_sub}\t{cookie.path or "/"}\t'
                    f'{secure}\t{expires}\t{cookie.name}\t{cookie.value}\n'
                )
            tmp.flush()
            tmp.close()

            _LOGGER.debug(
                'Acquired %d cookies for proxy=%s → %s',
                len(cookies), proxy, tmp.name,
            )
            return _CookieEntry(path=tmp.name)

        except Exception as exc:
            _LOGGER.warning(
                'Failed to acquire cookies for proxy=%s: %s', proxy, exc,
            )
            return None
