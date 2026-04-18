'''
Centralised rate-limit arbiter for all YouTube API traffic.

Every call that touches a YouTube endpoint — InnerTube browse/player/next,
HTML page scrapes, and RSS feed fetches — must acquire a token from the
shared :class:`YouTubeRateLimiter` before proceeding.

The limiter uses a **token-bucket** algorithm per call type.  Each bucket
refills at a steady rate (``refill_rate`` tokens/second) up to a
``burst`` ceiling.  When the bucket is empty the caller sleeps until a
token becomes available, plus a randomised jitter drawn from
``[jitter_min, jitter_max]`` to break up request cadence.

A global (cross-type) bucket is checked *in addition* to the per-type
bucket so that concurrent browse + player + next traffic from different
coroutines cannot collectively exceed a safe aggregate rate.

Typical YouTube soft/hard limits (empirical, as of 2025), assuming
unauthenticated browser cookies acquired via :class:`YouTubeCookieJar`
(CONSENT/SOCS + real session cookies, no logged-in account):

    browse – ~150 req/min, burst 20
            (50% of 300/min valid-context soft limit)
    player – ~30  req/min, burst 4
            (30% of 100/min; yt-dlp extract_info
             adds ~5 ungated sub-requests ≈160/min
             effective load, 47% headroom)
    next   – ~150 req/min, burst 20
            (50% of 300/min valid-context soft limit)
    html   – ~90  req/min, burst 10
            (50% of 180/min with-cookies soft limit)
    rss    – ~60  req/min, burst 15
            (plain XML, low risk)
    global – ~250 req/min aggregate across all types
            (83% of 300/min valid-context IP ceiling)

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging

from enum import Enum
from typing import ClassVar

from scrape_exchange.rate_limiter import RateLimiter, _BucketConfig

_LOGGER: logging.Logger = logging.getLogger(__name__)

# Renew cookies this many seconds before the TTL expires so there is never
# a gap where all workers hit an expired cookie file simultaneously.
_COOKIE_RENEWAL_BUFFER: float = 10 * 60.0


class YouTubeCallType(str, Enum):
    '''Discriminator for the different YouTube endpoint families.'''
    BROWSE = 'browse'
    PLAYER = 'player'
    NEXT = 'next'
    HTML = 'html'
    RSS = 'rss'


# Per-type defaults — tuned to ~50% of the with-cookies soft limits from the
# README rate-limit table (assumes unauthenticated browser session cookies
# acquired via YouTubeCookieJar).
_DEFAULT_CONFIGS: dict[YouTubeCallType, _BucketConfig] = {
    # ~150 req/min
    YouTubeCallType.BROWSE: _BucketConfig(
        burst=20, refill_rate=150 / 60,
        jitter_min=0.3, jitter_max=1.2,
    ),
    # ~30 req/min (yt-dlp makes ~5 sub-requests per
    # extract_info, so effective YouTube load is ~160 req/min
    # per proxy — under the 300/min soft limit with 47%
    # headroom)
    YouTubeCallType.PLAYER: _BucketConfig(
        burst=4, refill_rate=30 / 60,
        jitter_min=1.0, jitter_max=3.0,
    ),
    # ~150 req/min
    YouTubeCallType.NEXT: _BucketConfig(
        burst=20, refill_rate=150 / 60,
        jitter_min=0.3, jitter_max=1.0,
    ),
    # ~90 req/min (highest ban risk)
    YouTubeCallType.HTML: _BucketConfig(
        burst=10, refill_rate=90 / 60,
        jitter_min=1.5, jitter_max=4.0,
    ),
    # ~60 req/min (plain XML, low ban risk)
    YouTubeCallType.RSS: _BucketConfig(
        burst=15, refill_rate=1.0,
        jitter_min=0.2, jitter_max=0.8,
    ),
}

_GLOBAL_CONFIG: _BucketConfig = _BucketConfig(
    # ~250 req/min aggregate (83% of 300/min
    # valid-context IP ceiling)
    burst=20, refill_rate=250 / 60,
    # jitter applied per-type only
    jitter_min=0.0, jitter_max=0.0,
)


class YouTubeRateLimiter(RateLimiter[YouTubeCallType]):
    '''
    Async-safe, singleton rate limiter shared across all YouTube callers.

    When proxies are in use, each proxy gets its own independent set of
    per-type and global buckets so that traffic through different proxies
    is rate-limited independently.

    Usage::

        limiter = YouTubeRateLimiter.get()
        proxy = await limiter.acquire(
            YouTubeCallType.BROWSE, proxy='http://proxy:8080',
        )
        cookie_file = limiter.get_cookie_file_cached(proxy)

    The :meth:`acquire` call blocks (via ``asyncio.sleep``) until both the
    per-type bucket and the global bucket have a token available, then adds
    a random jitter sleep before returning.
    '''

    _instance: ClassVar['YouTubeRateLimiter | None'] = None

    def __init__(
        self, state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        super().__init__(
            'YouTube',
            state_dir=state_dir,
            redis_dsn=redis_dsn,
        )
        self._renewal_task: asyncio.Task | None = None

    def set_proxies(self, proxies: list[str] | str | None) -> None:
        '''
        Register proxies and schedule cookie warm-up + renewal as a background
        task.  Overrides the base implementation to trigger
        :meth:`_start_cookie_services` automatically whenever the proxy pool
        changes.

        If no event loop is running (e.g. unit tests) the cookie services are
        not started; call :meth:`warm_cookie_jar` manually in that case.
        '''
        super().set_proxies(proxies)
        try:
            asyncio.get_running_loop().create_task(
                self._start_cookie_services(),
                name='youtube-cookie-init',
            )
        except RuntimeError:
            pass  # no running loop — caller must warm manually

    @classmethod
    def reset(cls) -> None:
        '''Discard the singleton and cancel any running renewal task.'''
        if cls._instance is not None:
            task = cls._instance._renewal_task
            if task is not None and not task.done():
                task.cancel()
        super().reset()

    async def acquire(
        self,
        call_type: YouTubeCallType,
        proxy: str | None = None,
    ) -> str | None:
        '''
        Wait until a request of *call_type* is
        permitted, then return the selected proxy.

        Use :meth:`get_cookie_file_cached` to obtain
        the cookie path for the returned proxy.
        '''
        return await super().acquire(
            call_type, proxy=proxy,
        )

    def get_cookie_file_cached(
        self, proxy: str | None,
    ) -> str | None:
        '''
        Return the cached cookie file path for
        *proxy* without triggering network
        acquisition.

        This is the synchronous, non-blocking
        companion to :meth:`acquire`.  Call it
        immediately after ``acquire()`` returns
        when you need the cookie path for yt-dlp's
        ``--cookiefile`` flag.

        :returns: Filesystem path to the temp
            cookie file, or ``None`` if no valid
            cached entry exists for the proxy.
        '''
        from .youtube_cookiejar import YouTubeCookieJar
        entry = (
            YouTubeCookieJar.get()._entries.get(proxy)
        )
        if entry is not None and not entry.is_expired():
            return entry.path
        return None

    @property
    def default_configs(self) -> dict[YouTubeCallType, _BucketConfig]:
        return _DEFAULT_CONFIGS

    @property
    def global_config(self) -> _BucketConfig:
        return _GLOBAL_CONFIG

    async def get_cookie_file(self, proxy: str | None = None) -> str | None:
        '''
        Return a Netscape cookie file path for *proxy*, acquiring a fresh
        unauthenticated YouTube browser session via :class:`YouTubeCookieJar`
        if no valid file is cached.

        :param proxy: Proxy URL the cookie file should be acquired through,
            or ``None`` for a direct connection.
        :returns: Filesystem path to the temp cookie file, or ``None`` if
            acquisition failed.
        '''
        from .youtube_cookiejar import YouTubeCookieJar
        return await YouTubeCookieJar.get().get_cookie_file(proxy)

    async def warm_cookie_jar(self) -> None:
        '''
        Pre-acquire cookie files for every registered proxy concurrently.

        Called automatically by :meth:`set_proxies` via a background task.
        Can also be awaited directly when the caller needs cookies to be ready
        before proceeding (e.g. in tests).  When no proxy pool is registered a
        single direct-connection cookie file is acquired instead.
        '''
        from .youtube_cookiejar import YouTubeCookieJar
        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        proxies: list[str | None] = self._proxies or [None]
        await asyncio.gather(*[jar.get_cookie_file(p) for p in proxies])

    async def _start_cookie_services(self) -> None:
        '''Warm the cookie jar then start the proactive renewal loop.'''
        await self.warm_cookie_jar()
        if self._renewal_task is None or self._renewal_task.done():
            self._renewal_task = asyncio.create_task(
                self._cookie_renewal_loop(),
                name='youtube-cookie-renewal',
            )
            _LOGGER.debug('Cookie renewal task started')

    async def _cookie_renewal_loop(self) -> None:
        '''
        Background task that proactively renews cookies for every proxy before
        they expire, eliminating the gap that would occur if expiry were only
        detected lazily on the next request.
        '''
        from .youtube_cookiejar import YouTubeCookieJar, COOKIE_TTL
        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        sleep_for: float = COOKIE_TTL - _COOKIE_RENEWAL_BUFFER
        while True:
            await asyncio.sleep(sleep_for)
            proxies: list[str | None] = self._proxies or [None]
            _LOGGER.info(
                'Pre-emptively renewing cookies for %d proxy/proxies',
                len(proxies),
            )
            await asyncio.gather(*[jar.force_renew(p) for p in proxies])
