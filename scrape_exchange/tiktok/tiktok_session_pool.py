'''
TikTokApi/Playwright session pool — one Chromium context per
proxy, owned by a single async event loop, gated by the
TikTok rate limiter.

This module deliberately avoids any auto-fallback. If TikTokApi
or Playwright fails to bootstrap a session, the proxy is
excluded from the pool and a manual restart is required to
recover.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import contextlib
import logging
import time
from typing import Any, AsyncIterator
from urllib.parse import urlparse

# TikTokApi is an optional dep (the ``tiktok`` extra). Lazy-resolve
# so the module imports cleanly in environments without it (unit
# tests use ``patch`` to substitute a MagicMock, and the production
# install always includes the extra). Resolution at construction
# time gives a clear error if the dep is missing at runtime.
try:
    from TikTokApi import TikTokApi  # type: ignore[import-not-found]
except ImportError:
    TikTokApi = None  # type: ignore[assignment,misc]

from scrape_exchange.tiktok.tiktok_metrics import (
    MS_TOKEN_AGE_SECONDS,
    SESSION_ACQUIRE_ACTIVE,
    SESSION_ACQUIRE_WAIT_SECONDS,
    SESSION_POOL_SIZE,
)
from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_session_jar import (
    TikTokSessionJar,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType


_LOGGER: logging.Logger = logging.getLogger(__name__)


class SessionUnavailable(RuntimeError):
    '''Raised when a session is requested for a failed proxy.'''


def _proxy_ip(proxy: str) -> str:
    parsed = urlparse(proxy)
    return parsed.hostname or proxy


class _BoundSession:
    '''One TikTokApi session bound to one proxy + asyncio.Lock.'''

    def __init__(
        self, proxy: str, session: object,
    ) -> None:
        self.proxy: str = proxy
        self.proxy_ip: str = _proxy_ip(proxy)
        self.session: object = session
        self.lock: asyncio.Lock = asyncio.Lock()


class TikTokSessionPool:
    '''
    Owns one Chromium context per proxy. Single event loop;
    not thread-safe. Sessions are never multiplexed across
    concurrent acquires (each ``_BoundSession`` carries its
    own ``asyncio.Lock``).
    '''

    def __init__(
        self,
        proxies: list[str],
        state_dir: str,
        ms_token_ttl_seconds: int,
        rate_limiter: TikTokRateLimiter,
        scraper_label: str,
        worker_id: str = '0',
    ) -> None:
        self._proxies: list[str] = list(proxies)
        self._state_dir: str = state_dir
        self._ttl_seconds: int = ms_token_ttl_seconds
        self._rate_limiter: TikTokRateLimiter = rate_limiter
        self._scraper_label: str = scraper_label
        self._worker_id: str = worker_id
        self._jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=state_dir,
            ttl_seconds=ms_token_ttl_seconds,
        )
        self._api: Any = None
        self._sessions: dict[str, _BoundSession] = {}
        self._failed: set[str] = set()

    async def bootstrap(self) -> None:
        '''
        Create one Chromium context per proxy. On failure, mark
        all proxies failed and let the caller decide.

        Per-proxy retry / partial-success bootstrap is deferred
        to a follow-up spec; today's policy is "all or nothing"
        because ``TikTokApi.create_sessions`` is collective.
        '''
        if TikTokApi is None:
            raise RuntimeError(
                'tiktokapi is not installed; run '
                '`uv sync --extra tiktok`',
            )
        self._api = TikTokApi()
        SESSION_POOL_SIZE.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            state='bootstrapping',
            worker_id=self._worker_id,
        ).set(len(self._proxies))
        try:
            await self._api.create_sessions(
                num_sessions=len(self._proxies),
                proxies=self._proxies,
                ms_tokens=self._jar.tokens() or None,
                sleep_after=2,
            )
        except Exception as exc:
            _LOGGER.exception(
                'TikTokApi.create_sessions failed; '
                'marking all proxies failed',
                extra={'error': str(exc)},
            )
            self._failed.update(self._proxies)
            SESSION_POOL_SIZE.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                state='failed',
                worker_id=self._worker_id,
            ).set(len(self._proxies))
            SESSION_POOL_SIZE.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                state='bootstrapping',
                worker_id=self._worker_id,
            ).set(0)
            return

        api_sessions: list[object] = list(
            getattr(self._api, 'sessions', []),
        )
        for proxy, session in zip(
            self._proxies, api_sessions,
        ):
            self._sessions[proxy] = _BoundSession(
                proxy=proxy, session=session,
            )

        SESSION_POOL_SIZE.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            state='ready',
            worker_id=self._worker_id,
        ).set(len(self._sessions))
        SESSION_POOL_SIZE.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            state='bootstrapping',
            worker_id=self._worker_id,
        ).set(0)

    def ready_proxies(self) -> list[str]:
        return list(self._sessions)

    def failed_proxies(self) -> list[str]:
        return list(self._failed)

    @contextlib.asynccontextmanager
    async def session_for(
        self, proxy: str,
    ) -> AsyncIterator[object]:
        if proxy in self._failed or proxy not in self._sessions:
            raise SessionUnavailable(
                f'No session available for proxy {proxy}',
            )
        bound: _BoundSession = self._sessions[proxy]

        wait_start: float = time.monotonic()
        await self._rate_limiter.acquire(
            TikTokCallType.API, proxy=proxy,
        )
        async with bound.lock:
            wait_seconds: float = (
                time.monotonic() - wait_start
            )
            SESSION_ACQUIRE_WAIT_SECONDS.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                worker_id=self._worker_id,
            ).observe(wait_seconds)
            SESSION_ACQUIRE_ACTIVE.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                proxy_ip=bound.proxy_ip,
                worker_id=self._worker_id,
            ).inc()
            rec = self._jar.get(bound.proxy_ip)
            if rec is not None:
                MS_TOKEN_AGE_SECONDS.labels(
                    platform='tiktok',
                    scraper=self._scraper_label,
                    proxy_ip=bound.proxy_ip,
                    worker_id=self._worker_id,
                ).set(rec.age_seconds())
            try:
                yield bound.session
            finally:
                SESSION_ACQUIRE_ACTIVE.labels(
                    platform='tiktok',
                    scraper=self._scraper_label,
                    proxy_ip=bound.proxy_ip,
                    worker_id=self._worker_id,
                ).dec()

    async def shutdown(self) -> None:
        if self._api is None:
            return
        try:
            await self._api.close_sessions()
        finally:
            with contextlib.suppress(Exception):
                await self._api.stop_playwright()
        self._jar.flush()
