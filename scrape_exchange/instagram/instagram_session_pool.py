'''
Instagram session pool.

Owns one Camoufox browser context and page per proxy. The pool mirrors the
TikTok session-pool lifecycle, but avoids TikTokApi and token jars.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import contextlib
import logging
import time

from dataclasses import dataclass
from typing import Any, AsyncIterator
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrape_exchange.instagram.instagram_metrics import (
    SESSION_ACQUIRE_ACTIVE,
    SESSION_ACQUIRE_WAIT_SECONDS,
    SESSION_POOL_SIZE,
)
from scrape_exchange.instagram.instagram_rate_limiter import (
    InstagramCallType,
    InstagramRateLimiter,
)
from scrape_exchange.tiktok.tiktok_camoufox_api import (
    apply_camoufox_env,
    camoufox_launch_options,
)
from scrape_exchange.util import extract_proxy_port
from scrape_exchange.watchdog import Watchdog


_LOGGER: logging.Logger = logging.getLogger(__name__)

DIRECT_SESSION_PROXY: str = '__direct__'
BOOTSTRAP_URL: str = 'https://www.instagram.com/'


class SessionUnavailable(RuntimeError):
    '''Raised when a proxy has no ready browser session.'''


@dataclass
class InstagramBrowserSession:
    '''One browser session bound to one proxy.'''

    proxy: str
    proxy_key: str
    playwright: Any
    browser: Any
    context: Any
    page: Any
    lock: asyncio.Lock


def _proxy_key(proxy: str) -> str:
    if proxy == DIRECT_SESSION_PROXY:
        return 'direct'
    parsed = urlparse(proxy)
    host: str = parsed.hostname or proxy
    return f'{host}:{parsed.port}' if parsed.port is not None else host


def _to_playwright_proxy(proxy: str) -> dict[str, str]:
    parsed = urlparse(proxy)
    server: str = f'{parsed.scheme}://{parsed.hostname}'
    if parsed.port is not None:
        server = f'{server}:{parsed.port}'
    obj: dict[str, str] = {'server': server}
    if parsed.username:
        obj['username'] = parsed.username
    if parsed.password:
        obj['password'] = parsed.password
    return obj


def _to_session_proxy(proxy: str) -> dict[str, str] | None:
    if proxy == DIRECT_SESSION_PROXY:
        return None
    return _to_playwright_proxy(proxy)


class InstagramSessionPool:
    '''Own one Camoufox browser session per proxy.'''

    def __init__(
        self,
        proxies: list[str],
        state_dir: str,
        rate_limiter: InstagramRateLimiter,
        scraper_label: str,
        worker_id: str = '0',
        bootstrap_timeout_ms: int = 90_000,
    ) -> None:
        self._configured_proxy_count: int = len(proxies)
        self._proxies: list[str] = (
            list(proxies) if proxies else [DIRECT_SESSION_PROXY]
        )
        self._state_dir: str = state_dir
        self._rate_limiter: InstagramRateLimiter = rate_limiter
        self._scraper_label: str = scraper_label
        self._worker_id: str = worker_id
        self._bootstrap_timeout_ms: int = bootstrap_timeout_ms
        self._env_lock: asyncio.Lock = asyncio.Lock()
        self._sessions: dict[str, InstagramBrowserSession] = {}
        self._failed: set[str] = set()

    def _set_pool_gauge(self, state: str, value: int) -> None:
        SESSION_POOL_SIZE.labels(
            platform='instagram',
            scraper=self._scraper_label,
            state=state,
            worker_id=self._worker_id,
        ).set(value)

    async def bootstrap(self) -> None:
        '''Launch one browser session per proxy.'''
        _LOGGER.info(
            'Instagram session pool bootstrap starting',
            extra={'requested': self._configured_proxy_count},
        )
        self._set_pool_gauge('bootstrapping', len(self._proxies))
        for proxy in self._proxies:
            Watchdog.get().touch_work()
            await self._bootstrap_one(proxy)
            Watchdog.get().touch_work()
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        self._set_pool_gauge('bootstrapping', 0)
        _LOGGER.info(
            'Instagram session pool bootstrap complete',
            extra={
                'requested': self._configured_proxy_count,
                'ready': len(self._sessions),
                'failed': len(self._failed),
            },
        )

    async def _bootstrap_one(self, proxy: str) -> None:
        proxy_key: str = _proxy_key(proxy)
        proxy_obj: dict[str, str] | None = _to_session_proxy(proxy)
        playwright: Any = None
        browser: Any = None
        context: Any = None
        try:
            await self._rate_limiter.acquire(
                InstagramCallType.BOOTSTRAP, proxy=proxy,
            )
            async with self._env_lock:
                opts: dict = camoufox_launch_options(proxy_obj)
                apply_camoufox_env(opts.get('env') or {})
                playwright = await async_playwright().start()
                browser = await playwright.firefox.launch(
                    headless=True,
                    executable_path=str(opts['executable_path']),
                    args=opts.get('args'),
                    proxy=proxy_obj,
                )
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_navigation_timeout(
                self._bootstrap_timeout_ms,
            )
            await page.goto(
                BOOTSTRAP_URL,
                wait_until='domcontentloaded',
                timeout=self._bootstrap_timeout_ms,
            )
            self._sessions[proxy] = InstagramBrowserSession(
                proxy=proxy,
                proxy_key=proxy_key,
                playwright=playwright,
                browser=browser,
                context=context,
                page=page,
                lock=asyncio.Lock(),
            )
            _LOGGER.info(
                'Instagram proxy bootstrap succeeded',
                extra={'proxy_endpoint': proxy_key},
            )
        except Exception as exc:
            _LOGGER.exception(
                'Instagram session bootstrap failed; excluding proxy',
                extra={'proxy_endpoint': proxy_key, 'error': str(exc)},
            )
            self._failed.add(proxy)
            await self._close_parts(playwright, browser, context)

    @staticmethod
    async def _close_parts(
        playwright: Any,
        browser: Any,
        context: Any,
    ) -> None:
        with contextlib.suppress(Exception):
            if context is not None:
                await context.close()
        with contextlib.suppress(Exception):
            if browser is not None:
                await browser.close()
        with contextlib.suppress(Exception):
            if playwright is not None:
                await playwright.stop()

    @staticmethod
    async def _close_session(session: InstagramBrowserSession) -> None:
        await InstagramSessionPool._close_parts(
            session.playwright, session.browser, session.context,
        )

    def ready_proxies(self) -> list[str]:
        return list(self._sessions)

    def failed_proxies(self) -> list[str]:
        return list(self._failed)

    async def gate_profile_request(self, proxy: str) -> None:
        await self._rate_limiter.acquire(
            InstagramCallType.CREATOR, proxy=proxy,
        )

    async def quarantine(self, proxy: str) -> None:
        proxy_key: str = _proxy_key(proxy)
        bound: InstagramBrowserSession | None = self._sessions.get(proxy)
        if bound is not None:
            async with bound.lock:
                self._sessions.pop(proxy, None)
                await self._close_session(bound)
        self._failed.discard(proxy)
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        _LOGGER.info(
            'Instagram proxy session quarantine complete',
            extra={'proxy_endpoint': proxy_key},
        )

    async def rebuild(self, proxy: str) -> bool:
        await self.quarantine(proxy)
        await self._bootstrap_one(proxy)
        rebuilt: bool = proxy in self._sessions
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        return rebuilt

    @contextlib.asynccontextmanager
    async def session_for(
        self, proxy: str,
    ) -> AsyncIterator[InstagramBrowserSession]:
        if proxy in self._failed or proxy not in self._sessions:
            raise SessionUnavailable(
                f'No session available for proxy {proxy}',
            )
        bound: InstagramBrowserSession = self._sessions[proxy]
        wait_start: float = time.monotonic()
        async with bound.lock:
            wait_seconds: float = time.monotonic() - wait_start
            SESSION_ACQUIRE_WAIT_SECONDS.labels(
                platform='instagram',
                scraper=self._scraper_label,
                worker_id=self._worker_id,
            ).observe(wait_seconds)
            SESSION_ACQUIRE_ACTIVE.labels(
                platform='instagram',
                scraper=self._scraper_label,
                proxy_ip=bound.proxy_key,
                proxy_port=(
                    extract_proxy_port(bound.proxy)
                    if bound.proxy != DIRECT_SESSION_PROXY
                    else 'none'
                ),
                worker_id=self._worker_id,
            ).inc()
            try:
                yield bound
            finally:
                SESSION_ACQUIRE_ACTIVE.labels(
                    platform='instagram',
                    scraper=self._scraper_label,
                    proxy_ip=bound.proxy_key,
                    proxy_port=(
                        extract_proxy_port(bound.proxy)
                        if bound.proxy != DIRECT_SESSION_PROXY
                        else 'none'
                    ),
                    worker_id=self._worker_id,
                ).dec()

    async def shutdown(self) -> None:
        for bound in list(self._sessions.values()):
            await self._close_session(bound)
