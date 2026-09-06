'''Anonymous browser contexts bound to proxies; one page per scrape.'''

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import cast
from urllib.parse import SplitResult, unquote, urlsplit

from camoufox.exceptions import InvalidIP
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    ProxySettings,
    async_playwright,
)

from scrape_exchange.tiktok.tiktok_camoufox_api import camoufox_launch_options
from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_metrics import (
    SESSION_ACTIVE,
    SESSION_POOL_SIZE,
    SESSION_WAIT,
)
from scrape_exchange.twitch.twitch_rate_limiter import (
    TwitchCallType,
    TwitchRateLimiter,
)
from scrape_exchange.watchdog import Watchdog

DIRECT_PROXY: str = 'direct'
_LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class TwitchBrowserSession:
    browser: Browser
    context: BrowserContext
    lock: asyncio.Lock


def _browser_proxy(proxy: str) -> dict[str, str] | None:
    if proxy == DIRECT_PROXY:
        return None
    parsed: SplitResult = urlsplit(proxy)
    host: str = parsed.hostname or ''
    if ':' in host:
        host = f'[{host}]'
    server: str = f'{parsed.scheme}://{host}'
    if parsed.port is not None:
        server = f'{server}:{parsed.port}'
    result: dict[str, str] = {'server': server}
    if parsed.username:
        result['username'] = unquote(parsed.username)
    if parsed.password:
        result['password'] = unquote(parsed.password)
    return result


class TwitchSessionPool:
    def __init__(
        self, proxies: list[str], settings: TwitchScraperSettings,
        limiter: TwitchRateLimiter, worker_id: str,
    ) -> None:
        self.proxies: list[str] = proxies or [DIRECT_PROXY]
        self.settings: TwitchScraperSettings = settings
        self.limiter: TwitchRateLimiter = limiter
        self.worker_id: str = worker_id
        self._sessions: dict[str, TwitchBrowserSession] = {}
        self._playwright: Playwright | None = None
        self._launch_lock: asyncio.Lock = asyncio.Lock()

    async def _launch(self, proxy: str) -> Browser:
        if self._playwright is None:
            self._playwright = await async_playwright().start()
        browser_proxy: dict[str, str] | None = _browser_proxy(proxy)
        options: dict = {}
        # IP discovery uses external services and can fail transiently.
        # The bootstrap deadline still bounds all attempts and sleeps.
        for attempt in range(3):
            try:
                options = await asyncio.to_thread(
                    camoufox_launch_options, browser_proxy,
                )
                break
            except InvalidIP:
                if attempt == 2:
                    raise
                delay: int = 2 ** (attempt + 1)
                _LOGGER.warning(
                    'Twitch public-IP lookup failed; '
                    f'retrying in {delay}s (attempt {attempt + 2}/3)',
                )
                await asyncio.sleep(delay)
        # Pass per-browser environment directly, avoiding process-wide
        # CAMOU_CONFIG changes while other proxy sessions are running.
        return await self._playwright.firefox.launch(
            headless=True, executable_path=str(options['executable_path']),
            args=options.get('args'), env=options.get('env'),
            proxy=cast(ProxySettings | None, browser_proxy),
        )

    async def _bootstrap_one(self, proxy: str) -> bool:
        browser: Browser | None = None
        try:
            await self.limiter.acquire(TwitchCallType.BOOTSTRAP, proxy=proxy)
            async with self._launch_lock, asyncio.timeout(
                self.settings.session_bootstrap_timeout_seconds,
            ):
                browser = await self._launch(proxy)
                context: BrowserContext = await browser.new_context(
                    locale='en-US', service_workers='block',
                )
            self._sessions[proxy] = TwitchBrowserSession(
                browser, context, asyncio.Lock(),
            )
            browser = None
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - close partial browser startup
            # Browser errors can include proxy credentials. Log type only.
            _LOGGER.warning(
                f'Twitch browser startup failed: {type(exc).__name__}',
            )
            return False
        finally:
            if browser is not None:
                with contextlib.suppress(Exception):
                    await browser.close()
            SESSION_POOL_SIZE.labels(self.worker_id).set(len(self._sessions))

    async def bootstrap(self) -> None:
        for proxy in self.proxies:
            Watchdog.get().touch_work()
            await self._bootstrap_one(proxy)
            Watchdog.get().touch_work()

    def ready_proxies(self) -> list[str]:
        return list(self._sessions)

    @contextlib.asynccontextmanager
    async def session_for(self, proxy: str) -> AsyncIterator[Page]:
        session: TwitchBrowserSession = self._sessions[proxy]
        start: float = time.monotonic()
        async with session.lock:
            SESSION_WAIT.labels(self.worker_id).observe(
                time.monotonic() - start,
            )
            page: Page = await session.context.new_page()
            SESSION_ACTIVE.labels(self.worker_id).inc()
            try:
                yield page
            finally:
                SESSION_ACTIVE.labels(self.worker_id).dec()
                with contextlib.suppress(Exception):
                    await page.close()

    async def quarantine(self, proxy: str) -> None:
        session: TwitchBrowserSession | None = self._sessions.pop(proxy, None)
        if session is not None:
            async with session.lock:
                with contextlib.suppress(Exception):
                    await session.context.close()
                with contextlib.suppress(Exception):
                    await session.browser.close()
        SESSION_POOL_SIZE.labels(self.worker_id).set(len(self._sessions))

    async def rebuild(self, proxy: str) -> bool:
        await self.quarantine(proxy)
        return await self._bootstrap_one(proxy)

    async def shutdown(self) -> None:
        for proxy in list(self._sessions):
            await self.quarantine(proxy)
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
