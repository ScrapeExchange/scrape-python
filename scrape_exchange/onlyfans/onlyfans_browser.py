'''Observe public website responses in an anonymous, proxy-bound browser.'''

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any, cast
from urllib.parse import SplitResult, unquote, urlsplit

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    ProxySettings,
    Response,
    Route,
    async_playwright,
)

from scrape_exchange.onlyfans.endpoints import PROFILE_BASE_URL
from scrape_exchange.onlyfans.onlyfans_creator import (
    OnlyFansCreator,
    extract_profile,
    normalize_creator,
)
from scrape_exchange.onlyfans.onlyfans_rate_limiter import (
    OnlyFansCallType,
    OnlyFansRateLimiter,
)
from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from scrape_exchange.tiktok.tiktok_camoufox_api import camoufox_launch_options

_MAX_RESPONSE_BYTES: int = 2_000_000


class ProfileBlockedError(RuntimeError):
    '''Public access was blocked, rate-limited or required login.'''


class ProfileUnavailableError(RuntimeError):
    '''The requested public profile returned HTTP 404 or 410.'''


def browser_proxy(proxy: str | None) -> dict[str, str] | None:
    '''Convert a canonical proxy URL to Playwright's proxy settings.'''
    if proxy is None:
        return None
    parsed: SplitResult = urlsplit(proxy)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname:
        raise ValueError('Browser requires an HTTP(S) proxy')
    host: str = parsed.hostname
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


@contextlib.asynccontextmanager
async def anonymous_browser(
    proxy: str | None, settings: OnlyFansScraperSettings,
) -> AsyncIterator[BrowserContext]:
    '''No persistent context, cookies imported from disk or login action.'''
    configured_proxy: dict[str, str] | None = browser_proxy(proxy)
    browser: Browser | None = None
    async with async_playwright() as driver:
        try:
            async with asyncio.timeout(settings.browser_timeout_seconds):
                options: dict[str, Any] = await asyncio.to_thread(
                    camoufox_launch_options, configured_proxy,
                )
                browser = await driver.firefox.launch(
                    headless=True,
                    executable_path=str(options['executable_path']),
                    args=options.get('args'), env=options.get('env'),
                    proxy=cast(ProxySettings | None, configured_proxy),
                )
                context: BrowserContext = await browser.new_context(
                    locale='en-US', service_workers='block',
                )
            yield context
        finally:
            if browser is not None:
                await browser.close()


def _website_url(url: str) -> bool:
    parsed: SplitResult = urlsplit(url)
    host: str | None = urlsplit(PROFILE_BASE_URL).hostname
    return parsed.scheme == 'https' and parsed.hostname in (
        host, f'www.{host}',
    )


def _check_status(status: int) -> None:
    if status in (401, 403, 429):
        raise ProfileBlockedError(f'Anonymous access blocked (HTTP {status})')
    if status in (404, 410):
        raise ProfileUnavailableError('Public profile unavailable')
    if status >= 400:
        raise RuntimeError(f'Profile request failed (HTTP {status})')


async def fetch_profile(
    context: BrowserContext, requested: str, limiter: OnlyFansRateLimiter,
    proxy: str | None, settings: OnlyFansScraperSettings,
) -> OnlyFansCreator:
    '''Route website traffic through the limiter before it reaches the wire.'''
    username: str = normalize_creator(requested)
    page: Page = await context.new_page()
    ready: asyncio.Event = asyncio.Event()
    failures: list[Exception] = []
    creators: list[OnlyFansCreator] = []
    pending: set[asyncio.Task[None]] = set()
    active: bool = True

    async def gate(route: Route) -> None:
        try:
            if failures or route.request.resource_type in (
                'media', 'image', 'font',
            ):
                await route.abort()
                return
            if _website_url(route.request.url):
                call_type: OnlyFansCallType | None = None
                if route.request.resource_type == 'document':
                    call_type = OnlyFansCallType.CREATOR
                elif urlsplit(route.request.url).path.startswith('/api2/'):
                    call_type = OnlyFansCallType.DATA
                if call_type is not None:
                    await limiter.acquire(call_type, proxy=proxy)
            if failures:
                await route.abort()
                return
            await route.continue_()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - propagate route failures
            failures.append(exc)
            ready.set()
            with contextlib.suppress(Exception):
                await route.abort()

    async def capture(response: Response) -> None:
        try:
            _check_status(response.status)
            size: str = response.headers.get('content-length', '0')
            if size.isdigit() and int(size) > _MAX_RESPONSE_BYTES:
                raise ValueError('Public profile response is too large')
            raw: bytes = await response.body()
            if len(raw) > _MAX_RESPONSE_BYTES:
                raise ValueError('Public profile response is too large')
            payload: object = json.loads(raw)
            if isinstance(payload, dict) and payload.get('error'):
                error: object = payload['error']
                if isinstance(error, dict) and error.get('code') in (
                    401, 403, 429,
                ):
                    raise ProfileBlockedError('Public profile access denied')
            creators.append(extract_profile(payload, username))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - propagate capture failures
            failures.append(exc)
        finally:
            ready.set()

    def on_response(response: Response) -> None:
        if not active or not _website_url(response.url):
            return
        if (
            urlsplit(response.url).path.rstrip('/')
            != f'/api2/v2/users/{username}'
            or response.request.method != 'GET'
            or pending or creators or failures
        ):
            return
        task: asyncio.Task[None] = asyncio.create_task(capture(response))
        pending.add(task)
        task.add_done_callback(pending.discard)

    page.on('response', on_response)
    try:
        async with asyncio.timeout(settings.profile_timeout_seconds):
            await page.route('**/*', gate)
            try:
                response: Response | None = await page.goto(
                    f'{PROFILE_BASE_URL}/{username}',
                    wait_until='domcontentloaded',
                    timeout=settings.profile_timeout_seconds * 1000,
                )
            except Exception:
                if failures:
                    raise failures[0]
                raise
            if failures:
                raise failures[0]
            if response is not None:
                _check_status(response.status)
            if urlsplit(page.url).path.startswith(('/login', '/challenge')):
                raise ProfileBlockedError('Anonymous access requires login')
            await ready.wait()
            if failures:
                raise failures[0]
            if not _website_url(page.url) or (
                urlsplit(page.url).path.rstrip('/') != f'/{username}'
            ):
                raise ProfileBlockedError('Public profile redirected')
            if not creators:
                raise ValueError('No public profile response received')
            return creators[0]
    except ProfileBlockedError:
        call_type: OnlyFansCallType
        for call_type in OnlyFansCallType:
            await limiter.penalise(
                call_type, proxy=proxy,
                penalty_seconds=settings.blocked_cooldown_seconds,
            )
        raise
    finally:
        active = False
        page.remove_listener('response', on_response)
        task: asyncio.Task[None]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        with contextlib.suppress(Exception):
            await page.unroute('**/*', gate)
        await page.close()
