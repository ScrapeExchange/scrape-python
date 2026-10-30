'''
TikTok session pool — one Camoufox (anti-detect Firefox) browser
per proxy, each driven by its own ``CamoufoxTikTokApi``, owned by a
single async event loop and gated by the TikTok rate limiter.

Camoufox bakes one fingerprint + geo per browser launch, so a shared
browser cannot give per-proxy geo coherence; the pool therefore
holds one api/browser per proxy. Bootstraps are serialized because
Camoufox's config travels through the process-global ``CAMOU_CONFIG_*``
environment (see ``apply_camoufox_env``).

This module deliberately avoids any auto-fallback. If a proxy's
browser fails to bootstrap, that proxy is excluded and the rest
proceed; recovery is a manual restart.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import contextlib
import ctypes
import hashlib
import logging
import platform
import random
import time
from typing import Any, AsyncIterator
from urllib.parse import urlparse

from scrape_exchange.browser_memory import BrowserMemory, browser_memory
from scrape_exchange.tiktok.tiktok_metrics import (
    BROWSER_MEMORY_BYTES,
    BROWSER_PROCESSES,
    BROWSER_RECYCLE_TOTAL,
    MS_TOKEN_AGE_SECONDS,
    MS_TOKEN_REFRESH_TOTAL,
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
from scrape_exchange.util import extract_proxy_port
from scrape_exchange.watchdog import Watchdog


_LOGGER: logging.Logger = logging.getLogger(__name__)

# Pages navigated to when actively refreshing a stale ms_token.
_REFRESH_URLS: list[str] = [
    'https://www.tiktok.com/foryou',
    'https://www.tiktok.com',
]

_WATCHDOG_TOUCH_INTERVAL_SECONDS: float = 30.0

# Firefox prefs for every session browser: no autoplay (5 = block
# audio and video), so a session page never buffers feed videos even
# when TIKTOK_SUPPRESS_RESOURCE_TYPES lets media load. Fission and
# dom.ipc.processCount were measured to make no difference: under
# Playwright each session's single tab runs in one content process
# already.
FIREFOX_MEMORY_PREFS: dict[str, int] = {
    'media.autoplay.default': 5,
}
DIRECT_SESSION_PROXY: str = '__direct__'

_CAMOUFOX_RUNTIME_LIBS: dict[str, str] = {
    'libasound.so.2': 'libasound2t64',
    'libatk-1.0.so.0': 'libatk1.0-0t64',
    'libatk-bridge-2.0.so.0': 'libatk-bridge2.0-0t64',
    'libcairo.so.2': 'libcairo2',
    'libcups.so.2': 'libcups2t64',
    'libdbus-1.so.3': 'libdbus-1-3',
    'libdbus-glib-1.so.2': 'libdbus-glib-1-2',
    'libfontconfig.so.1': 'libfontconfig1',
    'libfreetype.so.6': 'libfreetype6',
    'libgbm.so.1': 'libgbm1',
    'libglib-2.0.so.0': 'libglib2.0-0t64',
    'libgdk-3.so.0': 'libgtk-3-0t64',
    'libgtk-3.so.0': 'libgtk-3-0t64',
    'libnss3.so': 'libnss3',
    'libpango-1.0.so.0': 'libpango-1.0-0',
    'libpangocairo-1.0.so.0': 'libpangocairo-1.0-0',
    'libX11-xcb.so.1': 'libx11-xcb1',
    'libxcb-shm.so.0': 'libxcb-shm0',
    'libXcomposite.so.1': 'libxcomposite1',
    'libXdamage.so.1': 'libxdamage1',
    'libXext.so.6': 'libxext6',
    'libXfixes.so.3': 'libxfixes3',
    'libXrandr.so.2': 'libxrandr2',
    'libXrender.so.1': 'libxrender1',
    'libXss.so.1': 'libxss1',
    'libXt.so.6': 'libxt6t64',
}


class SessionUnavailable(RuntimeError):
    '''Raised when a session is requested for a failed proxy.'''


def _missing_camoufox_runtime_libs() -> list[str]:
    '''Return Debian package names for missing Linux browser libs.'''
    if platform.system() != 'Linux':
        return []
    missing: set[str] = set()
    for lib, package in _CAMOUFOX_RUNTIME_LIBS.items():
        try:
            ctypes.CDLL(lib)
        except OSError:
            missing.add(package)
    return sorted(missing)


def _ensure_camoufox_runtime_libs() -> None:
    '''Fail early with host-package guidance when Camoufox cannot run.'''
    missing: list[str] = _missing_camoufox_runtime_libs()
    if not missing:
        return
    packages: str = ' '.join(missing)
    raise RuntimeError(
        'Camoufox browser runtime dependencies are missing on this '
        'host. Install them with: '
        f'sudo apt-get update && sudo apt-get install -y {packages}',
    )


async def _await_with_watchdog(
    awaitable: Any,
    *,
    interval_seconds: float = _WATCHDOG_TOUCH_INTERVAL_SECONDS,
) -> Any:
    '''
    Await a long-running operation while keeping the work watchdog
    fresh. Used for browser/session bootstrap, where slow proxies can
    legitimately take longer than the global work timeout.
    '''
    task: asyncio.Task = asyncio.create_task(awaitable)
    try:
        while True:
            Watchdog.get().touch_work()
            try:
                return await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=interval_seconds,
                )
            except TimeoutError:
                continue
    except BaseException:
        if not task.done():
            task.cancel()
        raise


def _proxy_key(proxy: str) -> str:
    '''Return a credential-safe identity for one proxy endpoint.'''
    if proxy == DIRECT_SESSION_PROXY:
        return 'direct'
    parsed = urlparse(proxy)
    host: str = parsed.hostname or proxy
    port: int | None = parsed.port
    endpoint: str = f'{host}:{port}' if port is not None else host
    if parsed.username is None and parsed.password is None:
        return endpoint
    credentials: str = f'{parsed.username or ""}:{parsed.password or ""}'
    digest: str = hashlib.sha256(credentials.encode()).hexdigest()[:12]
    return f'{endpoint}#{digest}'


def _to_playwright_proxy(proxy: str) -> dict[str, str]:
    '''
    Convert a canonical proxy URL string (``http://user:pass@host:
    port``, the format produced by ``scrape_exchange.proxy_loader``)
    into the proxy *object* Playwright's ``launch(proxy=...)``
    requires. TikTokApi forwards whatever it is given straight to
    Playwright, which rejects bare strings.
    '''
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


def _load_camoufox() -> tuple:
    '''Lazy-import the Camoufox layer so startup reports missing
    TikTok runtime dependencies clearly.'''
    try:
        from scrape_exchange.tiktok.tiktok_camoufox_api import (
            CamoufoxTikTokApi,
            apply_camoufox_env,
            camoufox_launch_options,
        )
    except ImportError as exc:
        raise RuntimeError(
            'TikTok runtime dependencies are not installed; run '
            '`uv sync`',
        ) from exc
    return (
        CamoufoxTikTokApi, apply_camoufox_env, camoufox_launch_options,
    )


async def _start_playwright() -> Any:
    '''Start a Playwright driver. Imported lazily, like Camoufox.'''
    from playwright.async_api import async_playwright
    return await async_playwright().start()


class _BoundSession:
    '''One per-proxy api + its single session + a no-multiplex lock.'''

    def __init__(
        self, proxy: str, api: Any, session: object,
    ) -> None:
        self.proxy: str = proxy
        self.proxy_key: str = _proxy_key(proxy)
        self.lock: asyncio.Lock = asyncio.Lock()
        self.replace(api, session)

    def replace(self, api: Any, session: object) -> None:
        '''Bind a (new) browser. Its age limit gets +/-10% jitter so
        browsers launched together are not recycled together.'''
        self.api: Any = api
        self.session: object = session
        self.created_at: float = time.monotonic()
        self.age_jitter: float = random.uniform(0.9, 1.1)

    def browser_alive(self) -> bool:
        browser: Any | None = getattr(self.api, 'browser', None)
        if browser is None:
            return False
        return bool(browser.is_connected())


class TikTokSessionPool:
    '''
    Owns one Camoufox browser + ``CamoufoxTikTokApi`` per proxy.
    Single event loop; not thread-safe. Sessions are never
    multiplexed across concurrent acquires (each ``_BoundSession``
    carries its own ``asyncio.Lock``).
    '''

    def __init__(
        self,
        proxies: list[str],
        state_dir: str,
        ms_token_ttl_seconds: int,
        rate_limiter: TikTokRateLimiter,
        scraper_label: str,
        api_call_type: TikTokCallType,
        worker_id: str = '0',
        refresh_fraction: float = 0.75,
        bootstrap_timeout_ms: int = 90_000,
        suppress_resource_types: list[str] | None = None,
        browser_webgl: bool = False,
        browser_max_age_seconds: float = 0.0,
    ) -> None:
        self._configured_proxy_count: int = len(proxies)
        self._proxies: list[str] = (
            list(proxies) if proxies else [DIRECT_SESSION_PROXY]
        )
        self._state_dir: str = state_dir
        self._ttl_seconds: int = ms_token_ttl_seconds
        self._rate_limiter: TikTokRateLimiter = rate_limiter
        self._scraper_label: str = scraper_label
        self._api_call_type: TikTokCallType = api_call_type
        self._worker_id: str = worker_id
        self._refresh_fraction: float = refresh_fraction
        self._bootstrap_timeout_ms: int = bootstrap_timeout_ms
        # None, not [], when empty: TikTokApi installs a route handler
        # for any list, which would intercept every request for nothing.
        self._suppress_resource_types: list[str] | None = (
            list(suppress_resource_types)
            if suppress_resource_types else None
        )
        self._jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=state_dir,
            ttl_seconds=ms_token_ttl_seconds,
        )
        # Guards the process-global CAMOU_CONFIG_* mutation +
        # launch, so per-proxy bootstraps never interleave.
        self._env_lock: asyncio.Lock = asyncio.Lock()
        self._sessions: dict[str, _BoundSession] = {}
        self._failed: set[str] = set()
        self._browser_webgl: bool = browser_webgl
        # Browsers grow from ~0.7 to ~1.5 GiB within an hour of
        # scraping; recycle them past this age. 0 disables.
        self._browser_max_age_seconds: float = browser_max_age_seconds
        # One Playwright driver (a ~75 MiB Node.js process) shared by
        # every browser of this worker instead of one per proxy.
        self._playwright: Any | None = None

    def _set_pool_gauge(self, state: str, value: int) -> None:
        SESSION_POOL_SIZE.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            state=state,
            worker_id=self._worker_id,
        ).set(value)

    async def _driver(self) -> Any:
        '''The worker's shared Playwright driver. Restarted when no
        session is alive, so a crashed driver cannot fail every later
        bootstrap and rebuild.'''
        if self._playwright is not None and not self._sessions:
            await self._stop_driver()
        if self._playwright is None:
            self._playwright = await _start_playwright()
        return self._playwright

    async def _stop_driver(self) -> None:
        driver: Any | None = self._playwright
        self._playwright = None
        if driver is not None:
            with contextlib.suppress(Exception):
                await driver.stop()

    def _firefox_prefs(self, opts: dict) -> dict[str, Any]:
        '''Firefox prefs for one browser. Camoufox's own prefs are
        only its WebGL ones (webgl.force-enabled, webgl.enable-webgl2):
        without them WebGL is unavailable, with them a browser was
        measured at 75-140 MiB more, so they are opt-in.'''
        prefs: dict[str, Any] = {}
        if self._browser_webgl:
            prefs.update(opts.get('firefox_user_prefs') or {})
        prefs.update(FIREFOX_MEMORY_PREFS)
        return prefs

    def _record_browser_memory(self) -> None:
        '''Publish this worker's browser and driver memory.'''
        usage: BrowserMemory | None = browser_memory()
        if usage is None:
            return
        kind: str
        bytes_: int
        processes: int
        for kind, bytes_, processes in (
            ('browser', usage.browser_bytes, usage.browser_processes),
            ('driver', usage.driver_bytes, usage.driver_processes),
        ):
            BROWSER_MEMORY_BYTES.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                kind=kind,
                worker_id=self._worker_id,
            ).set(bytes_)
            BROWSER_PROCESSES.labels(
                platform='tiktok',
                scraper=self._scraper_label,
                kind=kind,
                worker_id=self._worker_id,
            ).set(processes)

    async def bootstrap(self) -> None:
        '''
        Launch one Camoufox browser per proxy, serially. A proxy that
        fails to bootstrap is excluded; the rest still come up
        (partial success).
        '''
        _ensure_camoufox_runtime_libs()
        camoufox = _load_camoufox()
        _LOGGER.info(
            'TikTok session pool bootstrap starting',
            extra={'requested': self._configured_proxy_count},
        )
        self._set_pool_gauge('bootstrapping', len(self._proxies))
        for proxy in self._proxies:
            Watchdog.get().touch_work()
            await self._bootstrap_one(proxy, camoufox)
            Watchdog.get().touch_work()
        self._jar.flush()
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        self._set_pool_gauge('bootstrapping', 0)
        self._record_browser_memory()
        _LOGGER.info(
            'TikTok session pool bootstrap complete',
            extra={
                'requested': self._configured_proxy_count,
                'ready': len(self._sessions),
                'failed': len(self._failed),
            },
        )

    async def _launch(
        self, proxy: str, camoufox: tuple,
    ) -> tuple[Any, object, bool]:
        '''Launch one browser + session for *proxy*, reusing the
        proxy's stored ms_token when it has not expired. Returns the
        api, its session and whether the token was reused; on failure
        closes what it started and raises.'''
        (
            CamoufoxTikTokApi, apply_camoufox_env, camoufox_launch_options,
        ) = camoufox
        proxy_key: str = _proxy_key(proxy)
        proxy_obj: dict[str, str] | None = _to_session_proxy(proxy)
        api: Any = None
        try:
            await self._rate_limiter.acquire(
                TikTokCallType.BOOTSTRAP, proxy=proxy,
            )
            async with self._env_lock:
                driver: Any = await self._driver()
                opts: dict = camoufox_launch_options(proxy_obj)
                apply_camoufox_env(opts.get('env') or {})
                api = CamoufoxTikTokApi()
                api.set_rate_limiter(
                    self._rate_limiter, proxy, self._api_call_type,
                )
                if hasattr(api, 'set_metrics_context'):
                    api.set_metrics_context(
                        self._scraper_label, self._worker_id,
                    )
                rec = self._jar.get(proxy_key)
                ms_tokens: list[str] | None = (
                    [rec.ms_token]
                    if rec is not None and not rec.is_expired()
                    else None
                )
                await _await_with_watchdog(
                    api.create_sessions(
                        num_sessions=1,
                        browser='firefox',
                        executable_path=str(opts['executable_path']),
                        override_browser_args=opts.get('args'),
                        firefox_user_prefs=self._firefox_prefs(opts),
                        suppress_resource_load_types=(
                            self._suppress_resource_types
                        ),
                        playwright=driver,
                        proxies=[proxy_obj],
                        ms_tokens=ms_tokens,
                        sleep_after=2,
                        timeout=self._bootstrap_timeout_ms,
                    ),
                )
            sessions: list[object] = list(
                getattr(api, 'sessions', []),
            )
            if not sessions:
                raise RuntimeError(
                    'create_sessions produced no session',
                )
        except Exception:
            if api is not None:
                await self._close_api(api)
            raise
        session: object = sessions[0]
        token: Any = getattr(session, 'ms_token', None)
        if token:
            self._jar.set_token(proxy_key, token)
        return api, session, bool(ms_tokens)

    async def _bootstrap_one(self, proxy: str, camoufox: tuple) -> None:
        proxy_key: str = _proxy_key(proxy)
        _LOGGER.info(
            'TikTok proxy bootstrap starting',
            extra={'proxy_endpoint': proxy_key},
        )
        try:
            api: Any
            session: object
            reused: bool
            api, session, reused = await self._launch(proxy, camoufox)
        except Exception as exc:
            _LOGGER.exception(
                'TikTok session bootstrap failed; excluding proxy',
                extra={
                    'proxy_endpoint': proxy_key,
                    'error': str(exc),
                },
            )
            self._failed.add(proxy)
            return
        self._sessions[proxy] = _BoundSession(
            proxy=proxy, api=api, session=session,
        )
        _LOGGER.info(
            'TikTok proxy bootstrap succeeded',
            extra={
                'proxy_endpoint': proxy_key,
                'has_ms_token': bool(getattr(session, 'ms_token', None)),
                'reused_ms_token': reused,
            },
        )

    async def _recycle(self, proxy: str, reason: str) -> bool:
        '''Replace *proxy*'s browser without a gap: launch the new
        browser while the old one keeps serving, swap it in under the
        session lock (a worker mid-scrape finishes first), then close
        the old one. The ms_token is kept: this is a healthy identity.
        If the launch fails the old browser stays in service.'''
        bound: _BoundSession | None = self._sessions.get(proxy)
        if bound is None:
            return False
        _LOGGER.info(
            'TikTok browser recycle starting',
            extra={'proxy_endpoint': bound.proxy_key, 'reason': reason},
        )
        try:
            api: Any
            session: object
            api, session, _ = await self._launch(proxy, _load_camoufox())
        except Exception as exc:
            _LOGGER.warning(
                'TikTok browser recycle failed; keeping old browser',
                extra={
                    'proxy_endpoint': bound.proxy_key,
                    'reason': reason,
                    'error': str(exc),
                },
            )
            self._record_recycle(reason, 'failure')
            return False
        if self._sessions.get(proxy) is not bound:
            # Quarantined while the new browser launched.
            await self._close_api(api)
            return False
        async with bound.lock:
            old_api: Any = bound.api
            bound.replace(api, session)
        await self._close_api(old_api)
        self._record_recycle(reason, 'success')
        _LOGGER.info(
            'TikTok browser recycle complete',
            extra={'proxy_endpoint': bound.proxy_key, 'reason': reason},
        )
        return True

    def _record_recycle(self, reason: str, outcome: str) -> None:
        BROWSER_RECYCLE_TOTAL.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            reason=reason,
            outcome=outcome,
            worker_id=self._worker_id,
        ).inc()

    async def recycle_browsers(self) -> None:
        '''Replace every browser that has disconnected, and the one
        browser furthest past the age limit. One age recycle per call
        keeps a launch-time cohort from all restarting at once.'''
        bound: _BoundSession
        for bound in list(self._sessions.values()):
            if not bound.browser_alive():
                _LOGGER.warning(
                    'TikTok session browser disconnected',
                    extra={'proxy_endpoint': bound.proxy_key},
                )
                await self._recycle(bound.proxy, 'disconnected')
        if self._browser_max_age_seconds <= 0:
            return
        now: float = time.monotonic()
        overdue: list[tuple[float, str]] = [
            (
                (now - b.created_at)
                - self._browser_max_age_seconds * b.age_jitter,
                b.proxy,
            )
            for b in self._sessions.values()
        ]
        overdue = [item for item in overdue if item[0] >= 0]
        if overdue:
            await self._recycle(max(overdue)[1], 'age')

    @staticmethod
    async def _close_api(api: Any) -> None:
        with contextlib.suppress(Exception):
            await api.close_sessions()
        with contextlib.suppress(Exception):
            await api.stop_playwright()

    def ready_proxies(self) -> list[str]:
        return list(self._sessions)

    def failed_proxies(self) -> list[str]:
        return list(self._failed)

    async def gate_api_request(self, proxy: str) -> None:
        '''Consume one API token for a direct non-TikTokApi request.'''
        await self._rate_limiter.acquire(
            self._api_call_type, proxy=proxy,
        )

    async def rebuild(self, proxy: str) -> bool:
        '''Replace a blocked proxy's browser with a fresh session.

        The old ms_token is discarded so a poisoned identity is not
        immediately restored into the new browser. Returns whether the
        replacement session bootstrapped successfully.
        '''
        proxy_key: str = _proxy_key(proxy)
        _LOGGER.info(
            'TikTok proxy session rebuild starting',
            extra={'proxy_endpoint': proxy_key},
        )
        await self.quarantine(proxy)
        await self._bootstrap_one(proxy, _load_camoufox())
        rebuilt: bool = proxy in self._sessions
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        self._record_browser_memory()
        _LOGGER.info(
            'TikTok proxy session rebuild complete',
            extra={
                'proxy_endpoint': proxy_key,
                'rebuilt': rebuilt,
                'ready': len(self._sessions),
                'failed': len(self._failed),
            },
        )
        return rebuilt

    async def quarantine(self, proxy: str) -> None:
        '''Close and forget a burned proxy session.

        Bot-detected sessions should cool down without their old
        browser or ms_token being reused. The proxy itself may be
        rebuilt later by its owning worker.
        '''
        proxy_key: str = _proxy_key(proxy)
        had_session: bool = proxy in self._sessions
        _LOGGER.info(
            'TikTok proxy session quarantine starting',
            extra={
                'proxy_endpoint': proxy_key,
                'had_session': had_session,
            },
        )
        bound: _BoundSession | None = self._sessions.get(proxy)
        if bound is not None:
            async with bound.lock:
                self._sessions.pop(proxy, None)
                await self._close_api(bound.api)

        self._failed.discard(proxy)
        self._jar.delete_token(proxy_key)
        self._jar.flush()
        self._set_pool_gauge('ready', len(self._sessions))
        self._set_pool_gauge('failed', len(self._failed))
        self._record_browser_memory()
        _LOGGER.info(
            'TikTok proxy session quarantine complete',
            extra={
                'proxy_endpoint': proxy_key,
                'ready': len(self._sessions),
                'failed': len(self._failed),
            },
        )

    @contextlib.asynccontextmanager
    async def session_for(
        self, proxy: str,
    ) -> AsyncIterator[Any]:
        '''
        Lock the proxy's session and yield its ``CamoufoxTikTokApi``.
        Individual HTTP fetches are rate limited by the API instance,
        including pagination and TikTokApi's internal retries.
        '''
        if proxy in self._failed or proxy not in self._sessions:
            raise SessionUnavailable(
                f'No session available for proxy {proxy}',
            )
        bound: _BoundSession = self._sessions[proxy]

        wait_start: float = time.monotonic()
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
                proxy_ip=bound.proxy_key,
                proxy_port=(
                    extract_proxy_port(bound.proxy)
                    if bound.proxy != DIRECT_SESSION_PROXY
                    else 'none'
                ),
                worker_id=self._worker_id,
            ).inc()
            rec = self._jar.get(bound.proxy_key)
            if rec is not None:
                MS_TOKEN_AGE_SECONDS.labels(
                    platform='tiktok',
                    scraper=self._scraper_label,
                    proxy_ip=bound.proxy_key,
                    proxy_port=(
                        extract_proxy_port(bound.proxy)
                        if bound.proxy != DIRECT_SESSION_PROXY
                        else 'none'
                    ),
                    worker_id=self._worker_id,
                ).set(rec.age_seconds())
            try:
                yield bound.api
            finally:
                SESSION_ACQUIRE_ACTIVE.labels(
                    platform='tiktok',
                    scraper=self._scraper_label,
                    proxy_ip=bound.proxy_key,
                    proxy_port=(
                        extract_proxy_port(bound.proxy)
                        if bound.proxy != DIRECT_SESSION_PROXY
                        else 'none'
                    ),
                    worker_id=self._worker_id,
                ).dec()

    def _record_refresh(self, outcome: str) -> None:
        MS_TOKEN_REFRESH_TOTAL.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            outcome=outcome,
            worker_id=self._worker_id,
        ).inc()

    async def refresh_tokens(self) -> None:
        '''
        One refresh pass over every ready proxy. Each proxy is
        handled independently; a failure on one is recorded and
        does not abort the others. Persists the jar once at the end.
        Starts with :meth:`recycle_browsers`, so dead and over-age
        browsers are replaced on the same cadence.
        '''
        await self.recycle_browsers()
        for proxy in list(self._sessions):
            await self._refresh_one(proxy)
        self._jar.flush()
        self._record_browser_memory()

    async def _refresh_one(self, proxy: str) -> None:
        '''
        Refresh one proxy's ms_token. Passive every cycle: read the
        browser-context msToken (TikTok rotates it on responses) and
        persist if it changed. Active only when the stored token's
        age exceeds ``refresh_fraction * ttl``: navigate the page to
        force a new token first. The active navigation is a real
        TikTok request, so it gates on the BOOTSTRAP bucket and is
        acquired before taking the no-multiplex lock (mirrors
        ``session_for``); the passive cookie read hits no network.
        '''
        bound: _BoundSession | None = self._sessions.get(proxy)
        if bound is None:
            return
        proxy_key: str = bound.proxy_key
        rec = self._jar.get(proxy_key)
        threshold: float = (
            self._refresh_fraction * self._ttl_seconds
        )
        stale: bool = (
            rec is None or rec.age_seconds() >= threshold
        )
        old_token: str | None = (
            rec.ms_token if rec is not None else None
        )
        try:
            if stale:
                await self._rate_limiter.acquire(
                    TikTokCallType.BOOTSTRAP, proxy=proxy,
                )
            async with bound.lock:
                if stale:
                    await bound.session.page.goto(
                        random.choice(_REFRESH_URLS),
                    )
                cookies: dict = (
                    await bound.api.get_session_cookies(
                        bound.session,
                    )
                )
                token: Any = cookies.get('msToken')
                if token and token != old_token:
                    self._jar.set_token(proxy_key, token)
                    bound.session.ms_token = token
            self._record_refresh('success')
        except Exception as exc:
            _LOGGER.warning(
                'TikTok ms_token refresh failed',
                extra={
                    'proxy_endpoint': proxy_key, 'error': str(exc),
                },
            )
            self._record_refresh('failure')
        finally:
            rec2 = self._jar.get(proxy_key)
            if rec2 is not None:
                MS_TOKEN_AGE_SECONDS.labels(
                    platform='tiktok',
                    scraper=self._scraper_label,
                    proxy_ip=proxy_key,
                    proxy_port=(
                        extract_proxy_port(bound.proxy)
                        if bound.proxy != DIRECT_SESSION_PROXY
                        else 'none'
                    ),
                    worker_id=self._worker_id,
                ).set(rec2.age_seconds())

    async def run_refresh_loop(
        self, interval_seconds: float,
    ) -> None:
        '''
        Self-driving refresh loop: sleep ``interval_seconds`` then
        run one :meth:`refresh_tokens` pass, until cancelled. A
        future scraper tool owns the event loop and schedules this
        via ``asyncio.create_task``.
        '''
        while True:
            await asyncio.sleep(interval_seconds)
            await self.refresh_tokens()

    async def shutdown(self) -> None:
        for bound in list(self._sessions.values()):
            await self._close_api(bound.api)
        await self._stop_driver()
        self._jar.flush()
