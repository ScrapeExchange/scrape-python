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

from scrape_exchange.tiktok.tiktok_metrics import (
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


class _BoundSession:
    '''One per-proxy api + its single session + a no-multiplex lock.'''

    def __init__(
        self, proxy: str, api: Any, session: object,
    ) -> None:
        self.proxy: str = proxy
        self.proxy_key: str = _proxy_key(proxy)
        self.api: Any = api
        self.session: object = session
        self.lock: asyncio.Lock = asyncio.Lock()


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
        self._jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=state_dir,
            ttl_seconds=ms_token_ttl_seconds,
        )
        # Guards the process-global CAMOU_CONFIG_* mutation +
        # launch, so per-proxy bootstraps never interleave.
        self._env_lock: asyncio.Lock = asyncio.Lock()
        self._sessions: dict[str, _BoundSession] = {}
        self._failed: set[str] = set()

    def _set_pool_gauge(self, state: str, value: int) -> None:
        SESSION_POOL_SIZE.labels(
            platform='tiktok',
            scraper=self._scraper_label,
            state=state,
            worker_id=self._worker_id,
        ).set(value)

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
        _LOGGER.info(
            'TikTok session pool bootstrap complete',
            extra={
                'requested': self._configured_proxy_count,
                'ready': len(self._sessions),
                'failed': len(self._failed),
            },
        )

    async def _bootstrap_one(self, proxy: str, camoufox: tuple) -> None:
        (
            CamoufoxTikTokApi, apply_camoufox_env, camoufox_launch_options,
        ) = camoufox
        proxy_key: str = _proxy_key(proxy)
        proxy_obj: dict[str, str] | None = _to_session_proxy(proxy)
        api: Any = None
        try:
            _LOGGER.info(
                'TikTok proxy bootstrap starting',
                extra={'proxy_endpoint': proxy_key},
            )
            await self._rate_limiter.acquire(
                TikTokCallType.BOOTSTRAP, proxy=proxy,
            )
            async with self._env_lock:
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
            session: object = sessions[0]
            self._sessions[proxy] = _BoundSession(
                proxy=proxy, api=api, session=session,
            )
            token: Any = getattr(session, 'ms_token', None)
            if token:
                self._jar.set_token(proxy_key, token)
            _LOGGER.info(
                'TikTok proxy bootstrap succeeded',
                extra={
                    'proxy_endpoint': proxy_key,
                    'has_ms_token': bool(token),
                    'reused_ms_token': bool(ms_tokens),
                },
            )
        except Exception as exc:
            _LOGGER.exception(
                'TikTok session bootstrap failed; excluding proxy',
                extra={
                    'proxy_endpoint': proxy_key,
                    'error': str(exc),
                },
            )
            self._failed.add(proxy)
            if api is not None:
                await self._close_api(api)

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
        '''
        for proxy in list(self._sessions):
            await self._refresh_one(proxy)
        self._jar.flush()

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
        self._jar.flush()
