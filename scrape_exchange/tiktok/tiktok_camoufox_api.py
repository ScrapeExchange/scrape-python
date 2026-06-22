'''
CamoufoxTikTokApi — a TikTokApi subclass that runs both the X-Bogus
signing and the data fetch in Camoufox's *main* world.

Camoufox isolates page scripts from the main world. TikTokApi signs
with ``window.byted_acrawler.frontierSign`` (a main-world global) and
fetches data with the page's ``fetch`` — and the live site's webmssdk
hooks that ``fetch`` to inject the ``X-Gnarly`` signature every data
endpoint now requires. Run in Camoufox's *isolated* world (the
default for ``page.evaluate``), the signer is invisible and the fetch
bypasses the hook, so ``X-Gnarly`` is missing and TikTok returns an
empty ``userInfo``. Both overrides below move execution into the main
world (Camoufox's ``mw:`` prefix) to restore correct, fully-signed
requests.

This module imports TikTokApi at import time (the ``tiktok`` extra);
import it lazily from callers that must stay usable without the
extra.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import json
import os
import random
import time
from typing import Any

from camoufox import launch_options
from playwright.async_api import Error as PlaywrightError
from TikTokApi import TikTokApi
from TikTokApi.api.comment import Comment
from TikTokApi.api.hashtag import Hashtag
from TikTokApi.api.playlist import Playlist
from TikTokApi.api.search import Search
from TikTokApi.api.sound import Sound
from TikTokApi.api.trending import Trending
from TikTokApi.api.user import User
from TikTokApi.api.video import Video
from TikTokApi.tiktok import (
    TikTokPlaywrightSession,
    stealth_async,
    urlparse,
)

from scrape_exchange.tiktok.tiktok_rate_limiter import TikTokRateLimiter
from scrape_exchange.tiktok.tiktok_error_classification import (
    classify_tiktok_error,
)
from scrape_exchange.tiktok.tiktok_metrics import (
    API_CALL_DURATION_SECONDS,
    API_CALL_TOTAL,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType

# Firefox raises this when an in-flight navigation is superseded by
# another one. TikTok's own client-side routing fires between the two
# bootstrap gotos, so the second goto's request is aborted — the page
# is still navigating to TikTok, so this is not a real failure.
_NAV_SUPERSEDED_MARKER: str = 'NS_BINDING_ABORTED'


async def _bootstrap_goto(page: Any, url: str) -> None:
    '''
    Navigate to *url*, tolerating ``NS_BINDING_ABORTED`` (a navigation
    superseded by TikTok's client-side routing). The subsequent
    ``wait_for_load_state`` gates readiness; any other navigation
    error (connection refused, navigation timeout, …) propagates and
    fails the session, excluding the proxy.
    '''
    try:
        await page.goto(url)
    except PlaywrightError as exc:
        if _NAV_SUPERSEDED_MARKER not in str(exc):
            raise


def camoufox_launch_options(proxy: dict | None) -> dict:
    '''
    Build Camoufox launch options with the anti-detect config
    validated against TikTok:

    - ``geoip=True`` aligns timezone/locale/WebRTC with the proxy
      exit (or this host's public IP when ``proxy`` is None);
    - ``main_world_eval=True`` enables the ``mw:`` prefix the
      signing/fetch overrides rely on;
    - ``block_webgl=False`` keeps WebGL so canvas/WebGL probes pass;
    - ``humanize=True`` for realistic input timing.

    Returns the dict to splat into the Firefox launch
    (``executable_path``/``args``); its ``env`` entry must be staged
    with :func:`apply_camoufox_env` before launching.
    '''
    return launch_options(
        headless=True,
        geoip=True,
        proxy=proxy,
        main_world_eval=True,
        block_webgl=False,
        humanize=True,
    )


def apply_camoufox_env(env: dict) -> None:
    '''
    Stage Camoufox's launch config in the process environment.

    Camoufox passes its fingerprint/geoip config through chunked
    ``CAMOU_CONFIG_*`` env vars, which the Firefox subprocess
    inherits. Because ``os.environ`` is process-global but each proxy
    gets its own geo-distinct browser, the previous proxy's
    ``CAMOU_CONFIG_*`` chunks are cleared before the new ones are set
    — otherwise one proxy's geo/fingerprint leaks onto another's
    browser. Callers must serialize bootstraps so set→launch is not
    interleaved.
    '''
    stale: list[str] = [
        key for key in os.environ
        if key.startswith('CAMOU_CONFIG_')
    ]
    for key in stale:
        del os.environ[key]
    for key, value in env.items():
        os.environ[key] = str(value)


# Pages navigated to when the signer is not yet present; mirrors the
# retry targets TikTokApi itself uses.
_TRY_URLS: list[str] = [
    'https://www.tiktok.com/foryou',
    'https://www.tiktok.com',
    'https://www.tiktok.com/@tiktok',
]

# Main-world fetch poll: how many reads, and the gap between them.
_FETCH_POLL_ATTEMPTS: int = 60
_FETCH_POLL_INTERVAL_SECONDS: float = 0.5


class TikTokBotDetectionError(RuntimeError):
    '''TikTok returned a block page or an intentionally empty payload.'''


def _check_bot_detection_response(body: str) -> None:
    stripped: str = body.lstrip()
    if not stripped:
        raise TikTokBotDetectionError(
            'TikTok bot detection: empty response body',
        )
    if stripped.startswith('<'):
        raise TikTokBotDetectionError(
            'TikTok bot detection: HTML response body',
        )
    try:
        payload: object = json.loads(body)
    except json.JSONDecodeError:
        return
    if not isinstance(payload, dict):
        return
    user_info: object = payload.get('userInfo')
    if not isinstance(user_info, dict):
        return
    user: object = user_info.get('user')
    if isinstance(user, dict) and not user:
        raise TikTokBotDetectionError(
            'TikTok bot detection: empty userInfo payload',
        )


def _endpoint_label(url: str) -> str:
    path: str = urlparse(url).path.lower()
    if '/user/detail' in path:
        return 'user_info'
    if '/post/item_list' in path:
        return 'user_videos'
    if '/item/detail' in path or '/item/list' in path:
        return 'video_info'
    if '/challenge/detail' in path:
        return 'hashtag_info'
    if '/challenge/item_list' in path:
        return 'hashtag_videos'
    if '/repost/item_list' in path:
        return 'repost_videos'
    return 'unknown'


class CamoufoxTikTokApi(TikTokApi):
    '''TikTokApi that signs and fetches in Camoufox's main world.'''

    def set_rate_limiter(
        self,
        rate_limiter: TikTokRateLimiter,
        proxy: str,
        call_type: TikTokCallType = TikTokCallType.API,
    ) -> None:
        '''Bind every API fetch to one proxy-affine rate bucket.'''
        self._rate_limiter = rate_limiter
        self._rate_limit_proxy = proxy
        self._rate_limit_call_type = call_type

    def set_metrics_context(
        self, scraper_label: str, worker_id: str,
    ) -> None:
        self._scraper_label = scraper_label
        self._worker_id = worker_id

    def _owned_entity(
        self, entity_type: type, *args: Any, **kwargs: Any,
    ) -> Any:
        '''Create an entity whose requests stay on this API instance.

        TikTokApi 7.1.0 stores each entity's ``parent`` as a class
        variable. Constructing another API therefore redirects all
        existing entities to the newest API and browser session.
        An instance attribute shadows that global class variable.
        '''
        entity: Any = entity_type(*args, **kwargs)
        entity.parent = self
        return entity

    def user(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(User, *args, **kwargs)

    def video(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Video, *args, **kwargs)

    def sound(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Sound, *args, **kwargs)

    def hashtag(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Hashtag, *args, **kwargs)

    def comment(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Comment, *args, **kwargs)

    def trending(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Trending, *args, **kwargs)

    def search(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Search, *args, **kwargs)

    def playlist(self, *args: Any, **kwargs: Any) -> Any:
        return self._owned_entity(Playlist, *args, **kwargs)

    async def _TikTokApi__create_session(
        self,
        url: str = 'https://www.tiktok.com',
        ms_token: str | None = None,
        proxy: dict | None = None,
        context_options: dict | None = None,
        sleep_after: int = 1,
        cookies: dict | None = None,
        suppress_resource_load_types: list[str] | None = None,
        timeout: int = 30000,
    ) -> None:
        '''
        TikTokApi 7.1.0 waits for ``networkidle`` during bootstrap.
        TikTok often keeps background requests open, especially behind
        slower proxies, so Camoufox sessions use ``domcontentloaded``
        as the readiness gate and let msToken capture / API calls prove
        the session is usable.
        '''
        context_options = context_options or {}
        context: Any | None = None
        page: Any | None = None
        try:
            if ms_token is not None:
                cookies = dict(cookies or {})
                cookies['msToken'] = ms_token

            context = await self.browser.new_context(
                proxy=proxy, **context_options,
            )
            if cookies is not None:
                formatted_cookies: list[dict[str, str]] = [
                    {
                        'name': k,
                        'value': v,
                        'domain': urlparse(url).netloc,
                        'path': '/',
                    }
                    for k, v in cookies.items()
                    if v is not None
                ]
                await context.add_cookies(formatted_cookies)
            page = await context.new_page()
            await stealth_async(page)

            request_headers: dict | None = None

            def handle_request(request: Any) -> None:
                nonlocal request_headers
                request_headers = request.headers

            page.once('request', handle_request)

            if suppress_resource_load_types is not None:
                await page.route(
                    '**/*',
                    lambda route, request: (
                        route.abort()
                        if request.resource_type
                        in suppress_resource_load_types
                        else route.continue_()
                    ),
                )

            page.set_default_navigation_timeout(timeout)
            await _bootstrap_goto(page, url)
            await _bootstrap_goto(page, url)

            x, y = random.randint(0, 50), random.randint(0, 50)
            a, b = random.randint(1, 50), random.randint(100, 200)

            await page.mouse.move(x, y)
            await page.wait_for_load_state('domcontentloaded')
            await page.mouse.move(a, b)

            session = TikTokPlaywrightSession(
                context,
                page,
                ms_token=ms_token,
                proxy=proxy,
                headers=request_headers,
                base_url=url,
            )

            if ms_token is None:
                await asyncio.sleep(sleep_after)
                cookies = await self.get_session_cookies(session)
                ms_token = cookies.get('msToken')
                session.ms_token = ms_token
                if ms_token is None:
                    self.logger.info(
                        'Failed to get msToken on session index '
                        f'{len(self.sessions)}, you should consider '
                        'specifying ms_tokens',
                    )
            self.sessions.append(session)
            await self._TikTokApi__set_session_params(session)
        except Exception as exc:
            self.logger.error(f'Failed to create session: {exc}')
            if page is not None:
                await page.close()
            if context is not None:
                await context.close()
            raise

    async def generate_x_bogus(self, url: str, **kwargs: Any) -> Any:
        '''
        Evaluate ``byted_acrawler.frontierSign`` in the main world,
        where the signer lives. Retries by navigating to a real
        TikTok page until the signer is present.
        '''
        _, session = self._get_session(**kwargs)
        for _ in range(5):
            ready: Any = await session.page.evaluate(
                'mw:typeof window.byted_acrawler !== "undefined" '
                '&& typeof window.byted_acrawler.frontierSign '
                '=== "function"',
            )
            if ready:
                break
            await session.page.goto(random.choice(_TRY_URLS))
        else:
            raise TimeoutError(
                'window.byted_acrawler never appeared',
            )
        return await session.page.evaluate(
            f'mw:window.byted_acrawler.frontierSign('
            f'{json.dumps(url)})',
        )

    async def run_fetch_script(
        self, url: str, headers: dict, **kwargs: Any,
    ) -> Any:
        '''
        Run the data fetch in the main world so webmssdk's hooked
        ``fetch`` injects ``X-Gnarly``. Camoufox's ``mw:`` does not
        await promises, so the fetch is kicked off (stashing its
        result/error on main-world globals) and then polled with
        synchronous ``mw:`` reads.
        '''
        rate_limiter: TikTokRateLimiter | None = getattr(
            self, '_rate_limiter', None,
        )
        endpoint: str = _endpoint_label(url)
        scraper: str = getattr(self, '_scraper_label', 'tiktok')
        worker_id: str = getattr(self, '_worker_id', '0')
        started: float = time.monotonic()
        if rate_limiter is not None:
            await rate_limiter.acquire(
                getattr(
                    self, '_rate_limit_call_type',
                    TikTokCallType.API,
                ),
                proxy=self._rate_limit_proxy,
            )
        try:
            _, session = self._get_session(**kwargs)
            page = session.page
            url_js: str = json.dumps(url)
            headers_js: str = json.dumps(headers)
            await page.evaluate(
                'mw:((u,h)=>{window.__ttr=null;window.__tte=null;'
                'fetch(u,{method:"GET",headers:h}).then(r=>r.text())'
                '.then(t=>{window.__ttr=t;})'
                '.catch(e=>{window.__tte=String(e);});return true;})'
                f'({url_js},{headers_js})',
            )
            for _ in range(_FETCH_POLL_ATTEMPTS):
                err: Any = await page.evaluate('mw:window.__tte')
                if err:
                    raise RuntimeError(
                        f'main-world fetch failed: {err}',
                    )
                res: Any = await page.evaluate('mw:window.__ttr')
                if res is not None:
                    _check_bot_detection_response(res)
                    API_CALL_TOTAL.labels(
                        platform='tiktok',
                        scraper=scraper,
                        endpoint=endpoint,
                        outcome='success',
                        worker_id=worker_id,
                    ).inc()
                    return res
                await asyncio.sleep(_FETCH_POLL_INTERVAL_SECONDS)
            raise TimeoutError('main-world fetch timed out')
        except Exception as exc:
            API_CALL_TOTAL.labels(
                platform='tiktok',
                scraper=scraper,
                endpoint=endpoint,
                outcome=classify_tiktok_error(exc),
                worker_id=worker_id,
            ).inc()
            raise
        finally:
            API_CALL_DURATION_SECONDS.labels(
                platform='tiktok',
                scraper=scraper,
                endpoint=endpoint,
                worker_id=worker_id,
            ).observe(time.monotonic() - started)
