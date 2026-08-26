'''
Manages connections to YouTube for data import.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import base64
import os
import sys
import time

import random
from logging import Logger
from logging import getLogger

from httpx import AsyncClient
from httpx import Response
from httpx import ReadTimeout
from httpx import RequestError
from httpx import ConnectError
from httpx import ConnectTimeout
from httpx import Timeout
from httpx import TimeoutException

from httpx_curl_cffi import AsyncCurlTransport
from prometheus_client import Histogram

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.http_timeouts import (
    HTTP_CONNECT_TIMEOUT,
    HTTP_REQUEST_TIMEOUT,
)
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.util import (
    extract_proxy_ip,
    extract_proxy_port,
    proxy_network_for,
)
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from scrape_exchange.proxy_phase_metrics import (
    HTML_CURL_INFOS,
    record_html_phase_metrics,
)
from scrape_exchange._lazy_async_pool import _LazyAsyncPool

_LOGGER: Logger = getLogger(__name__)

_SCRIPT_NAME: str = (
    os.path.basename(sys.argv[0]) if sys.argv else 'unknown'
)

_SCRAPER_BY_SCRIPT: dict[str, str] = {
    'yt_video_scrape.py': 'video_scraper',
    'yt_channel_scrape.py': 'channel_scraper',
    'yt_rss_scrape.py': 'rss_scraper',
    'yt_discover_channels.py': 'discover',
}


def _get_scraper() -> str:
    '''Return the scraper label for the current script.'''
    return _SCRAPER_BY_SCRIPT.get(_SCRIPT_NAME, 'unknown')


# HTTP / InnerTube latency histogram for calls to YouTube. ``kind`` is
# ``'http'`` for plain HTTP GETs via this client and ``'innertube'`` for
# requests going through the innertube library (player/browse/next).
# ``status_class`` is ``'2xx'``/``'3xx'``/``'4xx'``/``'5xx'`` or
# ``'error'`` when no HTTP response arrived (timeout, connection error,
# raised exception).
METRIC_YT_REQUEST_DURATION: Histogram = Histogram(
    'api_request_duration_seconds',
    'Duration of requests to YouTube, by api type '
    '(html/innertube), HTTP status class, and the '
    'outbound proxy network/file. The previously-present '
    '``proxy_ip`` label was dropped because its ~108 '
    'unique values multiplied series cardinality enough '
    'to contribute materially to worker memory. The '
    'per-proxy_ip view is recoverable from scrape-failure '
    'and rate-limit metrics that still carry the label; '
    'this histogram aggregates by network/file only.',
    [
        'platform', 'scraper', 'api',
        'status_class', 'worker_id',
        'proxy_file',
    ],
    buckets=(
        0.1, 0.25, 0.5, 1.0, 2.5,
        5.0, 10.0, 30.0, 60.0,
    ),
)

YOUTUBE_DOMAIN: str = '.youtube.com'


def _yt_status_class(status_code: int | None) -> str:
    '''Return a coarse ``'Nxx'`` status-class label or ``'error'``.'''

    if status_code is None:
        return 'error'
    return f'{status_code // 100}xx'


# InnerTube client identity matching the WEB client that a real Chrome
# browser sends on every YouTube page navigation and XHR request.
INNERTUBE_CLIENT_NAME: str = '1'
INNERTUBE_CLIENT_VERSION: str = '2.20260708.00.00'
ANDROID_CLIENT_VERSION: str = '21.26.364'
ANDROID_USER_AGENT: str = (
    'com.google.android.youtube/21.26.364 '
    '(Linux; U; Android 11) gzip'
)

CONSENT_COOKIES: dict[str, str] = {
    'CONSENT': 'YES+cb.20210328-17-p0.en+FX+100',
    'SOCS': (
        'CAISNQgDEitib3FfaWRlbnRpdHlmcm9udGVuZHVpc2VydmVyXzIwMjM'
        'wODI5LjA3X3AwGgJlbiADGgYIgICUoQY'
    ),
}


def generate_visitor_info() -> str:
    '''
    Generate a VISITOR_INFO1_LIVE cookie value. YouTube sets this on every
    real browser session; its absence is a strong bot signal.  The value is
    an 11-byte random token, base64url-encoded (no padding), which matches
    the format YouTube produces.
    '''
    return base64.urlsafe_b64encode(os.urandom(11)).rstrip(b'=').decode()


class AsyncYouTubeClient(AsyncClient):
    '''
    An HTTP client for connecting to YouTube.
    '''

    SCRAPE_URL: str = f'https://www{YOUTUBE_DOMAIN}'

    def __init__(self, consent_cookies: dict[str, str] = CONSENT_COOKIES,
                 proxies: list[str] | None = None,
                 proxy: str | None = None, **kwargs) -> None:
        '''
        Initializes the YouTube client.

        :param consent_cookies: Cookies to set on every request.
        :param proxies: Pool of proxy URLs; the rate limiter selects the best
            one unless *proxy* is given explicitly.
        :param proxy: Pin the client to this specific proxy URL, bypassing
            rate-limiter selection.  Use when the caller needs a known proxy
            (e.g. cookie acquisition per proxy).
        :param kwargs: Additional arguments passed through to
            :class:`httpx.AsyncClient`.
        '''

        if proxy is not None:
            self.proxy: str | None = proxy
        elif proxies:
            self.proxy = (
                YouTubeRateLimiter.get().select_proxy(YouTubeCallType.HTML)
                or random.choice(proxies)
            )
        else:
            self.proxy = None

        if self.proxy:
            proxy_ip: str = extract_proxy_ip(self.proxy)
            proxy_port: str = extract_proxy_port(self.proxy)
            _LOGGER.debug(
                'Initializing AsyncYouTubeClient with proxy',
                extra={
                    'proxy_ip': proxy_ip,
                    'proxy_port': proxy_port,
                    'proxy_network': proxy_network_for(proxy_ip),
                }
            )
        else:
            _LOGGER.warning('Initializing AsyncYouTubeClient without proxy')

        # NB: do NOT set ``CurlOpt.FRESH_CONNECT`` here. With the
        # per-proxy pool in ``pooled_youtube_client_for_entry``,
        # this client is long-lived; FRESH_CONNECT=True would force
        # a brand-new TCP+TLS handshake on every request, defeating
        # keepalive and producing the SYN pressure that takes the
        # channel scraper's about-page success rate to zero under
        # fleet load.
        super().__init__(
            transport=AsyncCurlTransport(
                impersonate='chrome',
                default_headers=True,
                proxy=self.proxy,
                # Ask curl to populate per-phase timings on
                # every response so :func:`record_html_phase_metrics`
                # can emit TCP/TLS phase histograms.
                curl_infos=list(HTML_CURL_INFOS),
            ), **kwargs
        )

        self.consent_cookies: dict[str, str] = consent_cookies

        for name, value in consent_cookies.items():
            self.cookies.set(
                name, value, domain=YOUTUBE_DOMAIN, path='/'
            )

        # VISITOR_INFO1_LIVE is set by YouTube on every real browser
        # session.  Its absence is a strong bot-detection signal.
        # The pre-set value here is a placeholder; ``_warm_session``
        # below replaces it with one issued by YouTube on first use,
        # which materially raises HTML success rate (real browser
        # sessions hit ``youtube.com/`` first to acquire cookies
        # before navigating to channel pages).
        self.visitor_id: str = generate_visitor_info()
        self.cookies.set(
            'VISITOR_INFO1_LIVE', self.visitor_id,
            domain=YOUTUBE_DOMAIN, path='/'
        )

        # Lazy session warm-up. ``_warm_session`` runs once on first
        # ``get()`` and fetches https://www.youtube.com/ so the
        # cookie jar picks up YouTube-issued PREF, SOCS, YSC, and a
        # real VISITOR_INFO1_LIVE before we hit any channel page.
        # Concurrent first calls are deduped via the lock.
        self._session_warmed: bool = False
        self._warm_lock: asyncio.Lock = asyncio.Lock()

        # InnerTube context headers that Chrome sends on every YouTube
        # page load and XHR.  Including them makes the HTTP client
        # fingerprint consistent with a real browser session.
        self.headers['X-YouTube-Client-Name'] = INNERTUBE_CLIENT_NAME
        self.headers['X-YouTube-Client-Version'] = INNERTUBE_CLIENT_VERSION

    async def _warm_session(self) -> None:
        '''
        Fetch ``https://www.youtube.com/`` once so the cookie jar
        picks up YouTube-issued PREF/SOCS/YSC and a real
        VISITOR_INFO1_LIVE before we navigate to any channel page.
        Real browsers do this implicitly; jumping straight to a
        channel /about page tripped the WAF for ~99.8% of HTML
        requests under fleet load.

        Idempotent and lock-guarded so concurrent first calls from
        the same client only warm once. On warm-up failure we keep
        the pre-set placeholder cookies and proceed; a subsequent
        request may still succeed and the next one will retry the
        warm-up after ``reset_session_warm()``.
        '''
        if self._session_warmed:
            return
        async with self._warm_lock:
            if self._session_warmed:
                return
            try:
                # Bypass our get() wrapper — that would try to
                # acquire an HTML token and recurse into _warm.
                await super().get(
                    self.SCRAPE_URL,
                    timeout=Timeout(
                        HTTP_REQUEST_TIMEOUT,
                        connect=HTTP_CONNECT_TIMEOUT,
                    ),
                    follow_redirects=True,
                )
            except Exception as exc:
                _LOGGER.debug(
                    'Session warm-up failed; proceeding without it',
                    exc=exc,
                    extra={
                        'proxy_ip': (
                            extract_proxy_ip(self.proxy)
                            if self.proxy else 'none'
                        ),
                        'proxy_port': (
                            extract_proxy_port(self.proxy)
                            if self.proxy else 'none'
                        ),
                    },
                )
            self._session_warmed = True

    def reset_session_warm(self) -> None:
        '''Mark the session as un-warmed so the next ``get()`` will
        re-fetch ``youtube.com/``. Useful if cookies are observed
        to expire or the WAF starts rejecting the cached session.'''
        self._session_warmed = False

    async def get(self, url: str, retries: int = 0, delay: float = 1.0,
                  follow_redirects: bool = True, **kwargs) -> str | None:
        '''
        Performs a GET request to the specified URL.

        :param url: The URL to send the GET request to.
        :param retries: In-process retry count on transient errors.
            Default 0: callers re-queue failed work at the worker
            loop level, so an inner retry burns ~10s of worker time
            per attempt for marginal save rate. Was 3; the
            additional retries dominated worker time under fleet
            load and dragged channel scraper throughput down.
        :param delay: Initial backoff delay (seconds) between
            in-process retries; doubles each retry.
        :param kwargs: Additional arguments to pass to the GET request.

        :returns: The HTTP response.
        :raises: RuntimeError if the request fails after all retries.
        :raises: ValueError if the URL is not found (404).
        '''

        await self._warm_session()
        await YouTubeRateLimiter.get().acquire(
            YouTubeCallType.HTML, proxy=self.proxy
        )
        proxy_ip: str = (
            extract_proxy_ip(self.proxy) if self.proxy else 'none'
        )
        proxy_network: str = proxy_network_for(proxy_ip)
        proxy_port: str = (
            extract_proxy_port(self.proxy) if self.proxy else 'none'
        )
        proxy_file: str = proxy_file_label(self.proxy or '')
        extra: dict[str, str] = {
            'proxy_ip': proxy_ip,
            'proxy_port': proxy_port,
            'proxy_network': proxy_network,
            'proxy_file': proxy_file,
            'url': url,
        }
        _LOGGER.debug('HTTP GET', extra=extra)
        start: float = time.monotonic()
        # Channel /about pages are 1.2-2.2MB and YouTube serves
        # them slowly under fleet load; httpx's 5s default for
        # read+write+pool was tight enough to time out the body
        # even after a successful 3s connect. 10s read budget
        # leaves headroom; caller can override via kwargs.
        kwargs.setdefault(
            'timeout', Timeout(
                HTTP_REQUEST_TIMEOUT,
                connect=HTTP_CONNECT_TIMEOUT,
            ),
        )
        try:
            resp: Response = await super().get(url, **kwargs)
        except asyncio.CancelledError as exc:
            duration: float = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_file=proxy_file,
            ).observe(duration)
            # curl_cffi can raise CancelledError from its internal stream task
            # during cleanup even when the outer task is not being cancelled.
            # Only propagate if this task is genuinely being cancelled.
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            _LOGGER.debug(
                'HTTP GET cancelled (curl_cffi internal)',
                extra=extra | {'duration': duration},
            )
            if retries > 0:
                await asyncio.sleep(random.uniform(delay - 1, delay))
                return await self.get(
                    url, retries=retries - 1, delay=delay * 2, **kwargs
                )
            raise RuntimeError(
                f'Request cancelled fetching URL {url}'
            ) from exc
        except (TimeoutException, ConnectError, ReadTimeout,
                ConnectTimeout, ConnectionResetError,
                ConnectionRefusedError) as exc:
            duration = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_file=proxy_file,
            ).observe(duration)
            _LOGGER.debug(
                'HTTP GET timeout',
                exc=exc,
                extra=extra | {'duration': duration},
            )
            if retries > 0:
                await asyncio.sleep(random.uniform(delay-1, delay))
                _LOGGER.debug(
                    'Retrying GET request',
                    extra=extra | {'retries_left': retries, 'delay': delay},
                )
                return await self.get(
                    url, retries=retries - 1, delay=delay*2, **kwargs
                )

            raise RuntimeError(f'Timeout fetching URL {url}') from exc
        except RequestError as exc:
            duration = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_file=proxy_file,
            ).observe(duration)
            _LOGGER.debug(
                'HTTP GET request error',
                exc=exc,
                extra=extra | {'duration': duration},
            )
            raise
        except Exception as exc:
            duration = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_file=proxy_file,
            ).observe(duration)
            _LOGGER.debug(
                'HTTP GET error',
                exc=exc,
                extra=extra | {'duration': duration},
            )
            if retries > 0:
                await asyncio.sleep(random.uniform(delay-1, delay))
                _LOGGER.debug(
                    'Retrying GET request',
                    extra=extra | {'retries_left': retries, 'delay': delay}
                )
                return await self.get(
                    url, retries=retries - 1, delay=delay*2, **kwargs
                )

            raise RuntimeError(f'Timeout fetching URL {url}') from exc

        duration = time.monotonic() - start
        METRIC_YT_REQUEST_DURATION.labels(
            platform='youtube',
            scraper=_get_scraper(),
            api='html',
            status_class=_yt_status_class(resp.status_code),
            worker_id=get_worker_id(),
            proxy_file=proxy_file,
        ).observe(duration)
        record_html_phase_metrics(
            resp, proxy_file=proxy_file,
        )
        _LOGGER.debug(
            'HTTP GET completed',
            extra=extra | {
                'duration': duration,
                'status_code': resp.status_code,
            },
        )

        if (resp.status_code == 303
                and 'youtube.com' in resp.headers.get('Location', '')):
            # Follow redirect just once if it redirects to another YouTube URL
            if follow_redirects:
                _LOGGER.debug(
                    'Following redirect',
                    extra={'location': resp.headers['Location']}
                )
                return await self.get(
                    resp.headers['Location'], retries=retries, delay=delay,
                    follow_redirects=False, **kwargs
                )

        if resp.status_code == 404:
            raise ValueError(f'URL not found: {url}')

        if resp.status_code != 200:
            _LOGGER.warning(
                'Scrape failed',
                extra=extra | {
                    'duration': duration,
                    'status_code': resp.status_code,
                },
            )
            return None

        return resp.text

    @staticmethod
    async def _delay(min: float = 0.3, max: float = 0.8) -> None:
        await asyncio.sleep(random.uniform(min, max))

    def create_cookie_header(self, cookies: dict) -> str:
        '''
        Convert a cookies dictionary to a Cookie header string.

        :param cookies: Dictionary of cookie name -> value

        :returns: String formatted for use in Cookie HTTP header
        '''

        return '; '.join(f'{name}={value}' for name, value in cookies.items())


def _make_pooled_youtube_client_for_entry(
    entry: str | None,
) -> AsyncYouTubeClient:
    '''Pool factory for the curl_cffi-backed AsyncYouTubeClient.
    The constructor's existing ``proxy=`` arg pins the client to
    a specific proxy.'''

    return AsyncYouTubeClient(proxy=entry)


_YOUTUBE_CLIENT_POOL: _LazyAsyncPool[
    str | None, AsyncYouTubeClient,
] = _LazyAsyncPool(
    factory=_make_pooled_youtube_client_for_entry,
)


def pooled_youtube_client_for_entry(
    entry: str | None,
) -> AsyncYouTubeClient:
    '''Return the long-lived, keep-alive-pooled
    :class:`AsyncYouTubeClient` for ``entry`` (canonical proxy URL,
    ``local://<ipv4>``, or ``None`` for proxyless). Same instance
    across calls for the same key.

    The cached client is closed by
    ``aclose_pooled_youtube_clients()`` at scraper shutdown.
    Tests use ``_reset_pool_for_tests()`` to drop the cache
    without closing real connections.'''

    return _YOUTUBE_CLIENT_POOL.get(entry)


async def aclose_pooled_youtube_clients() -> None:
    '''Close every pooled AsyncYouTubeClient and empty the pool.
    Called from the scraper shutdown drain.'''

    await _YOUTUBE_CLIENT_POOL.aclose_all()


def _reset_pool_for_tests() -> None:
    '''Drop cached pooled clients without calling aclose. Tests
    only.'''

    _YOUTUBE_CLIENT_POOL.reset_for_tests()
