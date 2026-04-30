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
from httpx import TimeoutException

from httpx_curl_cffi import AsyncCurlTransport, CurlOpt
from prometheus_client import Histogram

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.util import extract_proxy_ip, proxy_network_for
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType

_LOGGER: Logger = getLogger(__name__)

_SCRIPT_NAME: str = (
    os.path.basename(sys.argv[0]) if sys.argv else 'unknown'
)

_SCRAPER_BY_SCRIPT: dict[str, str] = {
    'yt_video_scrape.py': 'video_scraper',
    'yt_channel_scrape.py': 'channel_scraper',
    'yt_rss_scrape.py': 'rss_scraper',
    'rebuild_creator_map.py': 'channel_scraper',
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
    'outbound proxy IP used.',
    [
        'platform', 'scraper', 'api',
        'status_class', 'worker_id',
        'proxy_ip', 'proxy_network',
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
INNERTUBE_CLIENT_VERSION: str = '2.20250626.01.00'

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
                 proxies: list[str] | str | None = None,
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

        if isinstance(proxies, str):
            proxies = proxies.split(',') if proxies else None

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
            _LOGGER.debug(
                'Initializing AsyncYouTubeClient with proxy',
                extra={
                    'proxy': self.proxy,
                    'proxy_ip': proxy_ip,
                    'proxy_network': proxy_network_for(proxy_ip),
                }
            )
        else:
            _LOGGER.warning('Initializing AsyncYouTubeClient without proxy')

        super().__init__(
            transport=AsyncCurlTransport(
                impersonate='chrome',
                default_headers=True,
                curl_options={CurlOpt.FRESH_CONNECT: True},
                proxy=self.proxy,
            ), **kwargs
        )

        self.consent_cookies: dict[str, str] = consent_cookies

        for name, value in consent_cookies.items():
            self.cookies.set(
                name, value, domain=YOUTUBE_DOMAIN, path='/'
            )

        # VISITOR_INFO1_LIVE is set by YouTube on every real browser
        # session.  Its absence is a strong bot-detection signal.
        self.visitor_id: str = generate_visitor_info()
        self.cookies.set(
            'VISITOR_INFO1_LIVE', self.visitor_id,
            domain=YOUTUBE_DOMAIN, path='/'
        )

        # InnerTube context headers that Chrome sends on every YouTube
        # page load and XHR.  Including them makes the HTTP client
        # fingerprint consistent with a real browser session.
        self.headers['X-YouTube-Client-Name'] = INNERTUBE_CLIENT_NAME
        self.headers['X-YouTube-Client-Version'] = INNERTUBE_CLIENT_VERSION

    async def get(self, url: str, retries: int = 3, delay: float = 1.0,
                  follow_redirects: bool = True, **kwargs) -> str | None:
        '''
        Performs a GET request to the specified URL.

        :param url: The URL to send the GET request to.
        :param kwargs: Additional arguments to pass to the GET request.

        :returns: The HTTP response.
        :raises: RuntimeError if the request fails after all retries.
        :raises: ValueError if the URL is not found (404).
        '''

        await YouTubeRateLimiter.get().acquire(
            YouTubeCallType.HTML, proxy=self.proxy
        )
        proxy_ip: str = (
            extract_proxy_ip(self.proxy) if self.proxy else 'none'
        )
        proxy_network: str = proxy_network_for(proxy_ip)
        extra: dict[str, str] = {
            'proxy_ip': proxy_ip,
            'proxy_network': proxy_network,
            'url': url,
        }
        _LOGGER.debug('HTTP GET', extra=extra)
        start: float = time.monotonic()
        try:
            resp: Response = await super().get(url, **kwargs)
        except asyncio.CancelledError as exc:
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_network=proxy_network,
            ).observe(time.monotonic() - start)
            # curl_cffi can raise CancelledError from its internal stream task
            # during cleanup even when the outer task is not being cancelled.
            # Only propagate if this task is genuinely being cancelled.
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            _LOGGER.debug(
                'HTTP GET cancelled (curl_cffi internal)', extra=extra,
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
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_network=proxy_network,
            ).observe(time.monotonic() - start)
            _LOGGER.debug('HTTP GET timeout', exc=exc, extra=extra)
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
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_network=proxy_network,
            ).observe(time.monotonic() - start)
            _LOGGER.debug('HTTP GET request error', exc=exc, extra=extra)
            raise
        except Exception as exc:
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube',
                scraper=_get_scraper(),
                api='html',
                status_class='error',
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_network=proxy_network,
            ).observe(time.monotonic() - start)
            _LOGGER.debug('HTTP GET error', exc=exc, extra=extra)
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

        METRIC_YT_REQUEST_DURATION.labels(
            platform='youtube',
            scraper=_get_scraper(),
            api='html',
            status_class=_yt_status_class(resp.status_code),
            worker_id=get_worker_id(),
            proxy_ip=proxy_ip,
            proxy_network=proxy_network,
        ).observe(time.monotonic() - start)

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
                extra={'url': url, 'status_code': resp.status_code}
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
