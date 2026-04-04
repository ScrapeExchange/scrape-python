'''
Manages connections to YouTube for data import.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio

import random
from logging import Logger
from logging import getLogger

from httpx import AsyncClient
from httpx import Response
from httpx import ReadTimeout
from httpx import RequestError
from httpx import ConnectError
from httpx import ConnectTimeout

from httpx_curl_cffi import AsyncCurlTransport, CurlOpt

_LOGGER: Logger = getLogger(__name__)

YOUTUBE_DOMAIN: str = '.youtube.com'


CONSENT_COOKIES: dict[str, str] = {
    'CONSENT': 'YES+cb.20210328-17-p0.en+FX+100',
    'SOCS': (
        'CAISNQgDEitib3FfaWRlbnRpdHlmcm9udGVuZHVpc2VydmVyXzIwMjM'
        'wODI5LjA3X3AwGgJlbiADGgYIgICUoQY'
    ),
}


class AsyncYouTubeClient(AsyncClient):
    '''
    An HTTP client for connecting to YouTube.
    '''
    SCRAPE_URL: str = 'https://www.youtube.com'

    def __init__(self, consent_cookies: dict[str, str] = CONSENT_COOKIES,
                 proxies: list[str] | str = [], **kwargs) -> None:
        '''
        Initializes the YouTube client.

        :param kwargs: Additional arguments to pass to the HTTP client.
        '''

        if isinstance(proxies, str):
            proxies = proxies.split(',') if proxies else None

        self.proxy: str = random.choice(proxies) if proxies else None
        _LOGGER.debug(
            f'Initializing AsyncYouTubeClient with proxy: {self.proxy}'
        )
        super().__init__(
            transport=AsyncCurlTransport(
                impersonate='chrome',
                default_headers=True,
                curl_options={CurlOpt.FRESH_CONNECT: True},
                proxy=self.proxy,
            ), **kwargs
        )

        self.consent_cookies: dict[str, str] = CONSENT_COOKIES

        for name, value in consent_cookies.items():
            self.cookies.set(
                name, value, domain=YOUTUBE_DOMAIN, path='/'
            )

    def get_headers(self) -> dict[str, str]:
        '''
        Get the current HTTP headers for the client.

        :returns: A dictionary of HTTP headers.
        '''

        return dict(self.headers)

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

        try:
            _LOGGER.debug(f'HTTP GET {url}')
            resp: Response = await super().get(url, **kwargs)
        except (ConnectTimeout, ConnectError, ReadTimeout,
                ConnectionResetError, ConnectionRefusedError) as exc:
            _LOGGER.debug(f'HTTP GET timeout for {url}: {exc}')
            if retries > 0:
                await asyncio.sleep(random.uniform(delay-1, delay))
                _LOGGER.debug(
                    f'Retrying GET request to {url} (retries left: {retries})'
                )
                return await self.get(
                    url, retries=retries - 1, delay=delay*2, **kwargs
                )

            raise RuntimeError(f'Timeout fetching URL {url}')
        except RequestError as exc:
            _LOGGER.debug(f'HTTP GET request error for {url}: {exc}')
            raise
        except Exception as exc:
            _LOGGER.debug(f'HTTP GET error for {url}: {exc}')
            if retries > 0:
                await asyncio.sleep(random.uniform(delay-1, delay))
                _LOGGER.debug(
                    f'Retrying GET request to {url} (retries left: {retries})'
                )
                return await self.get(
                    url, retries=retries - 1, delay=delay*2, **kwargs
                )

            raise RuntimeError(f'Timeout fetching URL {url}') from exc

        if (resp.status_code == 303
                and 'youtube.com' in resp.headers.get('Location', '')):
            # Follow redirect just once if it redirects to another YouTube URL
            if follow_redirects:
                _LOGGER.debug(
                    f'Following redirect to {resp.headers["Location"]}'
                )
                return await self.get(
                    resp.headers['Location'], retries=retries, delay=delay,
                    follow_redirects=False, **kwargs
                )

        if resp.status_code == 404:
            raise ValueError(f'URL not found: {url}')

        if resp.status_code != 200:
            _LOGGER.warning(f'Scrape for {url} failed: {resp.status_code}')
            return None

        if delay:
            await AsyncYouTubeClient._delay(0, delay)

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
