'''
Manages connections to YouTube for data import.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio

import logging
from random import random
from logging import Logger
from logging import getLogger

from httpx import AsyncClient
from httpx import Response
from httpx import ReadTimeout


_LOGGER: Logger = getLogger(__name__)

YOUTUBE_DOMAIN: str = '.youtube.com'


HEADERS: dict[str, str] = {
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': (
        'text/html,application/xhtml+xml,'
        'application/xml;q=0.9,*/*;q=0.8'
    ),
}
USER_AGENT: str = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

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

    def __init__(self, user_agent: str = USER_AGENT,
                 headers: dict[str, str] = HEADERS,
                 consent_cookies: dict[str, str] = CONSENT_COOKIES,
                 **kwargs) -> None:
        '''
        Initializes the YouTube client.

        :param kwargs: Additional arguments to pass to the HTTP client.
        '''

        super().__init__(**kwargs)

        self.headers = headers.copy()

        self.consent_cookies: dict[str, str] = CONSENT_COOKIES
        if user_agent:
            self.headers['User-Agent'] = user_agent

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

    async def get(self, url: str, delay: int | None = None, **kwargs
                  ) -> str | None:
        '''
        Performs a GET request to the specified URL.

        :param url: The URL to send the GET request to.
        :param kwargs: Additional arguments to pass to the GET request.

        :returns: The HTTP response.
        '''

        log_extra: dict = {'url': url}

        try:
            _LOGGER.debug(f'HTTP GET {url}', extra=log_extra)
            resp: Response = await super().get(url, **kwargs)
        except ReadTimeout as exc:
            _LOGGER.debug(
                f'HTTP GET for {url} timed out: {exc}', extra=log_extra
            )
            raise RuntimeError(f'Timeout fetching URL {url}')

        if (resp.status_code == 303
                and 'youtube.com' in resp.headers.get('Location', '')):
            # Follow redirect just once if it redirects to another YouTube URL
            logging.debug(f'Following redirect to {resp.headers["Location"]}')
            resp = await super().get(resp.headers['Location'], **kwargs)

        if resp.status_code != 200:
            logging.warning(f'Scrape for {url} failed: {resp.status_code}')
            return None

        if delay:
            await AsyncYouTubeClient._delay(0, delay)

        return resp.text

    @staticmethod
    async def _delay(min: int = 2, max: int = 5) -> None:
        await asyncio.sleep(random() * (max - min) + min)

    # get_consent_cookies removed — consent cookie management is no longer
    # handled by this client. Use explicit cookie injection via
    # `consent_cookies` constructor parameter or manage cookies externally.

    def create_cookie_header(self, cookies: dict) -> str:
        '''
        Convert a cookies dictionary to a Cookie header string.

        :param cookies: Dictionary of cookie name -> value

        :returns: String formatted for use in Cookie HTTP header
        '''

        return '; '.join(f'{name}={value}' for name, value in cookies.items())
