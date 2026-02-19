'''
Scrape.Exchange client used for uploading content

:author: boinko@scrape.exchange
:copyright: 2026
:license: GPLv3
'''


import asyncio
import logging

from typing import Self

from httpx import AsyncClient, Response

TOKEN_ENDPOINT: str = '/api/account/v1/token'

SCRAPE_EXCHANGE_URL: str = 'https://scrape.exchange'

_LOGGER: logging.Logger = logging.getLogger(__name__)


class ExchangeClient(AsyncClient):
    TOKEN_API: str = '/token'
    ME_API: str = '/me'
    POST_REGISTER_API: str = '/api/account/v1/register'
    POST_TOKEN_API: str = '/api/account/v1/token'
    POST_SCHEMA_API: str = '/api/schema/v1'
    POST_DATA_API: str = '/api/data/v1'
    GET_STATUS_API: str = '/api/status/v1'

    def __init__(self, exchange_url: str = SCRAPE_EXCHANGE_URL) -> None:
        '''
        Initializes the ExchangeClient with API credentials and URL.

        :param api_key_id: The API key ID for authentication.
        :param api_key_secret: The API key secret for authentication.
        :param api_url: The base URL of the Scrape.Exchange API.
        :returns: (none)
        :raises: (non)
        '''

        self.exchange_url: str = exchange_url
        self.jwt_header: str | None = None
        super().__init__(
            headers={
                'Content-Type': 'application/json',
            }
        )

    @staticmethod
    async def setup(api_key_id: str, api_key_secret: str,
                    exchange_url: str = SCRAPE_EXCHANGE_URL) -> Self:
        '''
        Factory to create an instance of ExchangeClient and the JWT token for
        later us.

        :param api_key_id: The API key ID for authentication.
        :param api_key_secret: The API key secret for authentication.
        :param api_url: The base URL of the Scrape.Exchange API.
        :returns: An initialized instance of ExchangeClient.
        :raises: (none)
        '''

        self = ExchangeClient(exchange_url)
        self.jwt_header = await ExchangeClient.get_jwt_token(
            api_key_id, api_key_secret, self.exchange_url
        )
        self.headers['Authorization'] = self.jwt_header
        return self

    @staticmethod
    async def get_jwt_token(api_key_id: str, api_key_secret: str,
                            api_url: str = SCRAPE_EXCHANGE_URL) -> str:
        '''
        Retrieves a JWT token from the Scrape.Exchange API using the provided
        API credentials.

        :returns: The JWT token as a string.
        :raises: RuntimeError if the token retrieval fails.
        '''

        async with AsyncClient() as client:
            response: Response = await client.post(
                f'{api_url}{TOKEN_ENDPOINT}',
                data={
                    'api_key_id': api_key_id,
                    'api_key_secret': api_key_secret
                }
            )
            if response.status_code != 200:
                _LOGGER.warning(
                    f'Failed to retrieve JWT token: {response.text}'
                )
                raise RuntimeError(
                    f'Failed to retrieve JWT token: {response.text}'
                )
            token_data: dict[str, str] = response.json()

            jwt_header: str = ExchangeClient.get_auth_header(
                token_data['access_token']
            )

            return jwt_header

    async def get(self, url: str, retries: int = 3, delay: float = 1.0,
                  **kwargs) -> Response:
        '''
        Overrides the AsyncClient get method to include the JWT token in the
        headers.

        :param url: The URL to send the GET request to.
        :returns: The response from the GET request.
        :raises: Exception if the GET request fails after the specified
        number of retries.
        '''

        try:
            _LOGGER.debug(f'HTTP GET {url}')
            result: Response = await super().get(url, **kwargs)
            return result
        except Exception as exc:
            if retries > 0:
                await asyncio.sleep(delay)
                _LOGGER.debug(
                    f'Retrying GET request to {url} (retries left: {retries})'
                )
                return await self.get(
                    url, retries=retries - 1, delay=delay*2, **kwargs
                )
            _LOGGER.warning(
                f'GET request to {url} failed after retries: {exc}'
            )
            raise exc

    @staticmethod
    def get_auth_header(token: str) -> str:
        return f'Bearer {token}'

