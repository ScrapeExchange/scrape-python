'''The scrape.exchange per-video existence check.

Shared by the RSS scraper, which asks before scraping, and by the
backlog-server queue's migration fallback, which asks about upload
state the server cannot answer yet.
'''

from httpx import Response

from scrape_exchange.exchange_client import ExchangeClient


async def exchange_video_exists(
    client: ExchangeClient, exchange_url: str, video_id: str,
) -> bool:
    '''Whether *video_id* is stored on scrape.exchange.

    :param client: an authenticated Scrape Exchange API client.
    :param exchange_url: base URL, without a trailing slash needed.
    :param video_id: the YouTube video ID to look up.
    :returns: True on 200 (stored), False on 404 (not stored).
    :raises RuntimeError: on any other status.
    '''
    url: str = (
        f'{exchange_url}{ExchangeClient.POST_DATA_API}'
        f'/content/youtube/video/{video_id}'
    )
    response: Response = await client.get(url)
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    raise RuntimeError(
        f'Unexpected status {response.status_code} checking video '
        f'{video_id}: {response.text}',
    )
