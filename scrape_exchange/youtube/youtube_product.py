'''
Model a YouTube store product as scraped from a channel's store tab

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Self
from logging import Logger
from logging import getLogger
from datetime import UTC, datetime

_LOGGER: Logger = getLogger(__name__)


class YouTubeProduct:
    '''
    Represents a product from a YouTube channel's store tab as returned
    by the InnerTube browse API.

    The data comes from a ``verticalProductCardRenderer`` inside a
    ``gridRenderer`` within a ``shelfRenderer``.
    '''

    def __init__(self,
                 title: str | None = None,
                 price: str | None = None,
                 merchant_name: str | None = None,
                 thumbnail_url: str | None = None,
                 product_url: str | None = None,
                 accessibility_title: str | None = None,
                 channel_id: str | None = None) -> None:

        self.title: str | None = title
        self.price: str | None = price
        self.merchant_name: str | None = merchant_name
        self.thumbnail_url: str | None = thumbnail_url
        self.product_url: str | None = product_url
        self.accessibility_title: str | None = accessibility_title
        self.channel_id: str | None = channel_id
        self.created_timestamp: datetime = datetime.now(tz=UTC)

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeProduct):
            return False

        return (
            self.title == other.title
            and self.price == other.price
            and self.merchant_name == other.merchant_name
            and self.thumbnail_url == other.thumbnail_url
            and self.product_url == other.product_url
            and self.accessibility_title == other.accessibility_title
            and self.channel_id == other.channel_id
        )

    def __hash__(self) -> int:
        return hash((self.title, self.product_url))

    def __repr__(self) -> str:
        return (
            f'YouTubeProduct(title={self.title!r}, '
            f'price={self.price}, '
            f'merchant={self.merchant_name!r})'
        )

    def to_dict(self) -> dict[str, any]:
        '''
        Returns a dict representation of the product.
        '''

        return {
            'title': self.title,
            'price': self.price,
            'merchant_name': self.merchant_name,
            'thumbnail_url': self.thumbnail_url,
            'product_url': self.product_url,
            'accessibility_title': self.accessibility_title,
            'channel_id': self.channel_id,
            'created_timestamp': self.created_timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory method that creates a YouTubeProduct from a dict
        previously produced by ``to_dict``.
        '''

        product = YouTubeProduct(
            title=data.get('title'),
            price=data.get('price'),
            merchant_name=data.get('merchant_name'),
            thumbnail_url=data.get('thumbnail_url'),
            product_url=data.get('product_url'),
            accessibility_title=data.get('accessibility_title'),
            channel_id=data.get('channel_id'),
        )

        ts: str | None = data.get('created_timestamp')
        if ts:
            product.created_timestamp = datetime.fromisoformat(ts)

        return product

    @staticmethod
    def from_innertube(item: dict[str, any],
                       channel_id: str | None = None) -> Self | None:
        '''
        Factory method that creates a YouTubeProduct from the raw
        ``verticalProductCardRenderer`` dict returned by the InnerTube
        browse API on a channel's store tab.

        :param item: a single item from the gridRenderer items list
        :param channel_id: optional channel ID to associate
        :returns: a YouTubeProduct or None if the item cannot be parsed
        '''

        vpc: dict = item.get('verticalProductCardRenderer', {})
        if not vpc:
            return None

        title: str | None = vpc.get('title')
        if not title:
            return None

        price: str | None = vpc.get('price')

        merchant_name: str | None = vpc.get('merchantName')

        accessibility_title: str | None = vpc.get('accessibilityTitle')

        # Thumbnail URL — take the first available thumbnail
        thumbnail_url: str | None = None
        thumbs: list = vpc.get(
            'thumbnail', {}
        ).get('thumbnails', [])
        if thumbs:
            thumbnail_url = thumbs[0].get('url')

        # Product URL — extract from the navigation endpoint commands
        product_url: str | None = None
        commands: list = vpc.get(
            'navigationEndpoint', {}
        ).get(
            'commandExecutorCommand', {}
        ).get('commands', [])
        for cmd in commands:
            url_ep: dict = cmd.get('urlEndpoint', {})
            if url_ep:
                product_url = url_ep.get('url')
                break

        return YouTubeProduct(
            title=title,
            price=price,
            merchant_name=merchant_name,
            thumbnail_url=thumbnail_url,
            product_url=product_url,
            accessibility_title=accessibility_title,
            channel_id=channel_id,
        )
