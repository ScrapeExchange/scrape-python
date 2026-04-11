#!/usr/bin/env python3
'''
Unit tests for YouTubeProduct
'''

import unittest

from scrape_exchange.youtube.youtube_product import YouTubeProduct


# Sample verticalProductCardRenderer as returned by the InnerTube browse API
# on the store tab
SAMPLE_PRODUCT_ITEM: dict = {
    'verticalProductCardRenderer': {
        'title': 'Peak Poland Mug Mug',
        'thumbnail': {
            'thumbnails': [
                {
                    'url': 'https://encrypted-tbn3.gstatic.com/shopping?q=tbn:peak-poland',
                    'width': 512,
                    'height': 512
                }
            ]
        },
        'navigationEndpoint': {
            'commandExecutorCommand': {
                'commands': [
                    {
                        'feedbackEndpoint': {
                            'feedbackToken': 'AB9zfpK...'
                        }
                    },
                    {
                        'urlEndpoint': {
                            'url': 'https://history-matters-store-2.creator-spring.com/listing/peak-poland-mug?product=658',
                            'target': 'TARGET_NEW_WINDOW'
                        }
                    }
                ]
            }
        },
        'price': '$14.99',
        'accessibilityTitle': 'Peak Poland Mug Mug, $14.99 , from Spring',
        'merchantName': 'Spring',
        'fromMerchantText': 'From Spring',
        'showOpenInNewIcon': True,
        'useNewStyle': False,
        'dealsData': {
            'currentPrice': '$14.99'
        },
        'ctaText': {
            'runs': [{'text': 'From Spring'}]
        },
        'ctaStyle': 'VERTICAL_PRODUCT_CARD_CTA_STYLE_FROM_VENDOR',
        'layoutStyle': 'VERTICAL_PRODUCT_CARD_LAYOUT_STYLE_DEFAULT'
    }
}

SAMPLE_PRODUCT_NO_URL: dict = {
    'verticalProductCardRenderer': {
        'title': 'Basic Product',
        'price': '$9.99',
        'merchantName': 'TestShop',
        'thumbnail': {
            'thumbnails': []
        },
        'navigationEndpoint': {
            'commandExecutorCommand': {
                'commands': []
            }
        }
    }
}


class TestYouTubeProduct(unittest.TestCase):

    def test_from_innertube(self) -> None:
        product: YouTubeProduct | None = YouTubeProduct.from_innertube(
            SAMPLE_PRODUCT_ITEM, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(product)
        self.assertEqual(product.title, 'Peak Poland Mug Mug')
        self.assertEqual(product.price, '$14.99')
        self.assertEqual(product.merchant_name, 'Spring')
        self.assertIn('peak-poland', product.thumbnail_url)
        self.assertIn(
            'creator-spring.com/listing/peak-poland-mug',
            product.product_url
        )
        self.assertEqual(
            product.accessibility_title,
            'Peak Poland Mug Mug, $14.99 , from Spring'
        )
        self.assertEqual(
            product.channel_id, 'UC22BdTgxefuvUivrjesETjg'
        )

    def test_from_innertube_empty(self) -> None:
        self.assertIsNone(YouTubeProduct.from_innertube({}))

    def test_from_innertube_no_title(self) -> None:
        item: dict = {
            'verticalProductCardRenderer': {
                'price': '$5.00',
            }
        }
        self.assertIsNone(YouTubeProduct.from_innertube(item))

    def test_from_innertube_no_url_no_thumb(self) -> None:
        product: YouTubeProduct | None = YouTubeProduct.from_innertube(
            SAMPLE_PRODUCT_NO_URL
        )
        self.assertIsNotNone(product)
        self.assertEqual(product.title, 'Basic Product')
        self.assertEqual(product.price, '$9.99')
        self.assertEqual(product.merchant_name, 'TestShop')
        self.assertIsNone(product.thumbnail_url)
        self.assertIsNone(product.product_url)
        self.assertIsNone(product.channel_id)

    def test_to_dict_from_dict_round_trip(self) -> None:
        product = YouTubeProduct(
            title='Test Product',
            price='$19.99',
            merchant_name='TestMerchant',
            thumbnail_url='https://example.com/thumb.jpg',
            product_url='https://example.com/product',
            accessibility_title='Test Product, $19.99',
            channel_id='UCtest',
        )
        data: dict = product.to_dict()
        restored: YouTubeProduct = YouTubeProduct.from_dict(data)
        self.assertEqual(product, restored)

    def test_to_dict_fields(self) -> None:
        product = YouTubeProduct(
            title='My Product',
            price='$9.99',
            merchant_name='Shop',
            thumbnail_url='https://example.com/t.jpg',
            product_url='https://example.com/p',
            accessibility_title='My Product, $9.99',
            channel_id='UCabc',
        )
        d: dict[str, any] = product.to_dict()
        self.assertEqual(d['title'], 'My Product')
        self.assertEqual(d['price'], '$9.99')
        self.assertEqual(d['merchant_name'], 'Shop')
        self.assertEqual(d['thumbnail_url'], 'https://example.com/t.jpg')
        self.assertEqual(d['product_url'], 'https://example.com/p')
        self.assertEqual(d['accessibility_title'], 'My Product, $9.99')
        self.assertEqual(d['channel_id'], 'UCabc')
        self.assertIn('created_timestamp', d)

    def test_hash_and_equality(self) -> None:
        a = YouTubeProduct(
            title='Mug', price='$10', product_url='https://ex.com/mug'
        )
        b = YouTubeProduct(
            title='Mug', price='$10', product_url='https://ex.com/mug'
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

        s: set = {a, b}
        self.assertEqual(len(s), 1)

    def test_repr(self) -> None:
        p = YouTubeProduct(
            title='Mug', price='$14.99', merchant_name='Spring'
        )
        self.assertIn('Mug', repr(p))
        self.assertIn('$14.99', repr(p))
        self.assertIn('Spring', repr(p))

    def test_equality_false_for_different_type(self) -> None:
        p = YouTubeProduct(title='X')
        self.assertNotEqual(p, 'not a product')

    def test_defaults(self) -> None:
        p = YouTubeProduct()
        self.assertIsNone(p.title)
        self.assertIsNone(p.price)
        self.assertIsNone(p.merchant_name)
        self.assertIsNone(p.thumbnail_url)
        self.assertIsNone(p.product_url)
        self.assertIsNone(p.accessibility_title)
        self.assertIsNone(p.channel_id)
        self.assertIsNotNone(p.created_timestamp)


if __name__ == '__main__':
    unittest.main()
