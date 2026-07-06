'''
Unit tests for Instagram queue normalization.
'''

import unittest

from scrape_exchange.queue_admin import (
    normalize_instagram_creator_handle,
)


class TestInstagramCreatorHandleNormalization(unittest.TestCase):

    def test_accepts_bare_at_and_url(self) -> None:
        self.assertEqual(
            normalize_instagram_creator_handle('NatGeo'),
            'natgeo',
        )
        self.assertEqual(
            normalize_instagram_creator_handle('@NatGeo'),
            'natgeo',
        )
        self.assertEqual(
            normalize_instagram_creator_handle(
                'https://www.instagram.com/NatGeo/?hl=en',
            ),
            'natgeo',
        )

    def test_rejects_reserved_and_invalid_handles(self) -> None:
        self.assertIsNone(
            normalize_instagram_creator_handle(
                'https://www.instagram.com/explore/',
            ),
        )
        self.assertIsNone(normalize_instagram_creator_handle('.bad'))
        self.assertIsNone(normalize_instagram_creator_handle('bad.'))
        self.assertIsNone(normalize_instagram_creator_handle('bad..name'))
        self.assertIsNone(normalize_instagram_creator_handle('bad-name'))


if __name__ == '__main__':
    unittest.main()
