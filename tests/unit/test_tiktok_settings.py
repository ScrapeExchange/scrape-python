'''
Unit tests for ``scrape_exchange.tiktok.settings``.
'''

import os
import unittest
from unittest.mock import patch


class TestTikTokScraperSettings(unittest.TestCase):

    def setUp(self) -> None:
        # ScraperSettings reads sys.argv via cli_parse_args=True.
        # Suppress that for unit tests by clearing argv during init.
        self._argv_patch = patch('sys.argv', ['test'])
        self._argv_patch.start()

    def tearDown(self) -> None:
        self._argv_patch.stop()

    def test_defaults(self) -> None:
        from scrape_exchange.tiktok.settings import (
            TikTokScraperSettings,
        )
        s: TikTokScraperSettings = TikTokScraperSettings()
        self.assertIsNone(s.creator_data_directory)
        self.assertIsNone(s.video_data_directory)
        self.assertIsNone(s.hashtag_data_directory)
        self.assertEqual(s.creator_list, 'tiktok_creators.lst')
        self.assertEqual(
            s.creator_map_file, 'tiktok_creator_map.csv',
        )
        self.assertEqual(
            s.session_state_dir, '/tmp/scrape_exchange/tiktok',
        )
        self.assertEqual(s.ms_token_ttl_seconds, 14400)
        self.assertEqual(s.bulk_batch_size, 1000)
        self.assertEqual(s.bulk_max_batch_bytes, 7 * 1024 ** 3)
        self.assertEqual(s.bulk_progress_timeout_seconds, 1800.0)

    def test_env_aliases(self) -> None:
        from scrape_exchange.tiktok.settings import (
            TikTokScraperSettings,
        )
        env: dict = {
            'TIKTOK_CREATOR_DATA_DIR': '/data/tt/creators',
            'TIKTOK_VIDEO_DATA_DIR': '/data/tt/videos',
            'TIKTOK_HASHTAG_DATA_DIR': '/data/tt/hashtags',
            'TIKTOK_CREATOR_LIST': 'creators.lst',
            'TIKTOK_CREATOR_MAP_FILE': 'cmap.csv',
            'TIKTOK_SESSION_STATE_DIR': '/var/lib/tiktok',
            'TIKTOK_MS_TOKEN_TTL': '7200',
        }
        with patch.dict(os.environ, env, clear=False):
            s: TikTokScraperSettings = TikTokScraperSettings()
            self.assertEqual(
                s.creator_data_directory, '/data/tt/creators',
            )
            self.assertEqual(
                s.video_data_directory, '/data/tt/videos',
            )
            self.assertEqual(
                s.hashtag_data_directory, '/data/tt/hashtags',
            )
            self.assertEqual(s.creator_list, 'creators.lst')
            self.assertEqual(s.creator_map_file, 'cmap.csv')
            self.assertEqual(s.session_state_dir, '/var/lib/tiktok')
            self.assertEqual(s.ms_token_ttl_seconds, 7200)


if __name__ == '__main__':
    unittest.main()
