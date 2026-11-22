'''Tests that the YouTube video, RSS and channel scrapers give each
supervisor child its own slice of the proxy pool.'''

import os
import tempfile
import unittest
from types import ModuleType
from unittest.mock import MagicMock, patch

import tools.yt_channel_scrape as channel_tool
import tools.yt_rss_scrape as rss_tool
import tools.yt_video_scrape as video_tool


class TestSplitProxyPool(unittest.TestCase):

    def setUp(self) -> None:
        self._dir: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.proxies: str = ','.join(
            f'http://p{i}:8080' for i in range(1, 167)
        )

    def tearDown(self) -> None:
        self._dir.cleanup()

    def _kwargs(
        self, tool: ModuleType, script: str, env: dict[str, str],
    ) -> dict:
        '''Run *tool*.main() with ScraperRunner mocked out and
        return the keyword arguments ScraperRunner was built with.'''
        with patch.dict(os.environ, env, clear=True), patch.object(
            tool.sys, 'argv', [script],
        ), patch.object(
            tool, 'ScraperRunner',
        ) as runner_cls:
            runner: MagicMock = runner_cls.return_value
            runner.run_sync.return_value = 0
            with self.assertRaises(SystemExit):
                tool.main()
        return runner_cls.call_args.kwargs

    def _assert_split(
        self, kwargs: dict, num_processes: int, concurrency: int,
    ) -> None:
        self.assertTrue(kwargs['split_proxy_pool'])
        self.assertEqual(kwargs['num_processes'], num_processes)
        self.assertEqual(kwargs['concurrency'], concurrency)

    def test_video(self) -> None:
        kwargs: dict = self._kwargs(
            video_tool, 'yt_video_scrape.py', {
                'PROXIES': self.proxies,
                'VIDEO_NUM_PROCESSES': '12',
                'VIDEO_CONCURRENCY': '32',
                'YTDLP_CACHE_DIR': os.path.join(
                    self._dir.name, 'cache',
                ),
            },
        )
        self._assert_split(kwargs, 12, 32)

    def test_rss(self) -> None:
        kwargs: dict = self._kwargs(
            rss_tool, 'yt_rss_scrape.py', {
                'PROXIES': self.proxies,
                'RSS_NUM_PROCESSES': '4',
                'RSS_CONCURRENCY': '24',
                'API_KEY_ID': 'test-id',
                'API_KEY_SECRET': 'test-secret',
            },
        )
        # 166 proxies over 4 children: slices of at most 42.
        self._assert_split(kwargs, 4, 42)

    def test_channel(self) -> None:
        # _validate_settings checks the data directory and API key;
        # it is not what this test is about.
        with patch.object(channel_tool, '_validate_settings'):
            kwargs: dict = self._kwargs(
                channel_tool, 'yt_channel_scrape.py', {
                    'PROXIES': self.proxies,
                    'CHANNEL_NUM_PROCESSES': '2',
                    'CHANNEL_CONCURRENCY': '12',
                },
            )
        # 166 proxies over 2 children: slices of 83.
        self._assert_split(kwargs, 2, 83)


if __name__ == '__main__':
    unittest.main()
