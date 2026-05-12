'''Tests for scrape_exchange.http_timeouts.

Each scraper type pulls its connect / request timeout from
its own pair of env vars (``RSS_*``, ``CHANNEL_*``,
``VIDEO_*``). The module also resolves a pair of
convenience aliases at import time based on
``sys.argv[0]`` so existing import sites can stay
unchanged.
'''

import importlib
import sys
import unittest
from unittest import mock


class TestHttpTimeoutSettingsDefaults(unittest.TestCase):
    '''Default values match the RSS playbook recommendation:
    5s connect, 10s request — across all three scraper
    types.'''

    def test_defaults_when_no_env(self) -> None:
        with mock.patch.dict('os.environ', {}, clear=True):
            import scrape_exchange.http_timeouts as h
            importlib.reload(h)
            self.assertEqual(
                h.HTTP_TIMEOUTS.rss_connect_timeout, 5.0,
            )
            self.assertEqual(
                h.HTTP_TIMEOUTS.rss_request_timeout, 30.0,
            )
            self.assertEqual(
                h.HTTP_TIMEOUTS.channel_connect_timeout,
                5.0,
            )
            self.assertEqual(
                h.HTTP_TIMEOUTS.channel_request_timeout,
                10.0,
            )
            self.assertEqual(
                h.HTTP_TIMEOUTS.video_connect_timeout, 5.0,
            )
            self.assertEqual(
                h.HTTP_TIMEOUTS.video_request_timeout,
                10.0,
            )


class TestPerScraperResolution(unittest.TestCase):
    '''The convenience aliases ``HTTP_CONNECT_TIMEOUT`` and
    ``HTTP_REQUEST_TIMEOUT`` resolve to the values for the
    scraper named by ``sys.argv[0]``.'''

    def _reload_with(
        self,
        script: str,
        env: dict[str, str],
    ) -> object:
        with mock.patch.object(sys, 'argv', [script]), \
                mock.patch.dict(
                    'os.environ', env, clear=True,
                ):
            import scrape_exchange.http_timeouts as h
            importlib.reload(h)
            return h

    def test_rss_scraper_picks_rss_values(self) -> None:
        h = self._reload_with(
            'yt_rss_scrape.py',
            {
                'RSS_CONNECT_TIMEOUT': '2.5',
                'RSS_REQUEST_TIMEOUT': '7.5',
                'CHANNEL_CONNECT_TIMEOUT': '99',
            },
        )
        self.assertEqual(h.HTTP_CONNECT_TIMEOUT, 2.5)
        self.assertEqual(h.HTTP_REQUEST_TIMEOUT, 7.5)

    def test_channel_scraper_picks_channel_values(
        self,
    ) -> None:
        h = self._reload_with(
            'yt_channel_scrape.py',
            {
                'RSS_CONNECT_TIMEOUT': '99',
                'CHANNEL_CONNECT_TIMEOUT': '4.0',
                'CHANNEL_REQUEST_TIMEOUT': '12.5',
            },
        )
        self.assertEqual(h.HTTP_CONNECT_TIMEOUT, 4.0)
        self.assertEqual(h.HTTP_REQUEST_TIMEOUT, 12.5)

    def test_video_scraper_picks_video_values(self) -> None:
        h = self._reload_with(
            'yt_video_scrape.py',
            {
                'VIDEO_CONNECT_TIMEOUT': '3.0',
                'VIDEO_REQUEST_TIMEOUT': '8.0',
            },
        )
        self.assertEqual(h.HTTP_CONNECT_TIMEOUT, 3.0)
        self.assertEqual(h.HTTP_REQUEST_TIMEOUT, 8.0)

    def test_unknown_script_falls_back_to_rss(
        self,
    ) -> None:
        '''A script not in the resolution map (e.g. tests
        running under unittest) defaults to RSS values
        rather than crashing.'''
        h = self._reload_with(
            'some_other_tool.py',
            {
                'RSS_CONNECT_TIMEOUT': '1.5',
                'RSS_REQUEST_TIMEOUT': '4.5',
            },
        )
        self.assertEqual(h.HTTP_CONNECT_TIMEOUT, 1.5)
        self.assertEqual(h.HTTP_REQUEST_TIMEOUT, 4.5)


if __name__ == '__main__':
    unittest.main()
