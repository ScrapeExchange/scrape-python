'''
Unit tests for the channel scraper's proxy_file label wiring.

The channel scraper's HTTP work goes through curl_cffi-backed
clients (AsyncYouTubeClient / InnerTube), so there is no direct
httpx codepath here for bind_failed detection. What this test
pins is the label-emission side: ``METRIC_CHANNELS_SCRAPED``,
``METRIC_CHANNEL_NO_CONTENT_FOUND``, and ``METRIC_SCRAPE_FAILURES``
must each carry the ``proxy_file`` label and read it from the
active proxy catalog.
'''

import unittest

from scrape_exchange.proxy_loader import (
    ProxyCatalog,
    set_active_catalog,
)
from tools import yt_channel_scrape


class TestChannelScraperProxyFileLabel(unittest.TestCase):

    def setUp(self) -> None:
        catalog: ProxyCatalog = ProxyCatalog(
            entries=['http://1.2.3.4:8080'],
            source={'http://1.2.3.4:8080': 'hype'},
        )
        set_active_catalog(catalog)

    def tearDown(self) -> None:
        set_active_catalog(ProxyCatalog())

    def test_channels_scraped_metric_has_proxy_file_label(
        self,
    ) -> None:
        labelnames: tuple[str, ...] = (
            yt_channel_scrape
            .METRIC_CHANNELS_SCRAPED._labelnames
        )
        self.assertIn('proxy_file', labelnames)

    def test_no_content_metric_has_proxy_file_label(self) -> None:
        labelnames: tuple[str, ...] = (
            yt_channel_scrape
            .METRIC_CHANNEL_NO_CONTENT_FOUND._labelnames
        )
        self.assertIn('proxy_file', labelnames)

    def test_scrape_failures_metric_has_proxy_file_label(
        self,
    ) -> None:
        labelnames: tuple[str, ...] = (
            yt_channel_scrape
            .METRIC_SCRAPE_FAILURES._labelnames
        )
        self.assertIn('proxy_file', labelnames)

    def test_proxy_file_label_resolves_from_active_catalog(
        self,
    ) -> None:
        '''proxy_file_label is the seam every metric site uses;
        verifying the catalog lookup pins the label against the
        configured source mapping.'''
        from scrape_exchange.proxy_loader import proxy_file_label
        self.assertEqual(
            proxy_file_label('http://1.2.3.4:8080'), 'hype',
        )
        self.assertEqual(
            proxy_file_label('http://9.9.9.9:80'), 'none',
        )


if __name__ == '__main__':
    unittest.main()
