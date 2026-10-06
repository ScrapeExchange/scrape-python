'''
Tests for the channel_subscriber_count_parse_total metric that
records whether a subscriber count could be parsed from a scraped
YouTube channel page.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

from prometheus_client import REGISTRY

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube.youtube_client import _get_scraper
from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    record_subscriber_count_parse,
)


METRIC_NAME: str = 'channel_subscriber_count_parse_total'


def _sample(
    source: str, outcome: str, scraper: str | None = None,
) -> float:
    value: float | None = REGISTRY.get_sample_value(
        METRIC_NAME,
        {
            'platform': 'youtube',
            'scraper': scraper or _get_scraper(),
            'source': source,
            'outcome': outcome,
            'worker_id': get_worker_id(),
        },
    )
    return value or 0.0


def _header_with(content_text: str) -> dict:
    return {
        'header': {
            'pageHeaderRenderer': {
                'content': {
                    'pageHeaderViewModel': {
                        'metadata': {
                            'contentMetadataViewModel': {
                                'metadataRows': [
                                    {
                                        'metadataParts': [
                                            {
                                                'text': {
                                                    'content': (
                                                        content_text
                                                    ),
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                },
            },
        },
        'metadata': {
            'channelMetadataRenderer': {'title': 'Test'},
        },
    }


def _load_yt_rss_scrape() -> ModuleType:
    for key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        if key in sys.modules:
            return sys.modules[key]
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_rss_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


class TestRecordSubscriberCountParse(unittest.TestCase):
    def test_found_and_missing(self) -> None:
        found_before: float = _sample('innertube', 'found')
        missing_before: float = _sample('innertube', 'missing')
        record_subscriber_count_parse('innertube', 0)
        record_subscriber_count_parse('innertube', None)
        self.assertEqual(
            _sample('innertube', 'found'), found_before + 1,
        )
        self.assertEqual(
            _sample('innertube', 'missing'), missing_before + 1,
        )


class TestChannelParsersRecordMetric(unittest.TestCase):
    def test_innertube_found(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('innertube', 'found')
        channel.parse_channel_video_data(
            _header_with('1.2K subscribers'),
        )
        self.assertEqual(channel.subscriber_count, 1200)
        self.assertEqual(_sample('innertube', 'found'), before + 1)

    def test_innertube_missing(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('innertube', 'missing')
        channel.parse_channel_video_data({
            'metadata': {'channelMetadataRenderer': {'title': 'T'}},
        })
        self.assertEqual(_sample('innertube', 'missing'), before + 1)

    def test_about_page_found(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('about_page', 'found')
        channel._parse_channel_about_data({
            'subscriberCountText': {'simpleText': '3M subscribers'},
        })
        self.assertEqual(channel.subscriber_count, 3000000)
        self.assertEqual(_sample('about_page', 'found'), before + 1)

    def test_about_page_missing(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('about_page', 'missing')
        channel._parse_channel_about_data({})
        self.assertIsNone(channel.subscriber_count)
        self.assertEqual(_sample('about_page', 'missing'), before + 1)

    def test_about_page_keeps_innertube_count(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        channel.subscriber_count = 42
        before: float = _sample('about_page', 'found')
        channel._parse_channel_about_data({
            'subscriberCountText': {'simpleText': '3M subscribers'},
        })
        self.assertEqual(channel.subscriber_count, 42)
        self.assertEqual(_sample('about_page', 'found'), before + 1)


class TestRssUpdateChannelRecordsMetric(
    unittest.IsolatedAsyncioTestCase,
):
    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.module: ModuleType = _load_yt_rss_scrape()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def _run(self, page: dict) -> int:
        settings: mock.MagicMock = mock.MagicMock()
        settings.channel_data_directory = self.tmp
        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = None
        with mock.patch.object(
            self.module, 'YouTubeChannelTabs',
        ) as tabs_cls, mock.patch.object(
            self.module,
            'canonical_handle_from_browse',
            return_value='canonhandle',
        ):
            tabs_cls.return_value.browse_channel = (
                mock.AsyncMock(return_value=page)
            )
            subs: int
            _, subs, _ = await self.module.update_channel(
                channel_handle='inputhandle',
                channel_id='UC_xyz',
                creator_map_backend=mock.AsyncMock(),
                name_map_backend=mock.AsyncMock(),
                validator=validator,
                proxy=None,
                settings=settings,
            )
        return subs

    async def test_found(self) -> None:
        before: float = _sample('innertube', 'found')
        subs: int = await self._run(_header_with('7 subscribers'))
        self.assertEqual(subs, 7)
        self.assertEqual(_sample('innertube', 'found'), before + 1)

    async def test_missing(self) -> None:
        before: float = _sample('innertube', 'missing')
        subs: int = await self._run({
            'metadata': {'channelMetadataRenderer': {'title': 'T'}},
        })
        self.assertEqual(subs, 0)
        self.assertEqual(_sample('innertube', 'missing'), before + 1)


if __name__ == '__main__':
    unittest.main()
