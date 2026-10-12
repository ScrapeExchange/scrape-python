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
    is_retry: str = 'false',
) -> float:
    value: float | None = REGISTRY.get_sample_value(
        METRIC_NAME,
        {
            'platform': 'youtube',
            'scraper': scraper or _get_scraper(),
            'source': source,
            'outcome': outcome,
            'is_retry': is_retry,
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
        missing_before: float = _sample('innertube', 'absent')
        record_subscriber_count_parse('innertube', 'found')
        record_subscriber_count_parse('innertube', 'absent')
        self.assertEqual(
            _sample('innertube', 'found'), found_before + 1,
        )
        self.assertEqual(
            _sample('innertube', 'absent'), missing_before + 1,
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
        before: float = _sample('innertube', 'absent')
        channel.parse_channel_video_data({
            'metadata': {'channelMetadataRenderer': {'title': 'T'}},
        })
        self.assertEqual(_sample('innertube', 'absent'), before + 1)

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
        before: float = _sample('about_page', 'absent')
        channel._parse_channel_about_data({})
        self.assertIsNone(channel.subscriber_count)
        self.assertEqual(_sample('about_page', 'absent'), before + 1)

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

    async def _run(self, page: dict) -> int | None:
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
            subs: int | None
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
        before: float = _sample('innertube', 'absent')
        subs: int | None = await self._run({
            'metadata': {'channelMetadataRenderer': {'title': 'T'}},
        })
        # Unknown, not zero: CHANNEL_MIN_SUBSCRIBERS must not treat a
        # hidden count as a tiny channel.
        self.assertIsNone(subs)
        self.assertEqual(_sample('innertube', 'absent'), before + 1)


class TestSubscriberCountParseOutcomes(unittest.TestCase):
    '''``found`` / ``absent`` / ``unparseable`` from both page
    sources, plus the ``is_retry`` label.'''

    def test_parser_reports_absent_without_count_text(self) -> None:
        self.assertEqual(
            YouTubeChannel.parse_subscriber_count_with_outcome({}),
            (None, 'absent'),
        )

    def test_parser_reports_found(self) -> None:
        self.assertEqual(
            YouTubeChannel.parse_subscriber_count_with_outcome(
                _header_with('1.2K subscribers'),
            ),
            (1200, 'found'),
        )

    def test_parser_reports_unparseable(self) -> None:
        # A row that ends in " subscribers" but carries no number:
        # present text that convert_number_string cannot convert.
        data: dict = _header_with(' subscribers')
        self.assertEqual(
            YouTubeChannel.parse_subscriber_count_with_outcome(data),
            (None, 'unparseable'),
        )
        # The count-only wrapper keeps its contract.
        self.assertIsNone(YouTubeChannel.parse_subscriber_count(data))

    def test_parser_reports_unparseable_for_non_numeric_text(
        self,
    ) -> None:
        self.assertEqual(
            YouTubeChannel.parse_subscriber_count_with_outcome(
                _header_with('No subscribers'),
            ),
            (None, 'unparseable'),
        )

    def test_innertube_unparseable_records_outcome(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('innertube', 'unparseable')
        channel.parse_channel_video_data(_header_with(' subscribers'))
        self.assertIsNone(channel.subscriber_count)
        self.assertEqual(
            _sample('innertube', 'unparseable'), before + 1,
        )

    def test_about_page_unparseable_records_outcome(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        before: float = _sample('about_page', 'unparseable')
        channel._parse_channel_about_data({
            'subscriberCountText': {'simpleText': ' subscribers'},
        })
        self.assertIsNone(channel.subscriber_count)
        self.assertEqual(
            _sample('about_page', 'unparseable'), before + 1,
        )

    def test_is_retry_label_is_recorded(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='Test', with_download_client=False,
        )
        channel.subscriber_count_retry = True
        found_before: float = _sample(
            'innertube', 'found', is_retry='true',
        )
        absent_before: float = _sample(
            'innertube', 'absent', is_retry='true',
        )
        plain_before: float = _sample('innertube', 'found')
        channel.parse_channel_video_data(_header_with('7 subscribers'))
        channel.parse_channel_video_data({
            'metadata': {'channelMetadataRenderer': {'title': 'T'}},
        })
        self.assertEqual(
            _sample('innertube', 'found', is_retry='true'),
            found_before + 1,
        )
        self.assertEqual(
            _sample('innertube', 'absent', is_retry='true'),
            absent_before + 1,
        )
        # A retry scrape must not be counted as a plain scrape.
        self.assertEqual(_sample('innertube', 'found'), plain_before)


if __name__ == '__main__':
    unittest.main()
