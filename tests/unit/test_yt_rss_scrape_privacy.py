'''
Privacy limits in the YouTube RSS scraper: lite channel records of
tiny channels are saved as ``.low_subs``, small channels are masked,
and videos of tiny channels are not queued.
'''

import shutil
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

from tests.unit.test_channel_min_subscribers import (
    yt_rss_scrape as _yt_rss_scrape,
)


yt_rss_scrape: ModuleType = _yt_rss_scrape
CHANNEL_ID: str = 'UC_xyz'


def _settings(base_dir: str) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.channel_data_directory = base_dir
    settings.channel_min_subscribers = 10
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    settings.extra_allowed_domains = frozenset()
    return settings


def _record(
    subscriber_count: int | None, view_count: int | None = None,
) -> dict:
    record: dict = {
        'channel_id': CHANNEL_ID,
        'channel_handle': 'handle',
        'url': f'https://www.youtube.com/channel/{CHANNEL_ID}',
        'title': 'Channel',
        'video_count': 3,
        'description': 'site https://myblog.example',
    }
    if subscriber_count is not None:
        record['subscriber_count'] = subscriber_count
    if view_count is not None:
        record['view_count'] = view_count
    return record


class TestRssPrivacyLimits(unittest.TestCase):

    def setUp(self) -> None:
        self.settings: MagicMock = _settings('/unused')

    def test_tiny_is_low_subs(self) -> None:
        record: dict = _record(3)
        self.assertEqual(
            yt_rss_scrape._apply_rss_privacy_limits(record, self.settings),
            '.low_subs')
        self.assertEqual(record['description'], 'site https://myblog.example')

    def test_unknown_count_is_low_subs(self) -> None:
        self.assertEqual(
            yt_rss_scrape._apply_rss_privacy_limits(
                _record(None), self.settings),
            '.low_subs')

    def test_small_is_masked(self) -> None:
        record: dict = _record(500)
        self.assertEqual(
            yt_rss_scrape._apply_rss_privacy_limits(record, self.settings),
            '')
        self.assertEqual(record['description'], 'site <url withheld>')

    def test_unknown_subs_with_views(self) -> None:
        record: dict = _record(None, 5_000_000)
        self.assertEqual(
            yt_rss_scrape._apply_rss_privacy_limits(record, self.settings),
            '')
        self.assertEqual(record['description'], 'site https://myblog.example')

    def test_large_unchanged(self) -> None:
        record: dict = _record(50_000)
        self.assertEqual(
            yt_rss_scrape._apply_rss_privacy_limits(record, self.settings),
            '')
        self.assertEqual(record['description'], 'site https://myblog.example')


class TestRssTinyGate(unittest.TestCase):

    def test_gate(self) -> None:
        settings: MagicMock = MagicMock()
        settings.channel_min_subscribers = 10
        self.assertTrue(yt_rss_scrape._rss_channel_is_tiny(3, settings))
        self.assertTrue(yt_rss_scrape._rss_channel_is_tiny(None, settings))
        self.assertFalse(yt_rss_scrape._rss_channel_is_tiny(10, settings))
        settings.channel_min_subscribers = 0
        self.assertFalse(
            yt_rss_scrape._rss_channel_is_tiny(None, settings))


class TestWriteChannelMarker(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    async def test_low_subs_suffix(self) -> None:
        await yt_rss_scrape._write_channel(
            _record(3), _settings(self.tmp), marker_suffix='.low_subs',
        )
        base: Path = Path(self.tmp) / f'channel-rss-{CHANNEL_ID}.json.br'
        self.assertTrue(Path(f'{base}.low_subs').exists())
        self.assertFalse(base.exists())


if __name__ == '__main__':
    unittest.main()
