'''
CHANNEL_MIN_SUBSCRIBERS: channels below the minimum are still scraped,
but their videos are not added to the video scrape queue by the RSS
scraper. Unknown subscriber counts never block queuing.
'''

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube._rss_circuit_state import CircuitReport
from scrape_exchange.youtube.channel_video_refresh import (
    below_min_subscribers,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


def _load_yt_rss_scrape() -> ModuleType:
    '''Load the RSS tool under the bare ``yt_rss_scrape`` key (and
    alias it as ``tools.yt_rss_scrape``) like the other RSS tests, so
    full discovery never executes the module twice and re-registers
    its Prometheus metrics.'''
    for key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        cached: ModuleType | None = sys.modules.get(key)
        if cached is not None:
            sys.modules.setdefault('yt_rss_scrape', cached)
            return cached
    path: Path = (
        Path(__file__).resolve().parents[2] / 'tools' / 'yt_rss_scrape.py'
    )
    spec = importlib.util.spec_from_file_location('yt_rss_scrape', path)
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


class TestBelowMinSubscribers(unittest.TestCase):

    def test_disabled_when_minimum_is_zero(self) -> None:
        self.assertFalse(below_min_subscribers(0, 0))
        self.assertFalse(below_min_subscribers(3, 0))

    def test_known_count_below_minimum(self) -> None:
        self.assertTrue(below_min_subscribers(0, 10))
        self.assertTrue(below_min_subscribers(9, 10))

    def test_count_at_or_above_minimum(self) -> None:
        self.assertFalse(below_min_subscribers(10, 10))
        self.assertFalse(below_min_subscribers(5000, 10))

    def test_unknown_count_never_blocks(self) -> None:
        self.assertFalse(below_min_subscribers(None, 10))


class TestSetting(unittest.TestCase):

    def tearDown(self) -> None:
        os.environ.pop('CHANNEL_MIN_SUBSCRIBERS', None)

    def test_defaults_to_ten(self) -> None:
        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(settings.channel_min_subscribers, 10)

    def test_reads_env(self) -> None:
        os.environ['CHANNEL_MIN_SUBSCRIBERS'] = '25'
        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(settings.channel_min_subscribers, 25)


class TestRssSkipsSmallChannels(unittest.IsolatedAsyncioTestCase):

    async def _process(
        self, subscriber_count: int | None, minimum: int,
    ) -> MagicMock:
        video: YouTubeVideo = YouTubeVideo(video_id='new-one')
        video.channel_id = 'UCX'
        settings = MagicMock()
        settings.exchange_url = 'https://scrape.exchange'
        data_dir: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(data_dir.cleanup)
        settings.video_data_directory = data_dir.name
        settings.channel_min_subscribers = minimum

        client = MagicMock()
        client.get = AsyncMock(return_value=MagicMock(status_code=404))
        video_queue = MagicMock()
        video_queue.get_states = AsyncMock(return_value={'new-one': None})
        video_queue.enqueue = AsyncMock()
        uploaded = MagicMock()
        uploaded.contains_many = AsyncMock(return_value={'new-one': False})
        creator_queue = MagicMock()
        for name in (
            'update_tier', 'set_no_feeds', 'rollback_no_feeds',
            'mark_had_feed', 'clear_no_feeds',
        ):
            setattr(creator_queue, name, AsyncMock())
        creator_queue.has_had_feed = AsyncMock(return_value=True)
        creator_queue.get_no_feeds = AsyncMock(return_value=None)
        breaker = MagicMock()
        breaker.acquire = AsyncMock(return_value=0.0)
        breaker.report = AsyncMock(return_value=CircuitReport(
            transition=None, suppress_channel_failure=False,
            rollback_channel_ids=[], state_after=None,
        ))
        with patch.object(
            yt_rss_scrape, '_fetch_rss_safe',
            new=AsyncMock(return_value=[video]),
        ), patch.object(
            yt_rss_scrape, 'update_channel',
            new=AsyncMock(
                return_value=(True, subscriber_count, 'handle'),
            ),
        ), patch.object(
            yt_rss_scrape, '_get_rss_circuit_breaker',
            return_value=breaker,
        ), patch.object(
            yt_rss_scrape, 'check_video_exists',
            new=AsyncMock(return_value=False),
        ):
            result = await yt_rss_scrape.process_channel(
                channel_handle='handle', channel_id='UCX',
                client=client, creator_queue=creator_queue,
                settings=settings,
                creator_map_backend=AsyncMock(),
                name_map_backend=AsyncMock(),
                channel_validator=MagicMock(), tier=1,
                video_queue=video_queue, uploaded_videos=uploaded,
            )
        self.assertIs(result, True)
        self.creator_queue = creator_queue
        return video_queue

    async def test_below_minimum_is_not_queued(self) -> None:
        queue: MagicMock = await self._process(3, 10)
        queue.enqueue.assert_not_awaited()
        # The tier still reflects the real (small) count.
        self.creator_queue.update_tier.assert_awaited_once_with('UCX', 3)

    async def test_at_minimum_is_queued(self) -> None:
        queue: MagicMock = await self._process(10, 10)
        queue.enqueue.assert_awaited_once()

    async def test_unknown_count_is_queued(self) -> None:
        queue: MagicMock = await self._process(None, 10)
        queue.enqueue.assert_awaited_once()
        # Unknown keeps the previous tier routing input of 0.
        self.creator_queue.update_tier.assert_awaited_once_with('UCX', 0)

    async def test_disabled_queues_tiny_channel(self) -> None:
        queue: MagicMock = await self._process(0, 0)
        queue.enqueue.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
