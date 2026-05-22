'''Tests for the RSS scraper's video scrape request handoff.'''

import importlib.util
import sys
import unittest
from types import ModuleType
from unittest import mock


def _load_yt_rss_scrape() -> ModuleType:
    for _key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        cached: ModuleType | None = sys.modules.get(_key)
        if cached is not None:
            return cached
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape',
        '/home/steven/src/scrape-python/tools/'
        'yt_rss_scrape.py',
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


class TestQueueVideoForScrape(
    unittest.IsolatedAsyncioTestCase,
):
    '''``_queue_video_for_scrape`` enqueues the video id onto the
    Redis-backed video scrape queue.'''

    async def asyncSetUp(self) -> None:
        self.module: ModuleType = _load_yt_rss_scrape()

    def _video(self, video_id: str) -> mock.MagicMock:
        v: mock.AsyncMock = mock.AsyncMock()
        v.video_id = video_id
        v.from_innertube = mock.AsyncMock()
        v.to_file = mock.AsyncMock()
        return v

    def _queue(self) -> mock.AsyncMock:
        q: mock.AsyncMock = mock.AsyncMock()
        q.enqueue = mock.AsyncMock()
        return q

    async def test_enqueues_video_id_with_rss_source(
        self,
    ) -> None:
        video = self._video('vid_a')
        queue: mock.AsyncMock = self._queue()
        result: str | None = (
            await self.module._queue_video_for_scrape(
                video,
                channel_handle='hh',
                video_queue=queue,
            )
        )
        self.assertEqual(result, 'vid_a')
        queue.enqueue.assert_awaited_once_with(
            'vid_a', source='rss',
        )
        video.from_innertube.assert_not_awaited()
        video.to_file.assert_not_awaited()

    async def test_returns_none_when_enqueue_raises(
        self,
    ) -> None:
        video = self._video('vid_b')
        queue: mock.AsyncMock = self._queue()
        queue.enqueue.side_effect = RuntimeError('redis down')
        result: str | None = (
            await self.module._queue_video_for_scrape(
                video,
                channel_handle='hh',
                video_queue=queue,
            )
        )
        self.assertIsNone(result)
        queue.enqueue.assert_awaited_once()
        video.from_innertube.assert_not_awaited()
        video.to_file.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
