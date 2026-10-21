'''
yt-rss fails a channel's scrape when its new videos cannot be added
to the video scrape queue, so the worker retries the channel instead
of losing those videos until the next feed read.
'''

import asyncio
import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from httpx2 import Response

from scrape_exchange.youtube._rss_circuit_state import CircuitReport
from scrape_exchange.youtube.youtube_video import YouTubeVideo

from tools import yt_rss_scrape


def _video(video_id: str) -> YouTubeVideo:
    video: YouTubeVideo = YouTubeVideo(video_id=video_id)
    video.channel_id = 'UCX'
    return video


async def _process(enqueue_side_effect: Any) -> object:
    '''Run process_channel on a feed of two new videos.'''
    videos: list[YouTubeVideo] = [_video('new-one'), _video('new-two')]
    with tempfile.TemporaryDirectory() as data_dir:
        settings: MagicMock = MagicMock()
        settings.redis_dsn = 'redis://fake'
        settings.exchange_url = 'https://scrape.exchange'
        settings.video_data_directory = data_dir
        settings.channel_min_subscribers = 0

        client: MagicMock = MagicMock()
        client.get = AsyncMock(return_value=Response(404))

        video_queue: MagicMock = MagicMock()
        video_queue.get_states = AsyncMock(return_value={})
        video_queue.enqueue = AsyncMock(side_effect=enqueue_side_effect)

        uploaded: MagicMock = MagicMock()
        uploaded.contains_many = AsyncMock(return_value={})

        creator_queue: MagicMock = MagicMock()
        creator_queue.has_had_feed = AsyncMock(return_value=True)
        creator_queue.get_no_feeds = AsyncMock(return_value=None)
        creator_queue.update_tier = AsyncMock()
        creator_queue.set_no_feeds = AsyncMock()
        creator_queue.rollback_no_feeds = AsyncMock()
        creator_queue.mark_had_feed = AsyncMock()
        creator_queue.clear_no_feeds = AsyncMock()

        breaker: MagicMock = MagicMock()
        breaker.acquire = AsyncMock(return_value=0.0)
        breaker.report = AsyncMock(return_value=CircuitReport(
            transition=None,
            suppress_channel_failure=False,
            rollback_channel_ids=[],
            state_after=None,
        ))

        with patch.object(
            yt_rss_scrape, '_fetch_rss_safe',
            new=AsyncMock(return_value=videos),
        ), patch.object(
            yt_rss_scrape, 'update_channel',
            new=AsyncMock(return_value=(True, 0, 'handle')),
        ), patch.object(
            yt_rss_scrape, '_get_rss_circuit_breaker',
            return_value=breaker,
        ):
            return await yt_rss_scrape.process_channel(
                channel_handle='handle',
                channel_id='UCX',
                client=client,
                creator_queue=creator_queue,
                settings=settings,
                creator_map_backend=AsyncMock(),
                name_map_backend=AsyncMock(),
                channel_validator=MagicMock(),
                tier=1,
                video_queue=video_queue,
                uploaded_videos=uploaded,
            )


class TestRssEnqueueFailure(unittest.IsolatedAsyncioTestCase):

    async def test_all_enqueued_is_success(self) -> None:
        self.assertIs(await _process(None), True)

    async def test_queue_unreachable_fails_the_channel(self) -> None:
        def side_effect(video_id: str, **_: Any) -> bool:
            if video_id == 'new-two':
                raise ConnectionError('backlog-server unreachable')
            return True

        with self.assertRaises(RuntimeError) as ctx:
            await _process(side_effect)
        self.assertIn('could not be queued', str(ctx.exception))
        self.assertIn('1 of 2', str(ctx.exception))

    async def test_malformed_video_id_is_skipped_not_failed(
        self,
    ) -> None:
        def side_effect(video_id: str, **_: Any) -> bool:
            if video_id == 'new-two':
                raise ValueError(f'invalid video_id: {video_id}')
            return True

        self.assertIs(await _process(side_effect), True)


if __name__ == '__main__':
    unittest.main()
