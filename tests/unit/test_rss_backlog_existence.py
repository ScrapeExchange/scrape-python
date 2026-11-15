'''
Tests for skipping the RSS scraper's scrape.exchange existence GET when
the backlog server records every upload.
'''

import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from httpx2 import Response

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)
from scrape_exchange.youtube._rss_circuit_state import CircuitReport
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from tests.unit.test_backlog_server_queue import FakeRedis
from tools import yt_rss_scrape


def _backlog_queue(
    *, fallback: bool, supports_uploaded: bool = True,
) -> BacklogServerVideoScrapeQueue:
    settings: VideoScrapeQueueSettings = VideoScrapeQueueSettings(
        backlog_uploaded_api_fallback=fallback,  # type: ignore[call-arg]
    )
    return BacklogServerVideoScrapeQueue(
        FakeRedis(),  # type: ignore[arg-type]
        settings,
        supports_uploaded=supports_uploaded,
    )


class TestKnowsEveryUpload(unittest.TestCase):

    def test_backlog_queue_after_backfill(self) -> None:
        self.assertIs(
            _backlog_queue(fallback=False).knows_every_upload, True,
        )

    def test_backlog_queue_during_migration(self) -> None:
        self.assertIs(
            _backlog_queue(fallback=True).knows_every_upload, False,
        )

    def test_backlog_queue_without_upload_state(self) -> None:
        queue: BacklogServerVideoScrapeQueue = _backlog_queue(
            fallback=False, supports_uploaded=False,
        )
        self.assertIs(queue.knows_every_upload, False)

    def test_redis_queue_never(self) -> None:
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            MagicMock(),
            VideoScrapeQueueSettings(),
            supports_uploaded=True,
        )
        self.assertIs(queue.knows_every_upload, False)


def _video(video_id: str) -> YouTubeVideo:
    video: YouTubeVideo = YouTubeVideo(video_id=video_id)
    video.channel_id = 'UCX'
    return video


async def _process(
    video_queue: MagicMock, client: MagicMock,
) -> bool:
    videos: list[YouTubeVideo] = [
        _video('uploaded-one'),
        _video('queued-one'),
        _video('unknown-one'),
    ]
    with tempfile.TemporaryDirectory() as data_dir:
        settings: MagicMock = MagicMock()
        settings.redis_dsn = 'redis://fake'
        settings.exchange_url = 'https://scrape.exchange'
        settings.video_data_directory = data_dir
        settings.channel_min_subscribers = 0

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
            new=AsyncMock(return_value=(True, 0, 'handle', False)),
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
            )


def _video_queue(
    *, knows_every_upload: bool, precheck_fails: bool = False,
) -> MagicMock:
    video_queue: MagicMock = MagicMock()
    video_queue.knows_every_upload = knows_every_upload
    states: dict[str, Any] = {
        'uploaded-one': None,
        'queued-one': 'queued',
        'unknown-one': None,
    }
    if precheck_fails:
        video_queue.get_states = AsyncMock(
            side_effect=ConnectionError('backlog server down'),
        )
    else:
        video_queue.get_states = AsyncMock(return_value=states)
    video_queue.done_many = AsyncMock(return_value={
        'uploaded-one': True,
        'queued-one': False,
        'unknown-one': False,
    })
    video_queue.mark_uploaded = AsyncMock()
    video_queue.enqueue = AsyncMock()
    return video_queue


def _enqueued(video_queue: MagicMock) -> list[str]:
    return [
        call.args[0] for call in video_queue.enqueue.await_args_list
    ]


class TestRssSkipsExistenceGet(unittest.IsolatedAsyncioTestCase):

    async def test_unknown_video_enqueued_without_get(self) -> None:
        client: MagicMock = MagicMock()
        client.get = AsyncMock(return_value=Response(404))
        video_queue: MagicMock = _video_queue(knows_every_upload=True)

        result: bool = await _process(video_queue, client)

        self.assertIs(result, True)
        client.get.assert_not_awaited()
        self.assertEqual(
            sorted(_enqueued(video_queue)),
            ['queued-one', 'unknown-one'],
        )
        video_queue.mark_uploaded.assert_not_awaited()

    async def test_migration_keeps_the_get(self) -> None:
        client: MagicMock = MagicMock()
        client.get = AsyncMock(return_value=Response(404))
        video_queue: MagicMock = _video_queue(knows_every_upload=False)

        await _process(video_queue, client)

        client.get.assert_awaited_once()
        self.assertIn('unknown-one', client.get.await_args.args[0])
        self.assertEqual(
            sorted(_enqueued(video_queue)),
            ['queued-one', 'unknown-one'],
        )

    async def test_failed_lookup_keeps_the_get(self) -> None:
        # Without an answer from the backlog server, "never seen"
        # cannot be told apart from "uploaded": ask scrape.exchange.
        client: MagicMock = MagicMock()
        client.get = AsyncMock(return_value=Response(404))
        video_queue: MagicMock = _video_queue(
            knows_every_upload=True, precheck_fails=True,
        )

        await _process(video_queue, client)

        self.assertEqual(client.get.await_count, 3)


if __name__ == '__main__':
    unittest.main()
