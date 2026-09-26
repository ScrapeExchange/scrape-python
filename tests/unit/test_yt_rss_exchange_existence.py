'''
Tests for RSS scrape.exchange existence-check backpressure.
'''

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from httpx2 import Response

from scrape_exchange.youtube._rss_circuit_state import CircuitReport

from tools import yt_rss_scrape


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestRssExchangeExistenceConcurrency(unittest.TestCase):

    def tearDown(self) -> None:
        os.environ.pop(
            'RSS_EXCHANGE_EXISTENCE_CONCURRENCY', None,
        )

    def test_setting_defaults_to_64(self) -> None:
        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )

        self.assertEqual(
            settings.rss_exchange_existence_concurrency,
            64,
        )

    def test_setting_reads_env_override(self) -> None:
        os.environ[
            'RSS_EXCHANGE_EXISTENCE_CONCURRENCY'
        ] = '32'

        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )

        self.assertEqual(
            settings.rss_exchange_existence_concurrency,
            32,
        )

    def test_gate_wait_happens_before_client_get(self) -> None:
        async def scenario() -> None:
            gate = asyncio.Semaphore(1)
            await gate.acquire()

            client = MagicMock()
            client.get = AsyncMock(
                return_value=Response(404),
            )
            settings = MagicMock()
            settings.exchange_url = 'https://scrape.exchange'

            task = asyncio.create_task(
                yt_rss_scrape.check_video_exists(
                    client, settings, 'video-id',
                    gate=gate,
                )
            )
            await asyncio.sleep(0)
            client.get.assert_not_called()

            gate.release()
            result = await task

            self.assertIs(result, False)
            client.get.assert_awaited_once()

        _run(scenario())


class TestRssRedisFirstExistence(unittest.TestCase):
    '''Redis-first pre-check: videos the fleet already uploaded or
    already tracks in the scrape queue must skip the scrape.exchange
    existence GET; only unknown videos hit the API.'''

    def test_uploaded_and_tracked_videos_skip_the_get(self) -> None:
        async def scenario() -> None:
            import tempfile
            from pathlib import Path

            from scrape_exchange.youtube.youtube_video import (
                YouTubeVideo,
            )

            def _video(video_id: str) -> YouTubeVideo:
                video: YouTubeVideo = YouTubeVideo(video_id=video_id)
                video.channel_id = 'UCX'
                return video

            videos: list[YouTubeVideo] = [
                _video('uploaded-one'),
                _video('queued-one'),
                _video('unknown-one'),
            ]

            with tempfile.TemporaryDirectory() as data_dir:
                settings = MagicMock()
                settings.redis_dsn = 'redis://fake'
                settings.exchange_url = 'https://scrape.exchange'
                settings.video_data_directory = data_dir

                client = MagicMock()
                client.get = AsyncMock(return_value=Response(404))

                video_queue = MagicMock()
                video_queue.get_states = AsyncMock(return_value={
                    'uploaded-one': None,
                    'queued-one': 'queued',
                    'unknown-one': None,
                })
                video_queue.enqueue = AsyncMock()

                uploaded_backend = MagicMock()
                uploaded_backend.contains_many = AsyncMock(
                    return_value={
                        'uploaded-one': True,
                        'queued-one': False,
                        'unknown-one': False,
                    },
                )

                creator_queue = MagicMock()
                creator_queue.has_had_feed = AsyncMock(
                    return_value=True,
                )
                creator_queue.get_no_feeds = AsyncMock(
                    return_value=None,
                )
                creator_queue.update_tier = AsyncMock()
                creator_queue.set_no_feeds = AsyncMock()
                creator_queue.rollback_no_feeds = AsyncMock()
                creator_queue.mark_had_feed = AsyncMock()
                creator_queue.clear_no_feeds = AsyncMock()

                breaker = MagicMock()
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
                ), patch.object(
                    yt_rss_scrape, 'UploadedVideoIds',
                    return_value=uploaded_backend,
                ):
                    result = await yt_rss_scrape.process_channel(
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

                self.assertIs(result, True)
                # Only the unknown video reaches the exchange.
                client.get.assert_awaited_once()
                requested: str = (
                    client.get.await_args_list[0].args[0]
                )
                self.assertIn('unknown-one', requested)
                # The unknown and queue-tracked videos are both
                # enqueued (the tracked one dedupes inside the
                # queue); the uploaded one is not.
                self.assertEqual(video_queue.enqueue.await_count, 2)
                enqueued: list[str] = [
                    call.args[0]
                    for call in video_queue.enqueue.await_args_list
                ]
                self.assertIn('unknown-one', enqueued)
                self.assertIn('queued-one', enqueued)
                self.assertNotIn('uploaded-one', enqueued)

        _run(scenario())
