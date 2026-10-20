'''Unit tests for scrape_exchange.video_queue_factory.'''

import unittest
from typing import Any
from unittest import mock

from scrape_exchange.backlog_server_queue import BacklogServerVideoScrapeQueue
from scrape_exchange.video_queue_factory import make_video_scrape_queue
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
)

DSN: str = 'redis://:pw@localhost:6390/0'


def settings(dsn: str | None) -> VideoScrapeQueueSettings:
    return VideoScrapeQueueSettings(
        backlog_server_dsn=dsn, mongo_dsn=None,
    )


class TestVideoQueueFactory(unittest.TestCase):

    def test_youtube_with_dsn_uses_backlog_server(self) -> None:
        sentinel: object = object()
        with mock.patch(
            'scrape_exchange.video_queue_factory.redis_from_url',
            return_value=sentinel,
        ) as patched:
            q: VideoScrapeQueue = make_video_scrape_queue(
                platform='youtube', redis=mock.Mock(),
                settings=settings(DSN), component='yt-video-queue',
            )
        self.assertIsInstance(q, BacklogServerVideoScrapeQueue)
        args: Any = patched.call_args
        self.assertEqual(args.args, (DSN,))
        self.assertEqual(args.kwargs['component'], 'yt-video-queue')
        self.assertTrue(args.kwargs['decode_responses'])
        self.assertIs(q._redis, sentinel)

    def test_backlog_server_client_is_shared(self) -> None:
        with mock.patch(
            'scrape_exchange.video_queue_factory.redis_from_url',
            side_effect=lambda *a, **k: object(),
        ) as patched:
            first: Any = make_video_scrape_queue(
                platform='youtube', redis=mock.Mock(),
                settings=settings('redis://localhost:6399/0'),
                component='a',
            )
            second: Any = make_video_scrape_queue(
                platform='youtube', redis=mock.Mock(),
                settings=settings('redis://localhost:6399/0'),
                component='b',
            )
        self.assertIs(first._redis, second._redis)
        self.assertEqual(patched.call_count, 1)

    def test_youtube_without_dsn_uses_redis(self) -> None:
        redis: mock.Mock = mock.Mock()
        q: VideoScrapeQueue = make_video_scrape_queue(
            platform='youtube', redis=redis, settings=settings(None),
            component='x',
        )
        self.assertIsInstance(q, RedisVideoScrapeQueue)
        self.assertIs(q._redis, redis)

    def test_tiktok_ignores_dsn(self) -> None:
        with mock.patch(
            'scrape_exchange.video_queue_factory.redis_from_url',
        ) as patched:
            q: VideoScrapeQueue = make_video_scrape_queue(
                platform='tiktok', redis=mock.Mock(),
                settings=settings(DSN), component='x',
            )
        self.assertIsInstance(q, RedisVideoScrapeQueue)
        self.assertEqual(q._key_prefix, 'tiktok:video')
        patched.assert_not_called()


if __name__ == '__main__':
    unittest.main()
