'''Unit tests for scrape_exchange.video_queue_factory.'''

import asyncio
import unittest
from typing import Any
from unittest import mock

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.video_queue_factory import make_video_scrape_queue
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
)

DSN: str = 'redis://:pw@localhost:6390/0'


def settings(dsn: str | None) -> VideoScrapeQueueSettings:
    return VideoScrapeQueueSettings(backlog_server_dsn=dsn)


class TestVideoQueueFactory(unittest.TestCase):

    def setUp(self) -> None:
        # The backlog client cache is process-wide; each test starts
        # with an empty one so a client built by an earlier test cannot
        # satisfy (or leak into) this one.
        no_loop: Any = mock.patch(
            'scrape_exchange.video_queue_factory._NO_LOOP', {},
        )
        no_loop.start()
        self.addCleanup(no_loop.stop)
        loose: Any = mock.patch(
            'scrape_exchange.video_queue_factory._BY_LOOP', {},
        )
        loose.start()
        self.addCleanup(loose.stop)

    def test_youtube_with_dsn_uses_backlog_server(self) -> None:
        sentinel: object = object()
        with mock.patch(
            'scrape_exchange.video_queue_factory.backlog_redis_from_url',
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

    def test_backlog_client_is_built_by_the_resilient_factory(
        self,
    ) -> None:
        '''The backlog client must come from backlog_redis_from_url,
        which adds the command retry, timeouts and health check the
        plain Redis client deliberately does not have.'''
        with mock.patch(
            'scrape_exchange.redis_client.redis_from_url',
        ) as inner:
            inner.return_value = mock.Mock()
            make_video_scrape_queue(
                platform='youtube', redis=mock.Mock(),
                settings=settings(DSN), component='yt-video-queue',
            )
        kwargs: Any = inner.call_args.kwargs
        self.assertIn('retry', kwargs)
        self.assertGreater(kwargs['retry'].get_retries(), 0)
        self.assertIsNotNone(kwargs['socket_connect_timeout'])
        self.assertIsNotNone(kwargs['socket_timeout'])
        self.assertGreater(kwargs['health_check_interval'], 0)

    def test_backlog_server_client_is_shared(self) -> None:
        with mock.patch(
            'scrape_exchange.video_queue_factory.backlog_redis_from_url',
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

    def test_backlog_client_is_not_shared_across_event_loops(
        self,
    ) -> None:
        '''A client is bound to the loop it was built on; reusing it on
        another loop can never establish a connection.'''
        created: list[Any] = []

        async def build() -> Any:
            q: Any = make_video_scrape_queue(
                platform='youtube', redis=mock.Mock(),
                settings=settings(DSN), component='a',
            )
            created.append(q._redis)
            return q

        async def twice_in_one_loop() -> tuple[Any, Any]:
            return await build(), await build()

        with mock.patch(
            'scrape_exchange.video_queue_factory.backlog_redis_from_url',
            side_effect=lambda *a, **k: object(),
        ) as patched:
            # Two separate asyncio.run calls: one loop each.
            asyncio.run(build())
            asyncio.run(build())
            same_a, same_b = asyncio.run(twice_in_one_loop())

        # Three loops, three clients: none is reused across loops.
        self.assertEqual(patched.call_count, 3)
        self.assertIsNot(created[0], created[1])
        self.assertIsNot(created[0], created[2])
        self.assertIsNot(created[1], created[2])
        # Two builds inside ONE loop do share.
        self.assertIs(same_a._redis, same_b._redis)

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
            'scrape_exchange.video_queue_factory.backlog_redis_from_url',
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
