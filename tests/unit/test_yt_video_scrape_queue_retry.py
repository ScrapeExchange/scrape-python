'''Tests for the video producer's resilience to a failing queue.

``_pop_entries_resilient`` is what keeps a backlog-server (or Redis)
connection failure from killing the producer task and, with it, every
consumer in the worker.
'''

import asyncio
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from scrape_exchange.scraper_metrics import (
    METRIC_QUEUE_CONNECTION_ERRORS,
)
from scrape_exchange.video_scrape_queue import (
    VideoQueueChannelContext,
    VideoScrapeQueueEntry,
)
from tools.yt_video_scrape import (
    VideoSettings,
    _pop_entries_resilient,
    _queue_driven_loop,
)


def _settings(
    base: float = 5.0,
    max_seconds: float = 60.0,
    batch: int = 10,
) -> MagicMock:
    s: MagicMock = MagicMock()
    s.video_queue_batch = batch
    s.video_queue_error_backoff_seconds = base
    s.video_queue_error_backoff_max_seconds = max_seconds
    s.video_queue_idle_poll_seconds = 0.0
    return s


def _entry(video_id: str) -> VideoScrapeQueueEntry:
    return VideoScrapeQueueEntry(
        video_id=video_id,
        channel=VideoQueueChannelContext(),
        source=None,
        meta={},
    )


class TestPopEntriesResilient(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_defaults_are_bounded(self) -> None:
        fields: dict = VideoSettings.model_fields
        self.assertEqual(
            fields['video_queue_error_backoff_seconds'].default, 5.0,
        )
        self.assertEqual(
            fields[
                'video_queue_error_backoff_max_seconds'
            ].default,
            60.0,
        )

    async def test_retries_connection_error_then_returns_entries(
        self,
    ) -> None:
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = [
            RedisConnectionError('connection refused'),
            [_entry('dQw4w9WgXcQ')],
        ]
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with (
            patch(
                'tools.yt_video_scrape.paced_sleep',
                side_effect=fake_sleep,
            ),
            self.assertLogs(level='WARNING') as logs,
        ):
            entries: list[VideoScrapeQueueEntry] = (
                await _pop_entries_resilient(queue, _settings())
            )

        self.assertEqual([e.video_id for e in entries], ['dQw4w9WgXcQ'])
        self.assertEqual(sleeps, [5.0])
        self.assertEqual(queue.pop_entries.await_count, 2)
        self.assertTrue(
            any(
                'Video queue unreachable' in r.getMessage()
                for r in logs.records
            ),
        )

    async def test_timeout_is_retried_too(self) -> None:
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = [
            RedisTimeoutError('timed out'),
            [],
        ]
        with patch(
            'tools.yt_video_scrape.paced_sleep', new_callable=AsyncMock,
        ):
            entries: list[VideoScrapeQueueEntry] = (
                await _pop_entries_resilient(queue, _settings())
            )
        self.assertEqual(entries, [])

    async def test_backoff_doubles_and_caps(self) -> None:
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = [
            RedisConnectionError('down') for _ in range(6)
        ] + [[]]
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with patch(
            'tools.yt_video_scrape.paced_sleep',
            side_effect=fake_sleep,
        ):
            await _pop_entries_resilient(
                queue, _settings(base=2.0, max_seconds=10.0),
            )
        self.assertEqual(sleeps, [2.0, 4.0, 8.0, 10.0, 10.0, 10.0])

    async def test_long_outage_does_not_overflow_the_backoff(
        self,
    ) -> None:
        '''float * 2**1024 overflows; a days-long outage must keep
        sleeping at the ceiling instead of crashing the producer.'''
        failures: int = 1100
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = [
            RedisConnectionError('down') for _ in range(failures)
        ] + [[]]
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with (
            patch(
                'tools.yt_video_scrape.paced_sleep',
                side_effect=fake_sleep,
            ),
            self.assertLogs(level='WARNING'),
        ):
            await _pop_entries_resilient(queue, _settings())
        self.assertEqual(len(sleeps), failures)
        self.assertEqual(sleeps[-1], 60.0)

    async def test_drained_queue_is_not_retried(self) -> None:
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.return_value = []
        with patch(
            'tools.yt_video_scrape.paced_sleep', new_callable=AsyncMock,
        ) as sleep:
            entries: list[VideoScrapeQueueEntry] = (
                await _pop_entries_resilient(queue, _settings())
            )
        self.assertEqual(entries, [])
        self.assertEqual(queue.pop_entries.await_count, 1)
        sleep.assert_not_awaited()

    async def test_non_connection_error_propagates(self) -> None:
        '''A protocol error is not transient: it must fail the worker
        rather than park it.'''
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = ResponseError('unknown command')
        with (
            patch(
                'tools.yt_video_scrape.paced_sleep',
                new_callable=AsyncMock,
            ) as sleep,
            self.assertRaises(ResponseError),
        ):
            await _pop_entries_resilient(queue, _settings())
        sleep.assert_not_awaited()

    async def test_cancellation_propagates(self) -> None:
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = asyncio.CancelledError()
        with (
            patch(
                'tools.yt_video_scrape.paced_sleep',
                new_callable=AsyncMock,
            ) as sleep,
            self.assertRaises(asyncio.CancelledError),
        ):
            await _pop_entries_resilient(queue, _settings())
        sleep.assert_not_awaited()


class TestProducerSurvivesQueueOutage(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_queue_driven_loop_keeps_running(self) -> None:
        '''One connection failure used to end the producer task and so
        the whole worker; the loop must outlive it.'''
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = (
            # First pop fails outright, every later pop is empty.
            [RedisConnectionError('connection reset by peer')]
            + [[] for _ in range(1000)]
        )
        settings: MagicMock = _settings()
        settings.video_queue_idle_poll_seconds = 0.001
        task: asyncio.Task[None] = asyncio.create_task(
            _queue_driven_loop(
                queue,
                settings,
                proxies=[],
                concurrency=1,
                uploaded=AsyncMock(),
                creator_map_backend=MagicMock(),
            ),
        )
        with patch(
            'tools.yt_video_scrape.paced_sleep', new_callable=AsyncMock,
        ) as sleep:
            await asyncio.sleep(0.05)
            self.assertFalse(
                task.done(),
                'the queue-driven loop must outlive the connection '
                'failure instead of the producer task dying',
            )
        sleep.assert_awaited()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_consumer_queue_outage_is_counted_not_a_bug(
        self,
    ) -> None:
        '''A complete/mark that fails with a connection error is a queue
        outage: it is counted and logged as such, the video is left to
        its lease, and the consumer keeps running.'''
        queue: AsyncMock = AsyncMock()
        queue.pop_entries.side_effect = (
            [[_entry('dQw4w9WgXcQ')]] + [[] for _ in range(1000)]
        )
        settings: MagicMock = _settings()
        settings.video_queue_idle_poll_seconds = 0.001
        counter: Any = METRIC_QUEUE_CONNECTION_ERRORS.labels(
            platform='youtube', scraper='video_scraper',
            operation='update',
        )
        before: float = counter._value.get()
        with (
            patch(
                'tools.yt_video_scrape._scrape_one_queued',
                side_effect=RedisConnectionError('reset by peer'),
            ),
            self.assertLogs(level='WARNING') as logs,
        ):
            task: asyncio.Task[None] = asyncio.create_task(
                _queue_driven_loop(
                    queue,
                    settings,
                    proxies=[],
                    concurrency=1,
                    uploaded=AsyncMock(),
                    creator_map_backend=MagicMock(),
                ),
            )
            await asyncio.sleep(0.05)
            self.assertFalse(task.done())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(counter._value.get(), before + 1)
        messages: list[str] = [r.getMessage() for r in logs.records]
        self.assertTrue(
            any('leaving the video to its lease' in m for m in messages),
        )
        self.assertFalse(
            any('Unexpected error' in m for m in messages),
        )


if __name__ == '__main__':
    unittest.main()
