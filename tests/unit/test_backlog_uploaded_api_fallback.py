'''Unit tests for the backlog queue's migration API fallback.'''

import asyncio
import unittest
from typing import Any
from unittest import mock

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.scraper_metrics import (
    METRIC_BACKLOG_UPLOADED_API_FALLBACK,
)
from scrape_exchange.video_scrape_queue import VideoScrapeQueueSettings
from tests.unit.test_backlog_server_queue import QUEUE, FakeRedis

IDS: list[str] = ['a' * 11, 'b' * 11, 'c' * 11]
# B008: a sentinel stands in for "some client is set" in the defaults.
CLIENT: Any = object()
HELPER: str = 'scrape_exchange.backlog_server_queue.exchange_video_exists'
LOGGER: str = 'scrape_exchange.backlog_server_queue'


def settings(**kwargs: Any) -> VideoScrapeQueueSettings:
    fields: dict[str, Any] = {
        'backlog_uploaded_api_fallback': True,
        'exchange_existence_concurrency': 64,
    }
    fields.update(kwargs)
    return VideoScrapeQueueSettings(**fields)  # type: ignore[arg-type]


def make(
    replies: list[Any] | None = None,
    *,
    fallback: bool = True,
    client: Any = CLIENT,
    concurrency: int = 64,
) -> tuple[BacklogServerVideoScrapeQueue, FakeRedis]:
    fake: FakeRedis = FakeRedis(replies)
    queue: BacklogServerVideoScrapeQueue = (
        BacklogServerVideoScrapeQueue(
            fake,  # type: ignore[arg-type]
            settings(
                backlog_uploaded_api_fallback=fallback,
                exchange_existence_concurrency=concurrency,
            ),
            supports_uploaded=True,
            exchange_client=client,
            exchange_url='http://localhost:6390',
        )
    )
    return queue, fake


def counter(outcome: str) -> float:
    return METRIC_BACKLOG_UPLOADED_API_FALLBACK.labels(
        outcome=outcome,
    )._value.get()


class TestBacklogUploadedApiFallback(unittest.IsolatedAsyncioTestCase):

    async def test_done_many_asks_api_only_for_unknown_ids(self) -> None:
        queue, fake = make([['scraped', None, 'queued']])
        asked: list[str] = []

        async def helper(client: Any, url: str, video_id: str) -> bool:
            asked.append(video_id)
            return video_id == IDS[1]

        with mock.patch(HELPER, side_effect=helper):
            answers: dict[str, bool] = await queue.done_many(IDS)
        self.assertEqual(
            answers, {IDS[0]: True, IDS[1]: True, IDS[2]: False},
        )
        self.assertEqual(asked, [IDS[1]])
        # A confirmed ID is marked uploaded, so it costs one lookup.
        self.assertIn(('BACKLOG.UPLOADED', QUEUE, IDS[1]), fake.calls)

    async def test_uploaded_many_asks_api_for_every_non_uploaded_id(
        self,
    ) -> None:
        queue, fake = make([['uploaded', 'scraped']])
        asked: list[str] = []

        async def helper(client: Any, url: str, video_id: str) -> bool:
            asked.append(video_id)
            return True

        with mock.patch(HELPER, side_effect=helper):
            answers: dict[str, bool] = await queue.uploaded_many(
                IDS[:2],
            )
        self.assertEqual(answers, {IDS[0]: True, IDS[1]: True})
        self.assertEqual(asked, [IDS[1]])
        self.assertIn(('BACKLOG.UPLOADED', QUEUE, IDS[1]), fake.calls)

    async def test_api_404_and_error_answer_false(self) -> None:
        queue, fake = make([['queued', 'queued']])
        not_uploaded: float = counter('not_uploaded')
        errors: float = counter('error')

        async def helper(client: Any, url: str, video_id: str) -> bool:
            if video_id == IDS[0]:
                return False
            raise RuntimeError('boom')

        with mock.patch(HELPER, side_effect=helper):
            answers: dict[str, bool] = await queue.uploaded_many(
                IDS[:2],
            )
        self.assertEqual(answers, {IDS[0]: False, IDS[1]: False})
        self.assertEqual(counter('not_uploaded') - not_uploaded, 1)
        self.assertEqual(counter('error') - errors, 1)
        self.assertFalse(
            [c for c in fake.calls if c[0] == 'BACKLOG.UPLOADED'],
        )

    async def test_concurrency_is_bounded(self) -> None:
        ids: list[str] = [f'v{i:010d}' for i in range(200)]
        queue, _ = make([list([None] * len(ids))], concurrency=8)
        in_flight: int = 0
        peak: int = 0

        async def helper(client: Any, url: str, video_id: str) -> bool:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            in_flight -= 1
            return False

        with mock.patch(HELPER, side_effect=helper):
            await queue.done_many(ids)
        self.assertLessEqual(peak, 8)
        self.assertGreater(peak, 1)

    async def test_no_api_calls_when_setting_off(self) -> None:
        queue, _ = make([['queued']], fallback=False)
        with mock.patch(HELPER) as patched:
            answers: dict[str, bool] = await queue.uploaded_many(
                IDS[:1],
            )
        self.assertEqual(answers, {IDS[0]: False})
        patched.assert_not_called()

    async def test_no_api_calls_with_api_fallback_false(self) -> None:
        queue, _ = make([['queued']])
        with mock.patch(HELPER) as patched:
            answers: dict[str, bool] = await queue.uploaded_many(
                IDS[:1], api_fallback=False,
            )
        self.assertEqual(answers, {IDS[0]: False})
        patched.assert_not_called()

    async def test_no_client_skips_fallback_and_logs_once(self) -> None:
        queue, _ = make([['queued'], ['queued']], client=None)
        with (
            mock.patch(HELPER) as patched,
            self.assertLogs(LOGGER, level='WARNING') as logs,
        ):
            await queue.uploaded_many(IDS[:1])
            await queue.uploaded_many(IDS[:1])
        patched.assert_not_called()
        self.assertEqual(len(logs.records), 1)


if __name__ == '__main__':
    unittest.main()
