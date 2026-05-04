'''Apply-bulk-results writes successful channel handles into
RedisExchangeChannelsSet.'''

import unittest

import fakeredis.aioredis

from scrape_exchange.bulk_upload import apply_bulk_results
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)


class _StubFM:
    def __init__(self) -> None:
        self.uploaded: list[str] = []

    async def mark_uploaded(self, filename: str) -> None:
        self.uploaded.append(filename)


def _strip_channel_filename(filename: str) -> str:
    return filename.removeprefix(
        'channel-',
    ).removesuffix('.json.br')


class TestApplyBulkResultsExchangeSet(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.s: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(self.redis)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_success_records_added_to_set(
        self,
    ) -> None:
        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
            ('UC2', 'channel-bravo.json.br'),
        ]
        results: list[dict] = [
            {
                'platform_content_id': 'UC1',
                'status': 'success',
            },
            {
                'platform_content_id': 'UC2',
                'status': 'failed',
            },
        ]
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
            exchange_set=self.s,
            handle_from_filename=_strip_channel_filename,
        )
        present: dict[str, bool] = (
            await self.s.contains_many(['alpha', 'bravo'])
        )
        self.assertEqual(
            present, {'alpha': True, 'bravo': False},
        )

    async def test_no_exchange_set_does_not_touch_redis(
        self,
    ) -> None:
        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
        ]
        results: list[dict] = [
            {
                'platform_content_id': 'UC1',
                'status': 'success',
            },
        ]
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
        )
        self.assertEqual(await self.s.size(), 0)

    async def test_handle_from_filename_skipped_without_set(
        self,
    ) -> None:
        '''If exchange_set is None, handle_from_filename is
        ignored even if provided.'''
        called: list[str] = []

        def _capture(filename: str) -> str:
            called.append(filename)
            return _strip_channel_filename(filename)

        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
        ]
        results: list[dict] = [
            {
                'platform_content_id': 'UC1',
                'status': 'success',
            },
        ]
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
            exchange_set=None,
            handle_from_filename=_capture,
        )
        self.assertEqual(called, [])
