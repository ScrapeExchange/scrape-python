'''Apply-bulk-results writes successful channel_ids into
RedisExchangeChannelsSet.'''

import unittest

import fakeredis.aioredis

from pathlib import Path

from scrape_exchange.bulk_upload import apply_bulk_results, BulkResults
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)


class _StubFM:
    def __init__(self) -> None:
        self.uploaded: list[str] = []
        self.rejected: list[str] = []

    async def mark_uploaded(self, filename: str) -> None:
        self.uploaded.append(filename)

    async def mark_rejected(
        self, filename: str, reason: str, error_class: str,
        job_id: str,
    ) -> Path:
        self.rejected.append(filename)
        return Path('/tmp/rejected') / filename


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

    async def test_rejected_record_is_not_added_to_set(self) -> None:
        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
            ('UC2', 'channel-bravo.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=2, succeeded=1, failed=1, duplicate=0,
            failures=[{
                'platform_content_id': 'UC2',
                'status': 'failed',
                'reason': 'INVALID_PLATFORM_CREATOR_ID',
                'retryable': False,
                'error_class': 'INVALID_PLATFORM_CREATOR_ID',
            }],
        )
        fm: _StubFM = _StubFM()

        success, failed, _missing, ids = await apply_bulk_results(
            batch_records, results, fm,
            batch_id='b', job_id='j',
            exchange_set=self.s,
            id_from_filename=_strip_channel_filename,
            platform='youtube', entity='channel',
        )

        present: dict[str, bool] = (
            await self.s.contains_many(['alpha', 'bravo'])
        )
        self.assertEqual(present, {'alpha': True, 'bravo': False})
        self.assertEqual(fm.rejected, ['channel-bravo.json.br'])
        self.assertEqual(fm.uploaded, ['channel-alpha.json.br'])
        self.assertNotIn('UC2', ids)
        self.assertEqual((success, failed), (1, 1))

    async def test_success_records_added_to_set(
        self,
    ) -> None:
        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
            ('UC2', 'channel-bravo.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=2, succeeded=1, failed=1, duplicate=0,
            failures=[{
                'platform_content_id': 'UC2',
                'status': 'failed',
            }],
        )
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
            exchange_set=self.s,
            id_from_filename=_strip_channel_filename,
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
        results: BulkResults = BulkResults(
            total=1, succeeded=1, failed=0, duplicate=0, failures=[],
        )
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
        )
        self.assertEqual(await self.s.size(), 0)

    async def test_id_from_filename_skipped_without_set(
        self,
    ) -> None:
        '''If exchange_set is None, id_from_filename is
        ignored even if provided.'''
        called: list[str] = []

        def _capture(filename: str) -> str:
            called.append(filename)
            return _strip_channel_filename(filename)

        batch_records: list[tuple[str, str]] = [
            ('UC1', 'channel-alpha.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=1, succeeded=1, failed=0, duplicate=0, failures=[],
        )
        await apply_bulk_results(
            batch_records, results, _StubFM(),
            batch_id='b', job_id='j',
            exchange_set=None,
            id_from_filename=_capture,
        )
        self.assertEqual(called, [])

    async def test_exchange_set_gets_channel_ids(self) -> None:
        '''An id-keyed filename yields the channel_id in the set.'''
        cid: str = 'UCabcdefghijklmnopqrstuv'
        filename: str = f'channel-{cid}.json.br'
        await apply_bulk_results(
            batch_records=[(cid, filename)],
            results=BulkResults(
                total=1, succeeded=1, failed=0, duplicate=0,
                failures=[],
            ),
            fm=_StubFM(),
            batch_id='b', job_id='j',
            exchange_set=self.s,
            id_from_filename=_strip_channel_filename,
        )
        membership: dict[str, bool] = (
            await self.s.contains_many([cid])
        )
        self.assertTrue(membership[cid])
