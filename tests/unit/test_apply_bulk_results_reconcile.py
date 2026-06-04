'''apply_bulk_results reconciles a failures-only results list,
gating salvage on total == records submitted and on the failures
list being complete.'''

import unittest

from scrape_exchange.bulk_upload import BulkResults, apply_bulk_results


class _StubFM:
    def __init__(self) -> None:
        self.uploaded: list[str] = []

    async def mark_uploaded(self, filename: str) -> None:
        self.uploaded.append(filename)


def _records() -> list[tuple[str, str]]:
    return [
        ('UC1', 'channel-a.json.br'),
        ('UC2', 'channel-b.json.br'),
        ('UC3', 'channel-c.json.br'),
    ]


class TestApplyBulkResultsReconcile(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_total_match_marks_non_failed_uploaded(self) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=2, failed=1, duplicate=0,
            failures=[{'platform_content_id': 'UC2',
                       'status': 'failed'}],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (2, 1, 0))
        self.assertCountEqual(
            fm.uploaded, ['channel-a.json.br', 'channel-c.json.br'],
        )
        self.assertEqual(ids, {'UC1', 'UC3'})

    async def test_total_less_than_submitted_retries_all(self) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=2, succeeded=1, failed=1, duplicate=0,
            failures=[{'platform_content_id': 'UC2',
                       'status': 'failed'}],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])

    async def test_empty_failures_total_match_marks_all(self) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=3, failed=0, duplicate=0, failures=[],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (3, 0, 0))
        self.assertCountEqual(
            fm.uploaded,
            ['channel-a.json.br', 'channel-b.json.br',
             'channel-c.json.br'],
        )

    async def test_job_level_failure_total_zero_retries_all(
        self,
    ) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=0, succeeded=0, failed=0, duplicate=0, failures=[],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])

    async def test_duplicate_not_in_failures_marked_uploaded(
        self,
    ) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=2, failed=0, duplicate=1, failures=[],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (3, 0, 0))
        self.assertEqual(len(fm.uploaded), 3)

    async def test_unmatchable_failure_retries_all(self) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=2, failed=1, duplicate=0,
            failures=[{'platform_content_id': 'UCXX',
                       'status': 'failed'}],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])

    async def test_record_index_fallback_matches_failure(self) -> None:
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=2, failed=1, duplicate=0,
            failures=[{'record_index': 1, 'status': 'failed'}],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (2, 1, 0))
        self.assertCountEqual(
            fm.uploaded, ['channel-a.json.br', 'channel-c.json.br'],
        )

    async def test_none_results_retries_all(self) -> None:
        fm = _StubFM()
        success, failed, missing, ids = await apply_bulk_results(
            _records(), None, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])

    async def test_failed_count_without_failure_entries_retries_all(
        self,
    ) -> None:
        # Data-loss guard: server says failed=1 but the failures list
        # is empty (mirroring lag/drop). Must NOT salvage.
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=2, failed=1, duplicate=0, failures=[],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])

    async def test_inconsistent_counts_retries_all(self) -> None:
        # succeeded + failed + duplicate (1+1+0) != total (3).
        fm = _StubFM()
        res = BulkResults(
            total=3, succeeded=1, failed=1, duplicate=0,
            failures=[{'platform_content_id': 'UC2',
                       'status': 'failed'}],
        )
        success, failed, missing, ids = await apply_bulk_results(
            _records(), res, fm, 'b', 'j',
        )
        self.assertEqual((success, failed, missing), (0, 0, 3))
        self.assertEqual(fm.uploaded, [])
