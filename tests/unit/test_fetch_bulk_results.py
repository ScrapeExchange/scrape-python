'''fetch_bulk_results parses counts + failures, or returns None.'''

import unittest

from scrape_exchange.bulk_upload import BulkResults, fetch_bulk_results


class _StubResp:
    def __init__(
        self, status_code: int, payload: dict, text: str = '',
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict:
        return self._payload


class _StubClient:
    def __init__(self, resp: _StubResp) -> None:
        self._resp = resp
        self.headers: dict[str, str] = {}

    async def get(self, url: str) -> _StubResp:
        return self._resp


class TestFetchBulkResults(unittest.IsolatedAsyncioTestCase):

    async def test_parses_counts_and_failures(self) -> None:
        resp = _StubResp(200, {
            'job_id': 'j',
            'total': 3, 'succeeded': 2, 'failed': 1, 'duplicate': 0,
            'results': [{'platform_content_id': 'UC2',
                         'status': 'failed'}],
        })
        out = await fetch_bulk_results('j', 'http://x', _StubClient(resp))
        self.assertIsInstance(out, BulkResults)
        self.assertEqual(out.total, 3)
        self.assertEqual(out.succeeded, 2)
        self.assertEqual(out.failed, 1)
        self.assertEqual(out.duplicate, 0)
        self.assertEqual(len(out.failures), 1)

    async def test_missing_total_returns_none(self) -> None:
        resp = _StubResp(200, {
            'job_id': 'j', 'succeeded': 2, 'failed': 1,
            'duplicate': 0, 'results': [],
        })
        out = await fetch_bulk_results('j', 'http://x', _StubClient(resp))
        self.assertIsNone(out)

    async def test_partial_counts_returns_none(self) -> None:
        # 'duplicate' absent -> server not fully upgraded.
        resp = _StubResp(200, {
            'job_id': 'j', 'total': 3, 'succeeded': 2, 'failed': 1,
            'results': [],
        })
        out = await fetch_bulk_results('j', 'http://x', _StubClient(resp))
        self.assertIsNone(out)

    async def test_non_200_returns_none(self) -> None:
        resp = _StubResp(500, {}, text='upstream boom')
        out = await fetch_bulk_results('j', 'http://x', _StubClient(resp))
        self.assertIsNone(out)
