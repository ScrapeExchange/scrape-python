'''fetch_bulk_results parses counts + failures, or returns None;
fetch_settled_bulk_results re-fetches until the counts settle.'''

import unittest

from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.bulk_upload import (
    BulkResults,
    fetch_bulk_results,
    fetch_settled_bulk_results,
    finalize_bulk_batch,
)


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


class _SeqClient:
    '''Returns the queued responses in order; repeats the last one.'''

    def __init__(self, resps: list[_StubResp]) -> None:
        self._resps: list[_StubResp] = resps
        self.calls: int = 0
        self.headers: dict[str, str] = {}

    async def get(self, url: str) -> _StubResp:
        resp: _StubResp = self._resps[min(self.calls, len(self._resps) - 1)]
        self.calls += 1
        return resp


def _counts(
    total: int, failed: int = 0, failures: list[dict] | None = None,
) -> _StubResp:
    return _StubResp(200, {
        'job_id': 'j', 'total': total, 'succeeded': total - failed,
        'failed': failed, 'duplicate': 0, 'results': failures or [],
    })


_NO_WAIT: tuple[float, ...] = (0.0, 0.0, 0.0, 0.0)


class TestFetchSettledBulkResults(unittest.IsolatedAsyncioTestCase):

    async def test_settled_first_fetch_does_not_refetch(self) -> None:
        client = _SeqClient([_counts(3)])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=3, delays=_NO_WAIT,
        )
        self.assertEqual(out.total, 3)
        self.assertEqual(client.calls, 1)

    async def test_zero_total_is_refetched_until_counts_land(self) -> None:
        # Terminal status published before counts were written.
        client = _SeqClient([_counts(0), _counts(0), _counts(1000)])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=1000,
            delays=_NO_WAIT,
        )
        self.assertEqual(out.total, 1000)
        self.assertEqual(client.calls, 3)

    async def test_incomplete_failures_list_is_refetched(self) -> None:
        failure: dict = {'platform_content_id': 'v1', 'status': 'failed'}
        client = _SeqClient([
            _counts(2, failed=1, failures=[]),
            _counts(2, failed=1, failures=[failure]),
        ])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=2, delays=_NO_WAIT,
        )
        self.assertEqual(len(out.failures), 1)
        self.assertEqual(client.calls, 2)

    async def test_fetch_error_is_retried(self) -> None:
        client = _SeqClient([_StubResp(500, {}), _counts(2)])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=2, delays=_NO_WAIT,
        )
        self.assertEqual(out.total, 2)

    async def test_gives_up_and_returns_last_result(self) -> None:
        client = _SeqClient([_counts(0)])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=5, delays=_NO_WAIT,
        )
        self.assertEqual(out.total, 0)
        self.assertEqual(client.calls, len(_NO_WAIT) + 1)

    async def test_total_above_expected_is_final(self) -> None:
        client = _SeqClient([_counts(7)])
        out = await fetch_settled_bulk_results(
            'j', 'http://x', client, expected_total=5, delays=_NO_WAIT,
        )
        self.assertEqual(out.total, 7)
        self.assertEqual(client.calls, 1)

    async def test_default_delays_back_off(self) -> None:
        client = _SeqClient([_counts(0)])
        with patch(
            'scrape_exchange.bulk_upload.asyncio.sleep',
            new_callable=AsyncMock,
        ) as sleep:
            await fetch_settled_bulk_results(
                'j', 'http://x', client, expected_total=1,
            )
        self.assertEqual(
            [c.args[0] for c in sleep.await_args_list],
            [0.5, 1.0, 2.0, 4.0],
        )


class TestFinalizeUsesSettledResults(unittest.IsolatedAsyncioTestCase):

    async def test_finalize_reconciles_after_counts_land(self) -> None:
        records: list[tuple[str, str]] = [
            ('v1', 'video-v1.json.br'), ('v2', 'video-v2.json.br'),
        ]
        client = _SeqClient([_counts(0), _counts(2)])
        apply = AsyncMock(return_value=(2, 0, 0, {'v1', 'v2'}))
        with patch(
            'scrape_exchange.bulk_upload.stream_bulk_job_progress',
            new=AsyncMock(return_value=True),
        ), patch(
            'scrape_exchange.bulk_upload.apply_bulk_results', new=apply,
        ), patch(
            'scrape_exchange.bulk_upload.delete_bulk_state',
            new=AsyncMock(),
        ), patch(
            'scrape_exchange.bulk_upload.asyncio.sleep',
            new_callable=AsyncMock,
        ):
            outcome = await finalize_bulk_batch(
                'j', 'b', records, exchange_url='http://x',
                client=client, fm=MagicMock(),
                progress_timeout_seconds=1.0,
            )
        applied: BulkResults = apply.await_args.args[1]
        self.assertEqual(applied.total, 2)
        self.assertEqual(outcome.success, 2)
        self.assertEqual(outcome.missing, 0)
