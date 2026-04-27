'''
Unit tests for the shared bulk-upload helpers in
``scrape_exchange.bulk_upload``. The reconciliation logic decides
which source files get moved to ``uploaded_dir``; bugs here
either lose data (mark a failed record as uploaded and delete the
file) or wedge the pipeline (never mark anything). These tests
cover the lookup paths and edge cases without hitting the API.

Also covers ``bulk_progress_ws_url`` (pure URL translation) and
``stream_bulk_job_progress`` (terminal-status detection over a
mocked WebSocket) so the WebSocket-driven progress channel has
matching coverage.
'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import websockets.exceptions

from scrape_exchange.bulk_upload import (
    apply_bulk_results,
    bulk_progress_ws_url,
    stream_bulk_job_progress,
)


class TestApplyBulkResults(unittest.IsolatedAsyncioTestCase):

    async def test_success_by_content_id_marks_uploaded(self) -> None:
        '''A success result keyed by platform_content_id moves the
        matching source file to uploaded_dir.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ]
        results: list[dict] = [
            {'platform_content_id': 'UCabc', 'status': 'success'},
            {'platform_content_id': 'UCxyz', 'status': 'success'},
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        marked: list[str] = [
            call.args[0] for call in fm.mark_uploaded.call_args_list
        ]
        self.assertEqual(
            sorted(marked),
            ['channel-UCabc.json.br', 'channel-UCxyz.json.br'],
        )

    async def test_failed_records_left_for_retry(self) -> None:
        '''A failed result must not call mark_uploaded.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
        ]
        results: list[dict] = [
            {
                'platform_content_id': 'UCabc',
                'status': 'failed',
                'reason': 'VALIDATION_ERROR: foo',
            },
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        fm.mark_uploaded.assert_not_called()

    async def test_record_index_fallback(self) -> None:
        '''When a result has only record_index, the matching slot
        in batch_records is used to find the source file.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ]
        results: list[dict] = [
            {'record_index': 1, 'status': 'success'},
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        fm.mark_uploaded.assert_awaited_once_with(
            'channel-UCxyz.json.br',
        )

    async def test_missing_results_left_for_retry(self) -> None:
        '''Source files with no matching result entry are not
        marked uploaded — they stay in base_dir for the next run.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
            ('UCmissing', 'channel-UCmissing.json.br'),
        ]
        # Only two results back from the server; UCmissing absent.
        results: list[dict] = [
            {'platform_content_id': 'UCabc', 'status': 'success'},
            {'platform_content_id': 'UCxyz', 'status': 'success'},
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        marked: list[str] = [
            call.args[0] for call in fm.mark_uploaded.call_args_list
        ]
        self.assertNotIn('channel-UCmissing.json.br', marked)
        self.assertEqual(len(marked), 2)

    async def test_unmatchable_result_skipped(self) -> None:
        '''A result with no matching content_id and no record_index
        is logged and ignored (no file marked).'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
        ]
        results: list[dict] = [
            {'platform_content_id': 'UCunknown', 'status': 'success'},
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        fm.mark_uploaded.assert_not_called()

    async def test_mark_uploaded_oserror_does_not_propagate(
        self,
    ) -> None:
        '''If mark_uploaded raises OSError (e.g. disk full,
        cross-device move), the loop continues with the next
        record rather than aborting the whole batch.'''
        fm = AsyncMock()
        fm.mark_uploaded.side_effect = [
            OSError('disk full'), None,
        ]
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ]
        results: list[dict] = [
            {'platform_content_id': 'UCabc', 'status': 'success'},
            {'platform_content_id': 'UCxyz', 'status': 'success'},
        ]
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        # Both attempts were made even though the first raised.
        self.assertEqual(fm.mark_uploaded.await_count, 2)


class TestBulkProgressWsUrl(unittest.TestCase):

    def test_https_becomes_wss(self) -> None:
        url: str = bulk_progress_ws_url(
            'https://api.scrape.exchange', 'abcdef',
        )
        self.assertEqual(
            url,
            'wss://api.scrape.exchange/api/v1/bulk/progress/abcdef',
        )

    def test_http_becomes_ws(self) -> None:
        url: str = bulk_progress_ws_url(
            'http://localhost:8000', 'xyz123',
        )
        self.assertEqual(
            url, 'ws://localhost:8000/api/v1/bulk/progress/xyz123',
        )

    def test_unknown_scheme_passthrough(self) -> None:
        '''Custom or schemeless test fixtures are left alone.'''
        url: str = bulk_progress_ws_url(
            'ws://test.local', 'abcdef',
        )
        self.assertEqual(
            url, 'ws://test.local/api/v1/bulk/progress/abcdef',
        )


def _fake_client(
    auth: str | None = 'Bearer test-token',
) -> MagicMock:
    client = MagicMock()
    client.headers = {} if auth is None else {'Authorization': auth}
    return client


class _FakeWebSocket:
    '''Minimal async-context-manager + recv() mock.'''

    def __init__(self, messages: list[str | Exception]) -> None:
        self._messages: list[str | Exception] = list(messages)

    async def __aenter__(self) -> '_FakeWebSocket':
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def recv(self) -> str:
        if not self._messages:
            raise websockets.exceptions.ConnectionClosedOK(None, None)
        item: str | Exception = self._messages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class TestStreamBulkJobProgress(unittest.IsolatedAsyncioTestCase):

    async def test_returns_true_on_completed_status(self) -> None:
        '''A message with status=completed is the terminal signal;
        the helper returns True so the caller proceeds to results.'''
        messages: list[str] = [
            orjson.dumps({
                'job_id': 'abcdef', 'status': 'pending',
            }).decode(),
            orjson.dumps({
                'job_id': 'abcdef', 'status': 'in_progress',
            }).decode(),
            orjson.dumps({
                'job_id': 'abcdef', 'status': 'completed',
            }).decode(),
        ]
        ws = _FakeWebSocket(messages)

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', _fake_client(), 30.0,
            )
        self.assertTrue(ok)

    async def test_returns_true_on_failed_status(self) -> None:
        '''status=failed is also terminal — the helper returns True
        so the caller still fetches per-record results.'''
        messages: list[str] = [
            orjson.dumps({
                'job_id': 'abcdef', 'status': 'failed',
            }).decode(),
        ]
        ws = _FakeWebSocket(messages)

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', _fake_client(), 30.0,
            )
        self.assertTrue(ok)

    async def test_returns_true_on_clean_close(self) -> None:
        '''ConnectionClosedOK (server code 1000) means "job already
        complete" — the caller should still proceed to fetch
        results, so the helper returns True.'''
        ws = _FakeWebSocket([])  # empty → recv raises ConnectionClosedOK

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', _fake_client(), 30.0,
            )
        self.assertTrue(ok)

    async def test_returns_false_on_error_close(self) -> None:
        '''An auth/ownership/not-found close (code 4xxx) is a real
        failure; the helper returns False and the caller leaves
        source files in base_dir for retry.'''
        err: websockets.exceptions.ConnectionClosedError = (
            websockets.exceptions.ConnectionClosedError(None, None)
        )
        ws = _FakeWebSocket([err])

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', _fake_client(), 30.0,
            )
        self.assertFalse(ok)

    async def test_returns_false_when_no_auth_header(self) -> None:
        '''If the ExchangeClient has no Authorization header, the
        WebSocket connection cannot be authenticated — bail without
        connecting.'''
        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
        ) as mock_connect:
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test',
                _fake_client(auth=None), 30.0,
            )
        self.assertFalse(ok)
        mock_connect.assert_not_called()

    async def test_non_json_messages_are_ignored(self) -> None:
        '''A garbled message must not abort the loop — the helper
        keeps reading until a terminal status arrives.'''
        messages: list[str] = [
            'not json at all',
            orjson.dumps({
                'job_id': 'abcdef', 'status': 'completed',
            }).decode(),
        ]
        ws = _FakeWebSocket(messages)

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', _fake_client(), 30.0,
            )
        self.assertTrue(ok)


if __name__ == '__main__':
    unittest.main()
