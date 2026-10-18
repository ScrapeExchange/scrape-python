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

import tempfile
import unittest
from datetime import datetime, timedelta, UTC
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import websockets.exceptions
import websockets.frames

from scrape_exchange.bulk_upload import (
    BULK_STATE_DIR_NAME,
    BulkResults,
    BulkUploadState,
    apply_bulk_results,
    bulk_progress_ws_url,
    delete_bulk_state,
    list_bulk_states,
    resume_pending_bulk_uploads,
    stream_bulk_job_progress,
    write_bulk_state,
)

_MAX_AGE: timedelta = timedelta(hours=3)


class TestApplyBulkResults(unittest.IsolatedAsyncioTestCase):

    async def test_success_by_content_id_marks_uploaded(self) -> None:
        '''A success result keyed by platform_content_id moves the
        matching source file to uploaded_dir.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ]
        # Failures-only contract: empty failures + total==submitted
        # means both records succeeded.
        results: BulkResults = BulkResults(
            total=2, succeeded=2, failed=0, duplicate=0, failures=[],
        )
        success, failed, missing, success_ids = await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        marked: list[str] = [
            call.args[0] for call in fm.mark_uploaded.call_args_list
        ]
        self.assertEqual(
            sorted(marked),
            ['channel-UCabc.json.br', 'channel-UCxyz.json.br'],
        )
        self.assertEqual((success, failed, missing), (2, 0, 0))
        self.assertEqual(success_ids, {'UCabc', 'UCxyz'})

    async def test_failed_records_left_for_retry(self) -> None:
        '''A failed record must not call mark_uploaded.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=1, succeeded=0, failed=1, duplicate=0,
            failures=[{
                'platform_content_id': 'UCabc',
                'status': 'failed',
                'reason': 'VALIDATION_ERROR: foo',
            }],
        )
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        fm.mark_uploaded.assert_not_called()

    async def test_record_index_fallback(self) -> None:
        '''A failure keyed only by record_index leaves that slot's
        source file for retry and marks the other uploaded.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=2, succeeded=1, failed=1, duplicate=0,
            failures=[{'record_index': 1, 'status': 'failed'}],
        )
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        # index 1 (UCxyz) failed -> left; index 0 (UCabc) uploaded.
        fm.mark_uploaded.assert_awaited_once_with(
            'channel-UCabc.json.br',
        )

    async def test_incomplete_total_left_for_retry(self) -> None:
        '''When the server's total is less than the records we sent,
        the whole batch is left in base_dir — successes cannot be
        identified under the failures-only contract.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
            ('UCmissing', 'channel-UCmissing.json.br'),
        ]
        # Server only accounted for 2 of the 3 submitted records.
        results: BulkResults = BulkResults(
            total=2, succeeded=2, failed=0, duplicate=0, failures=[],
        )
        success, failed, missing, _ = await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        fm.mark_uploaded.assert_not_called()
        self.assertEqual((success, failed, missing), (0, 0, 3))

    async def test_unmatchable_failure_left_for_retry(self) -> None:
        '''A failure with no matching content_id and no record_index
        blocks salvage — the whole batch is left for retry.'''
        fm = AsyncMock()
        batch_records: list[tuple[str, str]] = [
            ('UCabc', 'channel-UCabc.json.br'),
        ]
        results: BulkResults = BulkResults(
            total=1, succeeded=0, failed=1, duplicate=0,
            failures=[{
                'platform_content_id': 'UCunknown',
                'status': 'failed',
            }],
        )
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
        results: BulkResults = BulkResults(
            total=2, succeeded=2, failed=0, duplicate=0, failures=[],
        )
        await apply_bulk_results(
            batch_records, results, fm, 'batch1', 'job123',
        )
        # Both attempts were made even though the first raised.
        self.assertEqual(fm.mark_uploaded.await_count, 2)


class TestBulkProgressWsUrl(unittest.TestCase):

    def test_https_becomes_wss(self) -> None:
        url: str = bulk_progress_ws_url(
            'https://scrape.exchange', 'abcdef',
        )
        self.assertEqual(
            url,
            'wss://scrape.exchange/api/v1/bulk/progress/abcdef',
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

    async def test_service_restart_close_polls_status_until_terminal(
        self,
    ) -> None:
        '''
        A 1012 (service_restart) close means a uvicorn worker on
        the API was recycled mid-stream, but the bulk job itself
        keeps running. The helper falls back to polling
        ``GET /api/v1/bulk?job_id=...`` and returns True once a
        terminal status is observed, so the caller can still apply
        per-record results instead of re-uploading the batch.
        '''
        close: websockets.frames.Close = websockets.frames.Close(
            code=1012, reason='service restart',
        )
        err: websockets.exceptions.ConnectionClosedError = (
            websockets.exceptions.ConnectionClosedError(close, None)
        )
        ws = _FakeWebSocket([err])

        status_resp = MagicMock()
        status_resp.status_code = 200
        status_resp.json = MagicMock(
            return_value={'status': 'completed'},
        )

        client: MagicMock = _fake_client()
        client.get = AsyncMock(return_value=status_resp)

        with patch(
            'scrape_exchange.bulk_upload.websockets.connect',
            return_value=ws,
        ):
            ok: bool = await stream_bulk_job_progress(
                'abcdef', 'http://test', client, 30.0,
            )
        self.assertTrue(ok)
        client.get.assert_awaited_once()

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


def _fake_state(job_id: str = 'abc123') -> BulkUploadState:
    return BulkUploadState(
        job_id=job_id,
        batch_id='deadbeef',
        schema_owner='boinko',
        schema_version='0.0.2',
        platform='youtube',
        entity='channel',
        upload_filename=f'channels-{job_id}.jsonl',
        batch_records=[
            ('UCabc', 'channel-UCabc.json.br'),
            ('UCxyz', 'channel-UCxyz.json.br'),
        ],
        created_at=datetime.now(UTC).isoformat(),
    )


def _fake_fm(base_dir: Path) -> MagicMock:
    fm = MagicMock()
    fm.base_dir = base_dir
    fm.mark_uploaded = AsyncMock()
    return fm


class TestBulkUploadStatePersistence(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_write_and_list_round_trip(self) -> None:
        '''A written state file can be loaded back identically.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            state: BulkUploadState = _fake_state('jobaaa')
            await write_bulk_state(fm, state)

            state_dir: Path = Path(base) / BULK_STATE_DIR_NAME
            self.assertTrue(state_dir.is_dir())
            self.assertTrue(
                (state_dir / 'jobaaa.json').is_file(),
            )

            loaded: list[BulkUploadState] = list_bulk_states(fm)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0], state)

    async def test_delete_removes_file(self) -> None:
        '''delete_bulk_state removes the matching state file.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            state: BulkUploadState = _fake_state('jobzzz')
            await write_bulk_state(fm, state)
            self.assertEqual(len(list_bulk_states(fm)), 1)

            await delete_bulk_state(fm, 'jobzzz')

            self.assertEqual(list_bulk_states(fm), [])

    async def test_delete_missing_is_no_op(self) -> None:
        '''Deleting a non-existent state file does not raise.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            await delete_bulk_state(fm, 'nope')  # must not raise

    def test_corrupt_file_dropped_silently(self) -> None:
        '''A malformed state file is removed and skipped on load.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            state_dir: Path = Path(base) / BULK_STATE_DIR_NAME
            state_dir.mkdir()
            bad: Path = state_dir / 'bad.json'
            bad.write_bytes(b'{not valid json')

            loaded: list[BulkUploadState] = list_bulk_states(fm)

            self.assertEqual(loaded, [])
            self.assertFalse(bad.exists())

    def test_list_no_directory_returns_empty(self) -> None:
        '''Missing .bulk directory yields an empty list.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            self.assertEqual(list_bulk_states(fm), [])


def _resp(status_code: int, body: dict | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status_code
    r.json = MagicMock(return_value=body or {})
    r.text = ''
    return r


class TestResumePendingBulkUploads(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_no_state_files_is_no_op(self) -> None:
        '''Empty .bulk dir → resume returns without API calls.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            client = MagicMock()
            client.get = AsyncMock()
            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )
            client.get.assert_not_called()

    async def test_state_older_than_max_age_is_deleted(
        self,
    ) -> None:
        '''Accepted jobs older than max_age are abandoned, not re-polled.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            stale: BulkUploadState = _fake_state('stale')
            stale.created_at = (
                datetime.now(UTC) - timedelta(hours=4)
            ).isoformat()
            await write_bulk_state(fm, stale)
            client = MagicMock()
            client.get = AsyncMock()

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )

            client.get.assert_not_called()
            self.assertEqual(list_bulk_states(fm), [])

    async def test_404_deletes_state(self) -> None:
        '''API 404 on the job → state file is deleted.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            await write_bulk_state(fm, _fake_state('orphan'))
            client = MagicMock()
            client.get = AsyncMock(return_value=_resp(404))

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )

            client.get.assert_awaited_once()
            self.assertEqual(list_bulk_states(fm), [])

    async def test_terminal_status_reconciles_and_deletes(
        self,
    ) -> None:
        '''
        API returns ``completed`` → fetch results, apply them,
        delete state file.
        '''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            await write_bulk_state(fm, _fake_state('done'))
            # _fake_state submits 2 records; both succeed (empty
            # failures list, total == submitted) under ADR-0008.
            client = MagicMock()
            client.get = AsyncMock(side_effect=[
                _resp(200, {'status': 'completed'}),
                _resp(200, {
                    'total': 2, 'succeeded': 2, 'failed': 0,
                    'duplicate': 0, 'results': [],
                }),
            ])

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )

            self.assertEqual(client.get.await_count, 2)
            fm.mark_uploaded.assert_any_await('channel-UCabc.json.br')
            fm.mark_uploaded.assert_any_await('channel-UCxyz.json.br')
            self.assertEqual(list_bulk_states(fm), [])

    async def test_non_200_non_404_leaves_state(self) -> None:
        '''Server-side hiccups → state file kept for next start.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            await write_bulk_state(fm, _fake_state('hiccup'))
            client = MagicMock()
            client.get = AsyncMock(return_value=_resp(503))

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )

            self.assertEqual(len(list_bulk_states(fm)), 1)

    async def test_transport_failure_leaves_state(self) -> None:
        '''Transport-level error during status fetch → keep state.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            await write_bulk_state(fm, _fake_state('netfail'))
            client = MagicMock()
            client.get = AsyncMock(side_effect=RuntimeError('boom'))

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
                max_age=_MAX_AGE,
            )

            self.assertEqual(len(list_bulk_states(fm)), 1)

    async def test_pending_poll_touches_watchdog_work(
        self,
    ) -> None:
        '''Intentional resume polling must not look like a hang.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            fresh: BulkUploadState = _fake_state('pending')
            fresh.created_at = datetime.now(UTC).isoformat()
            await write_bulk_state(fm, fresh)
            client = MagicMock()
            client.get = AsyncMock(return_value=_resp(
                200, {'status': 'running'},
            ))
            touch = MagicMock()

            with patch(
                'scrape_exchange.bulk_upload._touch_watchdog_work',
                touch,
            ):
                await resume_pending_bulk_uploads(
                    fm, client, 'http://test',
                    max_age=_MAX_AGE,
                    poll_timeout_seconds=0.0,
                )

            touch.assert_called()
            self.assertEqual(len(list_bulk_states(fm)), 1)


if __name__ == '__main__':
    unittest.main()
