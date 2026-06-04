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

import asyncio
import os
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
    reserve_bulk_upload_slot,
    resume_pending_bulk_uploads,
    stream_bulk_job_progress,
    wait_for_bulk_upload_slot,
    write_bulk_state,
)


class TestChannelUploadSettings(unittest.TestCase):

    def test_channel_concurrency_does_not_set_upload_concurrency(
        self,
    ) -> None:
        from tools.yt_channel_upload import ChannelUploadSettings

        with patch.dict(
            os.environ,
            {
                'CHANNEL_CONCURRENCY': '768',
            },
            clear=True,
        ):
            settings = ChannelUploadSettings(
                _cli_parse_args=False,
            )

        self.assertEqual(settings.channel_upload_concurrency, 3)

    def test_channel_upload_concurrency_env_sets_upload_concurrency(
        self,
    ) -> None:
        from tools.yt_channel_upload import ChannelUploadSettings

        with patch.dict(
            os.environ,
            {
                'CHANNEL_UPLOAD_CONCURRENCY': '7',
            },
            clear=True,
        ):
            settings = ChannelUploadSettings(
                _cli_parse_args=False,
            )

        self.assertEqual(settings.channel_upload_concurrency, 7)


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


class TestWaitForBulkUploadSlot(unittest.IsolatedAsyncioTestCase):

    async def test_returns_immediately_below_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fm = MagicMock()
            fm.base_dir = Path(tmp)
            with patch(
                'scrape_exchange.bulk_upload.resume_pending_bulk_uploads',
                new=AsyncMock(),
            ) as resume_mock:
                await wait_for_bulk_upload_slot(
                    fm, MagicMock(), 'http://test',
                    max_active_jobs=1,
                )
            resume_mock.assert_not_called()

    async def test_waits_on_persisted_pending_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fm = MagicMock()
            fm.base_dir = Path(tmp)
            await write_bulk_state(
                fm,
                BulkUploadState(
                    job_id='job1',
                    batch_id='batch1',
                    schema_owner='boinko',
                    schema_version='0.0.2',
                    platform='youtube',
                    entity='channel',
                    upload_filename='channels-batch1.jsonl',
                    batch_records=[('UCabc', 'channel-UCabc.json.br')],
                ),
            )

            async def _resume(*_args, **_kwargs) -> None:
                await delete_bulk_state(fm, 'job1')

            with patch(
                'scrape_exchange.bulk_upload.resume_pending_bulk_uploads',
                new=AsyncMock(side_effect=_resume),
            ) as resume_mock:
                await wait_for_bulk_upload_slot(
                    fm, MagicMock(), 'http://test',
                    max_active_jobs=1,
                    sleep_seconds=0,
                )

            resume_mock.assert_awaited_once()
            self.assertEqual(list_bulk_states(fm), [])

    async def test_reservation_counts_before_state_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fm = MagicMock()
            fm.base_dir = Path(tmp)
            resume_mock = AsyncMock()

            with patch(
                'scrape_exchange.bulk_upload.resume_pending_bulk_uploads',
                new=resume_mock,
            ):
                async with reserve_bulk_upload_slot(
                    fm, MagicMock(), 'http://test',
                    max_active_jobs=1,
                    sleep_seconds=0,
                ):
                    task = asyncio.create_task(wait_for_bulk_upload_slot(
                        fm, MagicMock(), 'http://test',
                        max_active_jobs=1,
                        sleep_seconds=0,
                    ))
                    await asyncio.sleep(0)
                    self.assertFalse(task.done())

                await asyncio.wait_for(task, timeout=0.1)
            resume_mock.assert_awaited()


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


class TestCollectChannelRecordValidator(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_invalid_record_marked_invalid_and_skipped(
        self,
    ) -> None:
        '''A record that fails the validator must not return a
        line; the file must be moved to ``<filename>.invalid``.'''
        from tools.yt_channel_upload import _collect_channel_record

        channel = MagicMock()
        channel.channel_id = 'UCabc'
        channel.channel_handle = 'somehandle'
        channel.to_dict = MagicMock(
            return_value={'channel_id': 'UCabc'},
        )

        fm = AsyncMock()
        fm.read_file = AsyncMock(return_value={
            'channel_id': 'UCabc',
        })
        fm.mark_invalid = AsyncMock(
            return_value='foo.json.br.invalid',
        )

        validator = MagicMock()
        validator.validate = MagicMock(
            return_value='/url: required field missing',
        )

        with patch(
            'tools.yt_channel_upload.YouTubeChannel.from_dict',
            return_value=channel,
        ), patch(
            'tools.yt_channel_upload.resolve_channel_upload_handle',
            new=AsyncMock(return_value='somehandle'),
        ):
            result = await _collect_channel_record(
                'foo.json.br', fm,
                AsyncMock(), AsyncMock(), validator,
            )

        self.assertIsNone(result)
        validator.validate.assert_called_once()
        fm.mark_invalid.assert_awaited_once_with('foo.json.br')


class TestUnifiedBulkUploadLoopValidator(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_invalid_record_marked_invalid_and_skipped(
        self,
    ) -> None:
        '''A brotli'd record that fails the validator must be moved
        to ``<filename>.invalid`` before the loop waits for more
        work.'''
        import brotli
        from tools.yt_channel_upload import _unified_bulk_upload_loop

        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            bad_file: Path = base_dir / 'channel-UCbad.json.br'
            bad_file.write_bytes(
                brotli.compress(orjson.dumps({'channel_id': 'UCbad'})),
            )

            fm = MagicMock()
            fm.base_dir = base_dir
            fm.list_base = MagicMock(return_value=[bad_file.name])
            fm.mark_invalid = AsyncMock(
                return_value=f'{bad_file.name}.invalid',
            )

            validator = MagicMock()
            validator.validate = MagicMock(
                return_value='/url: required field missing',
            )

            settings = MagicMock()
            settings.bulk_batch_size = 100
            settings.bulk_max_batch_bytes = 1_000_000
            settings.max_active_bulk_jobs = 1

            class _StopLoop(Exception):
                pass

            async def _stop(*_args, **_kwargs) -> None:
                raise _StopLoop()

            with patch(
                'tools.yt_channel_upload._wait_for_channel_changes',
                new=_stop,
            ):
                with self.assertRaises(_StopLoop):
                    await _unified_bulk_upload_loop(
                        settings, MagicMock(), fm, validator,
                    )

            validator.validate.assert_called_once()
            fm.mark_invalid.assert_awaited_once_with(bad_file.name)


class TestEnqueueUploadChannelValidator(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_invalid_record_not_enqueued_and_marked(
        self,
    ) -> None:
        from tools.yt_channel_upload import enqueue_upload_channel

        channel = MagicMock()
        channel.channel_id = 'UCabc'
        channel.channel_handle = 'somehandle'
        channel.url = 'https://youtube.com/@somehandle'
        channel.to_dict = MagicMock(
            return_value={'channel_id': 'UCabc'},
        )

        client = MagicMock()
        client.enqueue_upload = MagicMock(return_value=True)

        fm = AsyncMock()
        fm.mark_invalid = AsyncMock(
            return_value='foo.json.br.invalid',
        )

        validator = MagicMock()
        validator.validate = MagicMock(
            return_value='/url: required field missing',
        )

        settings = MagicMock()
        settings.exchange_url = 'http://test'
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'

        with patch(
            'tools.yt_channel_upload'
            '.resolve_channel_upload_handle',
            new=AsyncMock(return_value='somehandle'),
        ):
            ok: bool = await enqueue_upload_channel(
                settings, client, fm, 'foo.json.br', channel,
                AsyncMock(), AsyncMock(), validator,
            )

        self.assertFalse(ok)
        client.enqueue_upload.assert_not_called()
        fm.mark_invalid.assert_awaited_once_with('foo.json.br')


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
            )
            client.get.assert_not_called()

    async def test_state_older_than_24_hours_is_deleted(
        self,
    ) -> None:
        '''Very old accepted jobs are abandoned instead of re-polled.'''
        with tempfile.TemporaryDirectory() as base:
            fm = _fake_fm(Path(base))
            stale: BulkUploadState = _fake_state('stale')
            stale.created_at = (
                datetime.now(UTC) - timedelta(hours=25)
            ).isoformat()
            await write_bulk_state(fm, stale)
            client = MagicMock()
            client.get = AsyncMock()

            await resume_pending_bulk_uploads(
                fm, client, 'http://test',
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
                    poll_timeout_seconds=0.0,
                )

            touch.assert_called()
            self.assertEqual(len(list_bulk_states(fm)), 1)


if __name__ == '__main__':
    unittest.main()
