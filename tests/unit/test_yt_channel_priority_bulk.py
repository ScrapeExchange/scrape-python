'''Tests for _unified_bulk_upload_loop — the single
bulk-batched loop that replaces the per-file priority
sweep and awatch watcher in upload-only mode.

Also includes tests for _priority_channel_work_items,
migrated from test_yt_channel_priority_sweep.py.
'''

import asyncio
import importlib.util
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

import brotli
import orjson


def _load_yt_channel_scrape() -> ModuleType:
    '''Load tools/yt_channel_scrape.py under the bare
    module name ``yt_channel_scrape``. Checks sys.modules
    under both ``yt_channel_scrape`` and
    ``tools.yt_channel_scrape`` first so that repeated
    test runs within the same process do not trigger
    prometheus duplicate-registry errors regardless of
    which test file loaded the module first.'''
    for key in ('yt_channel_scrape',
                'tools.yt_channel_scrape'):
        if key in sys.modules:
            mod: ModuleType = sys.modules[key]
            sys.modules['yt_channel_scrape'] = mod
            return mod
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = (
        repo_root / 'tools' / 'yt_channel_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_channel_scrape', module_path,
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_channel_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_channel_scrape: ModuleType = (
    _load_yt_channel_scrape()
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_channel_file(
    directory: Path,
    name: str,
    age_secs: float = 5.0,
    channel_id: str = 'UC_test',
) -> Path:
    '''Write a valid brotli-compressed channel record and
    set its mtime to ``age_secs`` seconds ago.'''
    record: dict = {
        'channel_handle': name.removeprefix(
            'channel-rss-',
        ).removeprefix('channel-').removesuffix(
            '.json.br',
        ),
        'channel_id': channel_id,
        'url': f'https://www.youtube.com/channel/{channel_id}',
        'title': 'Test Channel',
        'subscriber_count': 1,
        'video_count': 0,
        'view_count': 0,
        'description': '',
    }
    path: Path = directory / name
    path.write_bytes(brotli.compress(
        orjson.dumps(record),
        quality=1,
        mode=brotli.MODE_TEXT,
    ))
    now: float = time.time()
    os.utime(path, (now - age_secs, now - age_secs))
    return path


def _passing_validator() -> mock.MagicMock:
    v: mock.MagicMock = mock.MagicMock()
    v.validate.return_value = None
    return v


def _mock_client(
    status_code: int = 201,
    job_id: str = 'j1',
) -> mock.MagicMock:
    response: mock.MagicMock = mock.MagicMock()
    response.status_code = status_code
    response.json.return_value = {'job_id': job_id}
    client: mock.MagicMock = mock.MagicMock()
    client.post = mock.AsyncMock(return_value=response)
    client.get = mock.AsyncMock(return_value=response)
    client.exchange_url = 'http://exchange.test'
    client.headers = {'Authorization': 'Bearer test'}
    return client


def _make_fm(
    base_dir: Path,
) -> mock.MagicMock:
    '''Build a fake AssetFileManagement whose uploaded_dir
    is ``base_dir / "uploaded"`` and whose list_base and
    mark_uploaded behave realistically.'''
    uploaded_dir: Path = base_dir / 'uploaded'
    uploaded_dir.mkdir(exist_ok=True)

    fm: mock.MagicMock = mock.MagicMock()
    fm.base_dir = base_dir
    fm.uploaded_dir = uploaded_dir

    def _list_base(
        prefix: str = '',
        suffix: str = '',
    ) -> list[str]:
        return [
            f.name for f in base_dir.iterdir()
            if f.name.startswith(prefix)
            and f.name.endswith(suffix)
        ]

    fm.list_base.side_effect = _list_base

    async def _mark_uploaded(filename: str) -> Path:
        src: Path = base_dir / filename
        dst: Path = uploaded_dir / filename
        src.rename(dst)
        return dst

    fm.mark_uploaded = mock.AsyncMock(
        side_effect=_mark_uploaded,
    )
    return fm


# ---------------------------------------------------------------------------
# Migrated: _priority_channel_work_items tests
# ---------------------------------------------------------------------------

class TestPriorityChannelWorkItems(unittest.TestCase):
    '''_priority_channel_work_items enumerates
    channel-rss-*.json.br files in mtime ascending order
    and skips *.json.br.failed.'''

    def setUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name: str, age_secs: float) -> str:
        path: str = os.path.join(self.tmp, name)
        Path(path).write_bytes(b'x')
        now: float = time.time()
        os.utime(
            path, (now - age_secs, now - age_secs),
        )
        return path

    def test_returns_empty_for_empty_directory(
        self,
    ) -> None:
        result: list[Path] = (
            yt_channel_scrape
            ._priority_channel_work_items(self.tmp)
        )
        self.assertEqual(result, [])

    def test_filters_by_prefix_and_suffix(
        self,
    ) -> None:
        self._write(
            'channel-rss-a.json.br', age_secs=1,
        )
        self._write(
            'channel-rss-b.json.br', age_secs=1,
        )
        self._write(
            'channel-rss-c.json.br.failed',
            age_secs=1,
        )
        self._write(
            'channel-d.json.br', age_secs=1,
        )
        self._write('other.txt', age_secs=1)
        result: list[Path] = (
            yt_channel_scrape
            ._priority_channel_work_items(self.tmp)
        )
        names: set[str] = {p.name for p in result}
        self.assertEqual(
            names,
            {
                'channel-rss-a.json.br',
                'channel-rss-b.json.br',
            },
        )

    def test_orders_by_mtime_ascending(self) -> None:
        self._write(
            'channel-rss-young.json.br', age_secs=1,
        )
        self._write(
            'channel-rss-old.json.br', age_secs=60,
        )
        result: list[Path] = (
            yt_channel_scrape
            ._priority_channel_work_items(self.tmp)
        )
        self.assertEqual(
            [p.name for p in result],
            [
                'channel-rss-old.json.br',
                'channel-rss-young.json.br',
            ],
        )

    def test_missing_directory_returns_empty(
        self,
    ) -> None:
        result: list[Path] = (
            yt_channel_scrape
            ._priority_channel_work_items(
                os.path.join(self.tmp, 'doesnotexist'),
            )
        )
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# _unified_bulk_upload_loop tests
# ---------------------------------------------------------------------------

class TestUnifiedBulkUploadLoop(
    unittest.IsolatedAsyncioTestCase,
):
    async def asyncSetUp(self) -> None:
        self.tmp: Path = Path(tempfile.mkdtemp())
        self.base_dir: Path = self.tmp / 'base'
        self.priority_dir: Path = (
            self.tmp / 'priority'
        )
        self.base_dir.mkdir()
        self.priority_dir.mkdir()

        self.fm: mock.MagicMock = _make_fm(
            self.base_dir,
        )

        settings: mock.MagicMock = mock.MagicMock()
        settings.bulk_batch_size = 10
        settings.bulk_progress_timeout_seconds = 30.0
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://exchange.test'
        settings.channel_priority_directory_path = str(
            self.priority_dir,
        )
        self.settings: mock.MagicMock = settings

    async def asyncTearDown(self) -> None:
        shutil.rmtree(str(self.tmp), ignore_errors=True)

    def _make_priority(
        self,
        handle: str,
        channel_id: str,
        age_secs: float = 10.0,
    ) -> Path:
        return _make_channel_file(
            self.priority_dir,
            f'channel-rss-{handle}.json.br',
            age_secs=age_secs,
            channel_id=channel_id,
        )

    def _make_base(
        self,
        handle: str,
        channel_id: str,
        age_secs: float = 10.0,
    ) -> Path:
        return _make_channel_file(
            self.base_dir,
            f'channel-{handle}.json.br',
            age_secs=age_secs,
            channel_id=channel_id,
        )

    def _fetch_results(
        self,
        batch_records: list[tuple[str, Path, str]],
        failed_ids: set[str] | None = None,
    ) -> list[dict]:
        '''Build per-record results list from
        batch_records, marking all as success unless
        the channel_id is in failed_ids.'''
        results: list[dict] = []
        for idx, (cid, _path, _kind) in enumerate(
            batch_records,
        ):
            status: str = (
                'failed'
                if failed_ids and cid in failed_ids
                else 'success'
            )
            results.append({
                'platform_content_id': cid,
                'record_index': idx,
                'status': status,
            })
        return results

    # ------------------------------------------------------------------
    # Test 1: priority-only batch, all succeed
    # ------------------------------------------------------------------
    async def test_priority_only_batch_succeeds_all_moved(
        self,
    ) -> None:
        '''3 priority files, all succeed → all 3 moved to
        uploaded_dir; retries dict is empty at end.'''
        p1 = self._make_priority(
            'a', 'UC_a', age_secs=30,
        )
        p2 = self._make_priority(
            'b', 'UC_b', age_secs=20,
        )
        p3 = self._make_priority(
            'c', 'UC_c', age_secs=10,
        )
        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j1',
        )
        uploaded_dir: Path = self.fm.uploaded_dir

        async def fake_stream(
            job_id: str,
            exchange_url: str,
            cl: object,
            timeout: float,
        ) -> bool:
            return True

        async def fake_fetch(
            job_id: str,
            exchange_url: str,
            cl: object,
        ) -> list[dict]:
            # batch_records was built inside the loop;
            # derive results from what was posted
            return [
                {
                    'platform_content_id': 'UC_a',
                    'status': 'success',
                },
                {
                    'platform_content_id': 'UC_b',
                    'status': 'success',
                },
                {
                    'platform_content_id': 'UC_c',
                    'status': 'success',
                },
            ]

        with (
            mock.patch.object(
                yt_channel_scrape,
                'stream_bulk_job_progress',
                side_effect=fake_stream,
            ),
            mock.patch.object(
                yt_channel_scrape,
                'fetch_bulk_results',
                side_effect=fake_fetch,
            ),
            mock.patch(
                'asyncio.sleep',
                side_effect=[None, asyncio.CancelledError()],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        # All 3 priority files should be in uploaded_dir
        for p in (p1, p2, p3):
            self.assertFalse(
                p.exists(),
                f'{p.name} still in priority_dir',
            )
            self.assertTrue(
                (uploaded_dir / p.name).exists(),
                f'{p.name} missing from uploaded_dir',
            )

        # POST should have been called once
        client.post.assert_awaited_once()
        # The posted body should contain 3 jsonl lines
        call_kwargs = client.post.call_args
        batch_buf: bytes = (
            call_kwargs.kwargs['files']['file'][1]
        )
        lines: list[bytes] = [
            ln for ln in batch_buf.split(b'\n') if ln
        ]
        self.assertEqual(len(lines), 3)

    # ------------------------------------------------------------------
    # Test 2: top-up priority with base files
    # ------------------------------------------------------------------
    async def test_tops_up_priority_with_base_files(
        self,
    ) -> None:
        '''2 priority + 5 base, batch_size=4 → 2 priority
        + 2 oldest base in batch; priority → uploaded_dir,
        base → fm.mark_uploaded.'''
        self.settings.bulk_batch_size = 4

        p1 = self._make_priority(
            'p1', 'UC_p1', age_secs=50,
        )
        p2 = self._make_priority(
            'p2', 'UC_p2', age_secs=40,
        )
        # 5 base files with different ages
        b_old = self._make_base(
            'b_old', 'UC_bold', age_secs=100,
        )
        b_mid = self._make_base(
            'b_mid', 'UC_bmid', age_secs=80,
        )
        self._make_base('b_n1', 'UC_bn1', age_secs=20)
        self._make_base('b_n2', 'UC_bn2', age_secs=10)
        self._make_base('b_n3', 'UC_bn3', age_secs=5)

        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j2',
        )

        posted_ids: list[str] = []

        async def fake_fetch(
            job_id: str,
            exchange_url: str,
            cl: object,
        ) -> list[dict]:
            call_kwargs = client.post.call_args
            buf: bytes = (
                call_kwargs.kwargs['files']['file'][1]
            )
            results_out: list[dict] = []
            for idx, line in enumerate(
                b for b in buf.split(b'\n') if b
            ):
                rec: dict = orjson.loads(line)
                cid: str = rec['channel_id']
                posted_ids.append(cid)
                results_out.append({
                    'platform_content_id': cid,
                    'record_index': idx,
                    'status': 'success',
                })
            return results_out

        async def fake_stream(
            job_id: str,
            exchange_url: str,
            cl: object,
            timeout: float,
        ) -> bool:
            return True

        with (
            mock.patch.object(
                yt_channel_scrape,
                'stream_bulk_job_progress',
                side_effect=fake_stream,
            ),
            mock.patch.object(
                yt_channel_scrape,
                'fetch_bulk_results',
                side_effect=fake_fetch,
            ),
            mock.patch(
                'asyncio.sleep',
                side_effect=asyncio.CancelledError(),
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        self.assertEqual(len(posted_ids), 4)
        # Priority files come first
        self.assertIn('UC_p1', posted_ids)
        self.assertIn('UC_p2', posted_ids)
        # Oldest 2 base files
        self.assertIn('UC_bold', posted_ids)
        self.assertIn('UC_bmid', posted_ids)

        # Priority files moved to uploaded_dir
        uploaded_dir: Path = self.fm.uploaded_dir
        self.assertTrue((uploaded_dir / p1.name).exists())
        self.assertTrue((uploaded_dir / p2.name).exists())
        # Base files moved via fm.mark_uploaded
        self.assertTrue(
            (uploaded_dir / b_old.name).exists(),
        )
        self.assertTrue(
            (uploaded_dir / b_mid.name).exists(),
        )

    # ------------------------------------------------------------------
    # Test 3: partial failure bumps retry
    # ------------------------------------------------------------------
    async def test_priority_only_partial_failure_bumps_retry(
        self,
    ) -> None:
        '''1 success + 1 failed result → success file
        moved, failed file stays in priority_dir,
        retries[channel_id]==1.'''
        p_ok = self._make_priority(
            'ok', 'UC_ok', age_secs=20,
        )
        p_fail = self._make_priority(
            'fail', 'UC_fail', age_secs=10,
        )

        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j3',
        )

        async def fake_fetch(
            job_id: str,
            exchange_url: str,
            cl: object,
        ) -> list[dict]:
            return [
                {
                    'platform_content_id': 'UC_ok',
                    'status': 'success',
                },
                {
                    'platform_content_id': 'UC_fail',
                    'status': 'failed',
                    'reason': 'VALIDATION_ERROR',
                },
            ]

        async def fake_stream(
            job_id: str,
            exchange_url: str,
            cl: object,
            timeout: float,
        ) -> bool:
            return True

        with (
            mock.patch.object(
                yt_channel_scrape,
                'stream_bulk_job_progress',
                side_effect=fake_stream,
            ),
            mock.patch.object(
                yt_channel_scrape,
                'fetch_bulk_results',
                side_effect=fake_fetch,
            ),
            mock.patch(
                'asyncio.sleep',
                side_effect=[
                    None, asyncio.CancelledError(),
                ],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        uploaded_dir: Path = self.fm.uploaded_dir
        self.assertTrue(
            (uploaded_dir / p_ok.name).exists(),
            'success file not in uploaded_dir',
        )
        # Failed file still in priority_dir
        self.assertTrue(
            p_fail.exists(),
            'failed file should still be in priority_dir',
        )

    # ------------------------------------------------------------------
    # Test 4: max retries → rename to .failed
    # ------------------------------------------------------------------
    async def test_priority_max_retries_rename_failed(
        self,
    ) -> None:
        '''retries[channel_id]==PRIORITY_MAX_RETRIES-1 +
        failed result → file renamed .json.br.failed,
        retries cleared.'''
        max_retries: int = (
            yt_channel_scrape.PRIORITY_MAX_RETRIES
        )
        p = self._make_priority(
            'maxr', 'UC_maxr', age_secs=10,
        )

        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j4',
        )

        # Run the loop max_retries times with all-failed
        # results so the internal retry counter reaches MAX,
        # then verify the file is renamed to .failed.
        call_count: list[int] = [0]

        async def counting_fetch(
            job_id: str,
            exchange_url: str,
            cl: object,
        ) -> list[dict]:
            call_count[0] += 1
            return [
                {
                    'platform_content_id': 'UC_maxr',
                    'status': 'failed',
                    'reason': 'VALIDATION_ERROR',
                },
            ]

        async def counting_stream(
            job_id: str,
            exchange_url: str,
            cl: object,
            timeout: float,
        ) -> bool:
            return True

        # We need max_retries iterations, then a
        # CancelledError.  Each iteration calls sleep once.
        sleep_results: list = (
            [None] * max_retries
            + [asyncio.CancelledError()]
        )
        # POST must succeed for all iterations
        client.post = mock.AsyncMock(
            return_value=mock.MagicMock(
                status_code=201,
                json=mock.MagicMock(
                    return_value={'job_id': 'j4'},
                ),
            ),
        )

        with (
            mock.patch.object(
                yt_channel_scrape,
                'stream_bulk_job_progress',
                side_effect=counting_stream,
            ),
            mock.patch.object(
                yt_channel_scrape,
                'fetch_bulk_results',
                side_effect=counting_fetch,
            ),
            mock.patch(
                'asyncio.sleep',
                side_effect=sleep_results,
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        failed_path: Path = p.with_suffix(
            p.suffix + '.failed',
        )
        self.assertTrue(
            failed_path.exists(),
            f'Expected {failed_path.name} to exist',
        )
        self.assertFalse(
            p.exists(),
            'Original file should have been renamed',
        )

    # ------------------------------------------------------------------
    # Test 5: whole-batch POST error bumps priority retries
    # ------------------------------------------------------------------
    async def test_whole_batch_post_error_bumps_priority_only(
        self,
    ) -> None:
        '''POST raises Exception → priority retries++,
        base file untouched in base_dir.'''
        p1 = self._make_priority(
            'pp1', 'UC_pp1', age_secs=20,
        )
        p2 = self._make_priority(
            'pp2', 'UC_pp2', age_secs=10,
        )
        b1 = self._make_base(
            'bb1', 'UC_bb1', age_secs=10,
        )

        client: mock.MagicMock = mock.MagicMock()
        client.post = mock.AsyncMock(
            side_effect=Exception('network'),
        )
        client.exchange_url = 'http://exchange.test'
        client.headers = {'Authorization': 'Bearer test'}

        with (
            mock.patch(
                'asyncio.sleep',
                side_effect=[
                    None, asyncio.CancelledError(),
                ],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        # Priority files still in priority_dir
        self.assertTrue(
            p1.exists(),
            'p1 should still be in priority_dir',
        )
        self.assertTrue(
            p2.exists(),
            'p2 should still be in priority_dir',
        )
        # Base file untouched in base_dir
        self.assertTrue(
            b1.exists(),
            'base file should still be in base_dir',
        )
        self.fm.mark_uploaded.assert_not_called()

    # ------------------------------------------------------------------
    # Test 6: decode error renames priority file
    # ------------------------------------------------------------------
    async def test_decode_error_renames_priority_file(
        self,
    ) -> None:
        '''Priority file with garbage bytes → renamed
        .json.br.failed. Batch may be empty and loop
        continues without POST.'''
        p_bad: Path = (
            self.priority_dir
            / 'channel-rss-bad.json.br'
        )
        p_bad.write_bytes(b'not brotli at all!!!!')
        now: float = time.time()
        os.utime(p_bad, (now - 10, now - 10))

        client: mock.MagicMock = _mock_client()

        with (
            mock.patch(
                'asyncio.sleep',
                side_effect=[
                    None, asyncio.CancelledError(),
                ],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        failed_path: Path = p_bad.with_suffix(
            p_bad.suffix + '.failed',
        )
        self.assertTrue(
            failed_path.exists(),
            'corrupt priority file should be renamed',
        )
        self.assertFalse(p_bad.exists())
        # No POST should have been attempted (empty batch)
        client.post.assert_not_called()

    # ------------------------------------------------------------------
    # Test 7: schema validation failure renames priority
    # ------------------------------------------------------------------
    async def test_schema_validation_failure_renames_priority(
        self,
    ) -> None:
        '''Validator returns an error → priority file
        renamed .json.br.failed.'''
        p = self._make_priority(
            'v_fail', 'UC_vfail', age_secs=10,
        )

        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = (
            'missing required field "title"'
        )
        client: mock.MagicMock = _mock_client()

        with (
            mock.patch(
                'asyncio.sleep',
                side_effect=[
                    None, asyncio.CancelledError(),
                ],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    validator,
                    sleep_seconds=5.0,
                )

        failed_path: Path = p.with_suffix(
            p.suffix + '.failed',
        )
        self.assertTrue(
            failed_path.exists(),
            'validation-failed priority file not renamed',
        )
        client.post.assert_not_called()

    # ------------------------------------------------------------------
    # Test 8: empty dirs → just sleeps
    # ------------------------------------------------------------------
    async def test_empty_dirs_just_sleeps(
        self,
    ) -> None:
        '''Both dirs empty → no POST, asyncio.sleep
        called once with 5.0, CancelledError on 2nd
        call exits the loop.'''
        client: mock.MagicMock = _mock_client()

        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError()

        with mock.patch(
            'asyncio.sleep', side_effect=fake_sleep,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        client.post.assert_not_called()
        self.assertGreaterEqual(len(sleep_calls), 1)
        self.assertEqual(sleep_calls[0], 5.0)

    # ------------------------------------------------------------------
    # Test 9: no priority, full batch from base
    # ------------------------------------------------------------------
    async def test_no_priority_full_batch_from_base(
        self,
    ) -> None:
        '''priority_dir empty, 3 base files,
        batch_size=10 → batch contains all 3 base files,
        all succeed → all marked via fm.mark_uploaded.'''
        b1 = self._make_base(
            'x1', 'UC_x1', age_secs=30,
        )
        b2 = self._make_base(
            'x2', 'UC_x2', age_secs=20,
        )
        b3 = self._make_base(
            'x3', 'UC_x3', age_secs=10,
        )

        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j9',
        )

        async def fake_fetch(
            job_id: str,
            exchange_url: str,
            cl: object,
        ) -> list[dict]:
            buf: bytes = (
                client.post.call_args
                .kwargs['files']['file'][1]
            )
            results_out: list[dict] = []
            for idx, line in enumerate(
                b for b in buf.split(b'\n') if b
            ):
                rec: dict = orjson.loads(line)
                results_out.append({
                    'platform_content_id': (
                        rec['channel_id']
                    ),
                    'record_index': idx,
                    'status': 'success',
                })
            return results_out

        async def fake_stream(
            job_id: str,
            exchange_url: str,
            cl: object,
            timeout: float,
        ) -> bool:
            return True

        with (
            mock.patch.object(
                yt_channel_scrape,
                'stream_bulk_job_progress',
                side_effect=fake_stream,
            ),
            mock.patch.object(
                yt_channel_scrape,
                'fetch_bulk_results',
                side_effect=fake_fetch,
            ),
            mock.patch(
                'asyncio.sleep',
                side_effect=[
                    None, asyncio.CancelledError(),
                ],
            ),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await yt_channel_scrape\
                    ._unified_bulk_upload_loop(
                    self.settings,
                    client,
                    self.fm,
                    _passing_validator(),
                    sleep_seconds=5.0,
                )

        uploaded_dir: Path = self.fm.uploaded_dir
        for b in (b1, b2, b3):
            self.assertTrue(
                (uploaded_dir / b.name).exists(),
                f'{b.name} not in uploaded_dir',
            )
        # POST called once with 3 lines
        client.post.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
