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

from scrape_exchange.bulk_upload import BulkResults


def _load_yt_channel_upload() -> ModuleType:
    '''Load tools/yt_channel_upload.py under the bare
    module name ``yt_channel_upload``. Checks sys.modules
    under both ``yt_channel_upload`` and
    ``tools.yt_channel_upload`` first so that repeated
    test runs within the same process do not trigger
    prometheus duplicate-registry errors regardless of
    which test file loaded the module first.'''
    for key in ('yt_channel_upload',
                'tools.yt_channel_upload'):
        if key in sys.modules:
            mod: ModuleType = sys.modules[key]
            sys.modules['yt_channel_upload'] = mod
            sys.modules['tools.yt_channel_upload'] = mod
            return mod
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = (
        repo_root / 'tools' / 'yt_channel_upload.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_channel_upload', module_path,
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_channel_upload'] = module
    sys.modules['tools.yt_channel_upload'] = module
    spec.loader.exec_module(module)
    return module


yt_channel_scrape: ModuleType = (
    _load_yt_channel_upload()
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
        settings.max_active_bulk_jobs = 10
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://exchange.test'
        settings.channel_priority_directory_path = str(
            self.priority_dir,
        )
        self.settings: mock.MagicMock = settings
        self.wait_mock: mock.AsyncMock = mock.AsyncMock(
            side_effect=asyncio.CancelledError(),
        )
        self.wait_patcher: mock._patch = mock.patch.object(
            yt_channel_scrape,
            '_wait_for_channel_changes',
            self.wait_mock,
        )
        self.wait_patcher.start()

    async def asyncTearDown(self) -> None:
        self.wait_patcher.stop()
        shutil.rmtree(str(self.tmp), ignore_errors=True)


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

    def _make_base_rss(
        self,
        handle: str,
        channel_id: str,
        age_secs: float = 10.0,
    ) -> Path:
        return _make_channel_file(
            self.base_dir,
            f'channel-rss-{handle}.json.br',
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

    async def test_empty_dir_waits_for_changes(
        self,
    ) -> None:
        '''base_dir empty → no POST, then the inotify
        watcher is awaited on base_dir alone.'''
        client: mock.MagicMock = _mock_client()

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
        self.wait_mock.assert_awaited_once_with(self.fm.base_dir)

    # ------------------------------------------------------------------
    # Test 9: no priority, full batch from base
    # ------------------------------------------------------------------
    async def test_no_priority_full_batch_from_base(
        self,
    ) -> None:
        '''3 base files, batch_size=10 → batch contains all 3,
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
        ) -> BulkResults:
            buf: bytes = (
                client.post.call_args
                .kwargs['files']['file'][1]
            )
            n: int = sum(
                1 for b in buf.split(b'\n') if b
            )
            # All records succeed: empty failures list, total ==
            # records submitted (ADR-0008 failures-only contract).
            return BulkResults(
                total=n, succeeded=n, failed=0, duplicate=0,
                failures=[],
            )

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

    # ------------------------------------------------------------------
    # Test 10: channel-rss files in base dir are uploaded
    # ------------------------------------------------------------------
    async def test_base_dir_channel_rss_files_are_uploaded(
        self,
    ) -> None:
        '''channel-rss-*.json.br files directly in
        YOUTUBE_CHANNEL_DATA_DIR are uploaded and moved via
        fm.mark_uploaded.'''
        rss = self._make_base_rss(
            'rssbase', 'UC_rssbase', age_secs=20,
        )
        regular = self._make_base(
            'regular', 'UC_regular', age_secs=10,
        )

        client: mock.MagicMock = _mock_client(
            status_code=201, job_id='j10',
        )

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
        ) -> BulkResults:
            buf: bytes = (
                client.post.call_args
                .kwargs['files']['file'][1]
            )
            n: int = sum(
                1 for b in buf.split(b'\n') if b
            )
            return BulkResults(
                total=n, succeeded=n, failed=0, duplicate=0,
                failures=[],
            )

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
        self.assertTrue((uploaded_dir / rss.name).exists())
        self.assertTrue((uploaded_dir / regular.name).exists())

        batch_buf: bytes = (
            client.post.call_args.kwargs['files']['file'][1]
        )
        posted_ids: set[str] = {
            orjson.loads(line)['channel_id']
            for line in batch_buf.split(b'\n') if line
        }
        self.assertEqual(
            posted_ids,
            {'UC_rssbase', 'UC_regular'},
        )


if __name__ == '__main__':
    unittest.main()
