'''Unit tests for the priority-directory drain logic in
yt_channel_scrape.py.

Covers:
- Bare channel_id resolved successfully: handle queued,
  entry stays intact until cleanup.
- Bare channel_id unresolvable (resolve_channel_id returns
  None): entry renamed to <channel_id>.failed immediately.
- Channel scrape succeeds: cleanup pass deletes the priority
  entry.
- Channel scrape produces a .not_found sentinel: cleanup pass
  deletes the priority entry.
- Channel scrape fails entirely (no output file, no sentinel):
  cleanup pass renames entry to <channel_id>.failed.
- InconsistentIdentityError during bind: entry renamed to
  <channel_id>.failed immediately; no handle queued.
'''

import asyncio
import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


# ---------------------------------------------------------------------------
# Module loader (mirrors the pattern in other tool test files)
# ---------------------------------------------------------------------------

def _load_yt_channel_scrape() -> ModuleType:
    '''Load tools/yt_channel_scrape.py once per process.'''
    for _key in (
        'yt_channel_scrape',
        'tools.yt_channel_scrape',
    ):
        if _key in sys.modules:
            return sys.modules[_key]
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
    sys.modules['tools.yt_channel_scrape'] = module
    spec.loader.exec_module(module)
    return module


_MOD: ModuleType = _load_yt_channel_scrape()

# Convenience aliases
_VALID_ID: str = 'UCaaaaaaaaaaaaaaaaaaaaaa'  # 24 chars (UC + 22)
_HANDLE: str = 'mychannel'


def _make_settings(priority_dir: str) -> mock.MagicMock:
    s: mock.MagicMock = mock.MagicMock()
    s.channel_priority_directory_path = priority_dir
    return s


def _make_identity_store() -> mock.AsyncMock:
    store: mock.AsyncMock = mock.AsyncMock()
    store.bind = mock.AsyncMock()
    store.handle_map.get = mock.AsyncMock(return_value=None)
    return store


def _make_queue() -> mock.AsyncMock:
    queue: mock.AsyncMock = mock.AsyncMock()
    queue.enqueue_scheduled = mock.AsyncMock()
    return queue


# ---------------------------------------------------------------------------
# _drain_priority_directory tests
# ---------------------------------------------------------------------------

class TestDrainPriorityDirectoryResolveSuccess(
    unittest.IsolatedAsyncioTestCase,
):
    '''A bare channel_id that resolves successfully is added
    to the returned dict; the priority file is NOT deleted
    here (cleanup happens later).'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_resolved_channel_id_returned(self) -> None:
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=_HANDLE),
        ):
            result: dict[str, Path] = (
                await _MOD._drain_priority_directory(
                    settings, store,
                )
            )

        # Legacy drain now returns a {channel_id: path} map.
        self.assertIn(_VALID_ID, result)
        self.assertEqual(result[_VALID_ID], self.entry)
        # File not renamed yet
        self.assertTrue(self.entry.exists())
        # .failed must not exist
        failed: Path = self.priority_dir / (
            _VALID_ID + '.failed'
        )
        self.assertFalse(failed.exists())

    async def test_bind_called_with_id_and_handle(
        self,
    ) -> None:
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=_HANDLE),
        ):
            await _MOD._drain_priority_directory(
                settings, store,
            )

        store.bind.assert_awaited_once_with(
            _VALID_ID, _HANDLE,
        )


class TestDrainPriorityDirectoryUnresolvable(
    unittest.IsolatedAsyncioTestCase,
):
    '''When resolve_channel_id returns None the channel is still
    scrapable by channel_id, so it is enqueued (not renamed
    .failed); the handle is simply absent.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_unresolvable_id_enqueued_not_failed(self) -> None:
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=None),
        ):
            result: dict[str, Path] = (
                await _MOD._drain_priority_directory(
                    settings, store,
                )
            )

        # channel_id alone is enough to scrape.
        self.assertEqual(result, {_VALID_ID: self.entry})
        self.assertTrue(self.entry.exists())
        failed: Path = self.priority_dir / (
            _VALID_ID + '.failed'
        )
        self.assertFalse(failed.exists())

    async def test_bind_not_called(self) -> None:
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=None),
        ):
            await _MOD._drain_priority_directory(
                settings, store,
            )

        store.bind.assert_not_awaited()


class TestDrainPriorityDirectoryInconsistentBind(
    unittest.IsolatedAsyncioTestCase,
):
    '''An InconsistentIdentityError during bind is best-effort: the
    channel is still enqueued by channel_id (not renamed .failed).'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_inconsistent_bind_enqueued_not_failed(
        self,
    ) -> None:
        from scrape_exchange.youtube.channel_identity import (
            InconsistentIdentityError,
        )
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()
        store.bind.side_effect = InconsistentIdentityError(
            'conflict',
        )

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=_HANDLE),
        ):
            result: dict[str, Path] = (
                await _MOD._drain_priority_directory(
                    settings, store,
                )
            )

        self.assertEqual(result, {_VALID_ID: self.entry})
        self.assertTrue(self.entry.exists())
        failed: Path = self.priority_dir / (
            _VALID_ID + '.failed'
        )
        self.assertFalse(failed.exists())


class TestDrainPriorityDirectorySkipsInvalidNames(
    unittest.IsolatedAsyncioTestCase,
):
    '''Invalid filename entries are renamed to .failed.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_non_channel_id_renamed_failed(self) -> None:
        invalid: Path = self.priority_dir / 'bad"handle'
        invalid.write_text('')
        (
            self.priority_dir / (_VALID_ID + '.failed')
        ).write_text('')

        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        store: mock.AsyncMock = _make_identity_store()

        with mock.patch.object(
            _MOD, 'resolve_channel_id',
            new=mock.AsyncMock(return_value=_HANDLE),
        ) as mock_resolve:
            result: dict[str, Path] = (
                await _MOD._drain_priority_directory(
                    settings, store,
                )
            )

        self.assertEqual(result, {})
        mock_resolve.assert_not_awaited()
        self.assertFalse(invalid.exists())
        self.assertTrue(
            self.priority_dir.joinpath('bad"handle.failed').exists(),
        )

    async def test_nonexistent_dir_returns_empty(
        self,
    ) -> None:
        settings: mock.MagicMock = _make_settings(
            str(self.priority_dir / 'does_not_exist'),
        )
        store: mock.AsyncMock = _make_identity_store()

        result: dict[str, Path] = (
            await _MOD._drain_priority_directory(
                settings, store,
            )
        )
        self.assertEqual(result, {})


class TestDrainPriorityDirectoryRedisHandles(
    unittest.IsolatedAsyncioTestCase,
):
    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.settings: mock.MagicMock = _make_settings(
            str(self.priority_dir),
        )
        self.store: mock.AsyncMock = _make_identity_store()
        self.queue: mock.AsyncMock = _make_queue()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_at_handle_map_hit_is_processed(
        self,
    ) -> None:
        entry: Path = self.priority_dir / '@veritasium'
        entry.write_text('')
        self.store.handle_map.get.return_value = _VALID_ID

        with mock.patch.object(
            _MOD, 'resolve_channel_handle',
            new=mock.AsyncMock(),
        ) as resolve:
            result: dict[str, Path] = (
                await _MOD._drain_priority_directory(
                    self.settings, self.store,
                    channel_queue=self.queue,
                )
            )

        self.assertEqual(result, {})
        self.store.handle_map.get.assert_awaited_once_with(
            'veritasium',
        )
        resolve.assert_not_awaited()
        self.store.bind.assert_awaited_once_with(
            _VALID_ID, 'veritasium',
        )
        self.queue.enqueue_scheduled.assert_awaited_once_with(
            _VALID_ID,
            source='priority_directory',
            priority=True,
        )
        self.assertFalse(entry.exists())
        self.assertTrue(
            (self.priority_dir / '@veritasium.processed').exists()
        )

    async def test_bare_handle_innertube_hit_is_processed(
        self,
    ) -> None:
        entry: Path = self.priority_dir / 'veritasium'
        entry.write_text('')

        with mock.patch.object(
            _MOD, 'resolve_channel_handle',
            new=mock.AsyncMock(return_value=_VALID_ID),
        ) as resolve:
            await _MOD._drain_priority_directory(
                self.settings, self.store,
                channel_queue=self.queue,
            )

        resolve.assert_awaited_once_with('veritasium')
        self.store.bind.assert_awaited_once_with(
            _VALID_ID, 'veritasium',
        )
        self.assertFalse(entry.exists())
        self.assertTrue(
            (self.priority_dir / 'veritasium.processed').exists()
        )

    async def test_handle_resolution_miss_is_failed(
        self,
    ) -> None:
        entry: Path = self.priority_dir / '@veritasium'
        entry.write_text('')

        with mock.patch.object(
            _MOD, 'resolve_channel_handle',
            new=mock.AsyncMock(return_value=None),
        ):
            await _MOD._drain_priority_directory(
                self.settings, self.store,
                channel_queue=self.queue,
            )

        self.assertFalse(entry.exists())
        self.assertTrue(
            (self.priority_dir / '@veritasium.failed').exists()
        )
        self.queue.enqueue_scheduled.assert_not_awaited()

    async def test_processed_and_failed_entries_are_skipped(
        self,
    ) -> None:
        processed: Path = self.priority_dir / (
            'veritasium.processed'
        )
        failed: Path = self.priority_dir / 'veritasium.failed'
        processed.write_text('')
        failed.write_text('')

        with mock.patch.object(
            _MOD, 'resolve_channel_handle',
            new=mock.AsyncMock(),
        ) as resolve:
            await _MOD._drain_priority_directory(
                self.settings, self.store,
                channel_queue=self.queue,
            )

        resolve.assert_not_awaited()
        self.store.handle_map.get.assert_not_awaited()
        self.assertTrue(processed.exists())
        self.assertTrue(failed.exists())

    async def test_channel_id_and_handle_share_one_drain(
        self,
    ) -> None:
        channel_entry: Path = self.priority_dir / _VALID_ID
        handle_entry: Path = self.priority_dir / 'veritasium'
        channel_entry.write_text('')
        handle_entry.write_text('')
        second_id: str = 'UCbbbbbbbbbbbbbbbbbbbbbb'

        with (
            mock.patch.object(
                _MOD, 'resolve_channel_id',
                new=mock.AsyncMock(return_value=_HANDLE),
            ),
            mock.patch.object(
                _MOD, 'resolve_channel_handle',
                new=mock.AsyncMock(return_value=second_id),
            ),
        ):
            await _MOD._drain_priority_directory(
                self.settings, self.store,
                channel_queue=self.queue,
            )

        self.assertEqual(
            self.queue.enqueue_scheduled.await_count, 2,
        )
        self.assertTrue(
            (self.priority_dir / f'{_VALID_ID}.processed').exists()
        )
        self.assertTrue(
            (self.priority_dir / 'veritasium.processed').exists()
        )


class TestPriorityDrainLoop(unittest.IsolatedAsyncioTestCase):
    async def test_repeats_until_shutdown(self) -> None:
        shutdown: asyncio.Event = asyncio.Event()
        settings: mock.MagicMock = mock.MagicMock()
        settings.channel_priority_drain_interval_seconds = 0.01
        queue: mock.AsyncMock = _make_queue()
        store: mock.AsyncMock = _make_identity_store()

        calls: int = 0

        async def drain(*_args: object, **_kwargs: object) -> None:
            nonlocal calls
            calls += 1
            if calls >= 2:
                shutdown.set()

        mocked: mock.AsyncMock = mock.AsyncMock(side_effect=drain)
        with mock.patch.object(
            _MOD, '_drain_priority_directory', new=mocked,
        ):
            await _MOD._priority_drain_loop(
                queue, store, settings, shutdown,
            )

        self.assertGreaterEqual(mocked.await_count, 2)

    async def test_survives_one_drain_exception(self) -> None:
        shutdown: asyncio.Event = asyncio.Event()
        settings: mock.MagicMock = mock.MagicMock()
        settings.channel_priority_drain_interval_seconds = 0.01
        queue: mock.AsyncMock = _make_queue()
        store: mock.AsyncMock = _make_identity_store()

        calls: int = 0

        async def drain(*_args: object, **_kwargs: object) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError('boom')
            if calls >= 2:
                shutdown.set()

        mocked: mock.AsyncMock = mock.AsyncMock(
            side_effect=drain,
        )
        with mock.patch.object(
            _MOD, '_drain_priority_directory', new=mocked,
        ):
            await _MOD._priority_drain_loop(
                queue, store, settings, shutdown,
            )

        self.assertEqual(mocked.await_count, 2)


# ---------------------------------------------------------------------------
# _priority_post_cleanup tests
# ---------------------------------------------------------------------------

class TestPriorityPostCleanupSuccess(
    unittest.IsolatedAsyncioTestCase,
):
    '''When the scraped .json.br file exists in base_dir,
    the priority entry is deleted.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.base_dir: Path = Path(self.tmp) / 'data'
        self.base_dir.mkdir()
        self.uploaded_dir: Path = (
            self.base_dir / 'uploaded'
        )
        self.uploaded_dir.mkdir()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.priority_entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.priority_entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_fm(self) -> object:
        from scrape_exchange.file_management import (
            AssetFileManagement,
        )
        return AssetFileManagement(str(self.base_dir))

    def _priority_handles(self) -> dict[str, Path]:
        return {_HANDLE: self.priority_entry}

    async def test_scraped_in_base_dir_deletes_entry(
        self,
    ) -> None:
        prefix: str = _MOD.CHANNEL_FILE_PREFIX
        postfix: str = _MOD.CHANNEL_FILE_POSTFIX
        (
            self.base_dir
            / f'{prefix}{_HANDLE}{postfix}'
        ).write_text('')
        fm = self._make_fm()
        _MOD._priority_post_cleanup(
            self._priority_handles(), fm,
        )
        self.assertFalse(self.priority_entry.exists())

    async def test_scraped_in_uploaded_dir_deletes_entry(
        self,
    ) -> None:
        prefix: str = _MOD.CHANNEL_FILE_PREFIX
        postfix: str = _MOD.CHANNEL_FILE_POSTFIX
        (
            self.uploaded_dir
            / f'{prefix}{_HANDLE}{postfix}'
        ).write_text('')
        fm = self._make_fm()
        _MOD._priority_post_cleanup(
            self._priority_handles(), fm,
        )
        self.assertFalse(self.priority_entry.exists())


class TestPriorityPostCleanupNotFound(
    unittest.IsolatedAsyncioTestCase,
):
    '''.not_found sentinel in base_dir means the channel
    doesn't exist on YouTube — priority entry deleted.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.base_dir: Path = Path(self.tmp) / 'data'
        self.base_dir.mkdir()
        (self.base_dir / 'uploaded').mkdir()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.priority_entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.priority_entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_fm(self) -> object:
        from scrape_exchange.file_management import (
            AssetFileManagement,
        )
        return AssetFileManagement(str(self.base_dir))

    async def test_not_found_sentinel_deletes_entry(
        self,
    ) -> None:
        prefix: str = _MOD.CHANNEL_FILE_PREFIX
        (
            self.base_dir
            / f'{prefix}{_HANDLE}.not_found'
        ).write_text('')
        fm = self._make_fm()
        _MOD._priority_post_cleanup(
            {_HANDLE: self.priority_entry}, fm,
        )
        self.assertFalse(self.priority_entry.exists())

    async def test_unresolved_sentinel_deletes_entry(
        self,
    ) -> None:
        prefix: str = _MOD.CHANNEL_FILE_PREFIX
        (
            self.base_dir
            / f'{prefix}{_HANDLE}.unresolved'
        ).write_text('')
        fm = self._make_fm()
        _MOD._priority_post_cleanup(
            {_HANDLE: self.priority_entry}, fm,
        )
        self.assertFalse(self.priority_entry.exists())


class TestPriorityPostCleanupFailure(
    unittest.IsolatedAsyncioTestCase,
):
    '''When neither a scraped file nor any sentinel exists,
    the priority entry is renamed to <channel_id>.failed.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.base_dir: Path = Path(self.tmp) / 'data'
        self.base_dir.mkdir()
        (self.base_dir / 'uploaded').mkdir()
        self.priority_dir: Path = Path(self.tmp) / 'priority'
        self.priority_dir.mkdir()
        self.priority_entry: Path = (
            self.priority_dir / _VALID_ID
        )
        self.priority_entry.write_text('')

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_fm(self) -> object:
        from scrape_exchange.file_management import (
            AssetFileManagement,
        )
        return AssetFileManagement(str(self.base_dir))

    async def test_no_output_renames_to_failed(
        self,
    ) -> None:
        fm = self._make_fm()
        _MOD._priority_post_cleanup(
            {_HANDLE: self.priority_entry}, fm,
        )
        self.assertFalse(self.priority_entry.exists())
        failed: Path = self.priority_dir / (
            _VALID_ID + '.failed'
        )
        self.assertTrue(failed.exists())


if __name__ == '__main__':
    unittest.main()
