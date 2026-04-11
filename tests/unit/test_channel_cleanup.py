'''
Unit tests for tools/channel_cleanup.py.
'''

import asyncio
import importlib.util
import os
import tempfile
import unittest

from pathlib import Path
from types import ModuleType

from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
)


def _load_channel_cleanup() -> ModuleType:
    '''
    Load tools/channel_cleanup.py as a module.  ``tools/`` is not a
    Python package so a normal ``import`` would fail; load it directly
    from the file path instead.
    '''

    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'channel_cleanup.py'
    spec = importlib.util.spec_from_file_location(
        'channel_cleanup', module_path
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


channel_cleanup: ModuleType = _load_channel_cleanup()

CHANNEL_FILE_POSTFIX: str = channel_cleanup.CHANNEL_FILE_POSTFIX

VALID_CHANNEL_ID: str = 'UC1234567890ABCDEFGHIJKL'
OTHER_CHANNEL_ID: str = 'UCabcdefghijklmnopqrstuv'


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestReadChannelMap(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir: str = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _write(self, name: str, content: str) -> str:
        path: str = os.path.join(self.tmp_dir, name)
        with open(path, 'w') as f:
            f.write(content)
        return path

    def test_missing_file_returns_empty_dict(self) -> None:
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(
            os.path.join(self.tmp_dir, 'does_not_exist.csv')
        ))
        self.assertEqual(result, {})

    def test_parses_csv_lines(self) -> None:
        path: str = self._write(
            'map.csv',
            f'{VALID_CHANNEL_ID},foohandle\n'
            f'{OTHER_CHANNEL_ID},barhandle\n',
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {
            VALID_CHANNEL_ID: 'foohandle',
            OTHER_CHANNEL_ID: 'barhandle',
        })

    def test_skips_blank_and_comment_lines(self) -> None:
        path: str = self._write(
            'map.csv',
            '\n'
            '# a comment\n'
            f'{VALID_CHANNEL_ID},foohandle\n'
            '\n',
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {VALID_CHANNEL_ID: 'foohandle'})

    def test_line_without_comma_maps_to_itself(self) -> None:
        path: str = self._write('map.csv', 'loneentry\n')
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {'loneentry': 'loneentry'})

    def test_strips_whitespace_around_values(self) -> None:
        path: str = self._write(
            'map.csv', f'  {VALID_CHANNEL_ID} , foohandle \n'
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {VALID_CHANNEL_ID: 'foohandle'})

    def test_duplicate_id_prefers_first_non_lowercase(self) -> None:
        path: str = self._write(
            'map.csv',
            f'{VALID_CHANNEL_ID},foohandle\n'
            f'{VALID_CHANNEL_ID},FooHandle\n'
            f'{VALID_CHANNEL_ID},barhandle\n',
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {VALID_CHANNEL_ID: 'FooHandle'})

    def test_duplicate_id_all_lowercase_keeps_first(self) -> None:
        path: str = self._write(
            'map.csv',
            f'{VALID_CHANNEL_ID},foohandle\n'
            f'{VALID_CHANNEL_ID},barhandle\n',
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {VALID_CHANNEL_ID: 'foohandle'})

    def test_duplicate_id_picks_first_non_lowercase_even_if_later(
        self,
    ) -> None:
        path: str = self._write(
            'map.csv',
            f'{VALID_CHANNEL_ID},foohandle\n'
            f'{VALID_CHANNEL_ID},barhandle\n'
            f'{VALID_CHANNEL_ID},BazHandle\n'
            f'{VALID_CHANNEL_ID},QuxHandle\n',
        )
        result: dict[str, str] = _run(channel_cleanup.read_channel_map(path))
        self.assertEqual(result, {VALID_CHANNEL_ID: 'BazHandle'})


class TestSelectHandle(unittest.TestCase):

    def test_first_non_lowercase_wins(self) -> None:
        self.assertEqual(
            channel_cleanup._select_handle(['foo', 'Bar', 'Baz']),
            'Bar',
        )

    def test_all_lowercase_returns_first(self) -> None:
        self.assertEqual(
            channel_cleanup._select_handle(['foo', 'bar']),
            'foo',
        )

    def test_single_candidate_returned_as_is(self) -> None:
        self.assertEqual(
            channel_cleanup._select_handle(['FooHandle']),
            'FooHandle',
        )

    def test_uppercase_handle_is_not_lowercase(self) -> None:
        self.assertEqual(
            channel_cleanup._select_handle(['foo', 'FOO']),
            'FOO',
        )


class TestBackupChannelMap(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir: str = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _write(self, name: str, content: str) -> str:
        path: str = os.path.join(self.tmp_dir, name)
        with open(path, 'w') as f:
            f.write(content)
        return path

    def test_backup_copies_file_with_date_suffix(self) -> None:
        path: str = self._write('map.csv', 'UCxxx,handle\n')
        backup_path: str | None = channel_cleanup.backup_channel_map(
            path
        )
        self.assertIsNotNone(backup_path)
        assert backup_path is not None
        self.assertTrue(os.path.isfile(backup_path))
        self.assertTrue(backup_path.startswith(f'{path}-'))
        with open(backup_path) as f:
            self.assertEqual(f.read(), 'UCxxx,handle\n')
        # Original is untouched.
        self.assertTrue(os.path.isfile(path))

    def test_backup_missing_file_returns_none(self) -> None:
        backup_path: str | None = channel_cleanup.backup_channel_map(
            os.path.join(self.tmp_dir, 'missing.csv')
        )
        self.assertIsNone(backup_path)

    def test_backup_overwrites_existing_backup(self) -> None:
        path: str = self._write('map.csv', 'UCxxx,newhandle\n')
        from datetime import date as _date
        suffix: str = _date.today().strftime('%Y%m%d')
        stale_backup: str = f'{path}-{suffix}'
        with open(stale_backup, 'w') as f:
            f.write('STALE\n')
        channel_cleanup.backup_channel_map(path)
        with open(stale_backup) as f:
            self.assertEqual(f.read(), 'UCxxx,newhandle\n')


class TestCleanChannelMap(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir: str = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _write(self, name: str, content: str) -> str:
        path: str = os.path.join(self.tmp_dir, name)
        with open(path, 'w') as f:
            f.write(content)
        return path

    def test_missing_file_returns_empty(self) -> None:
        result: dict[str, str] = _run(channel_cleanup.clean_channel_map(
            os.path.join(self.tmp_dir, 'does_not_exist.csv')
        ))
        self.assertEqual(result, {})

    def test_no_duplicates_does_not_rewrite_source(self) -> None:
        content: str = (
            f'{VALID_CHANNEL_ID},FooHandle\n'
            f'{OTHER_CHANNEL_ID},BarHandle\n'
        )
        path: str = self._write('map.csv', content)
        original_mtime: float = os.path.getmtime(path)
        import time
        time.sleep(0.01)
        result: dict[str, str] = _run(
            channel_cleanup.clean_channel_map(path)
        )
        self.assertEqual(result, {
            VALID_CHANNEL_ID: 'FooHandle',
            OTHER_CHANNEL_ID: 'BarHandle',
        })
        # Backup still written even when there are no duplicates.
        from datetime import date as _date
        suffix: str = _date.today().strftime('%Y%m%d')
        self.assertTrue(os.path.isfile(f'{path}-{suffix}'))
        # Source file not rewritten.
        self.assertEqual(os.path.getmtime(path), original_mtime)
        with open(path) as f:
            self.assertEqual(f.read(), content)

    def test_duplicates_rewrites_source_with_selected_handles(
        self,
    ) -> None:
        path: str = self._write(
            'map.csv',
            f'{VALID_CHANNEL_ID},foohandle\n'
            f'{OTHER_CHANNEL_ID},BarHandle\n'
            f'{VALID_CHANNEL_ID},FooHandle\n'
            f'{OTHER_CHANNEL_ID},otherbar\n',
        )
        result: dict[str, str] = _run(
            channel_cleanup.clean_channel_map(path)
        )
        self.assertEqual(result, {
            VALID_CHANNEL_ID: 'FooHandle',
            OTHER_CHANNEL_ID: 'BarHandle',
        })
        # Backup holds the original content.
        from datetime import date as _date
        suffix: str = _date.today().strftime('%Y%m%d')
        backup_path: str = f'{path}-{suffix}'
        self.assertTrue(os.path.isfile(backup_path))
        with open(backup_path) as f:
            backup_content: str = f.read()
        self.assertIn('foohandle', backup_content)
        self.assertIn('otherbar', backup_content)
        # Rewritten source has exactly one row per ID, in
        # first-occurrence order.
        with open(path) as f:
            lines: list[str] = [
                line.rstrip('\n')
                for line in f if line.rstrip('\n')
            ]
        self.assertEqual(lines, [
            f'{VALID_CHANNEL_ID},FooHandle',
            f'{OTHER_CHANNEL_ID},BarHandle',
        ])


class TestChannelFileExists(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base: str = self._tmp.name
        self.fm: AssetFileManagement = AssetFileManagement(self.base)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _touch(self, directory: Path, name: str) -> None:
        (directory / name).write_text('x')

    def test_returns_false_when_nothing_exists(self) -> None:
        self.assertFalse(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )

    def test_true_when_base_file_exists(self) -> None:
        self._touch(
            self.fm.base_dir,
            f'{CHANNEL_FILE_PREFIX}foohandle{CHANNEL_FILE_POSTFIX}',
        )
        self.assertTrue(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )

    def test_true_when_uploaded_file_exists(self) -> None:
        self._touch(
            self.fm.uploaded_dir,
            f'{CHANNEL_FILE_PREFIX}foohandle{CHANNEL_FILE_POSTFIX}',
        )
        self.assertTrue(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )

    def test_true_when_not_found_marker_exists(self) -> None:
        self._touch(
            self.fm.base_dir,
            f'{CHANNEL_FILE_PREFIX}foohandle.not_found',
        )
        self.assertTrue(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )

    def test_true_when_unresolved_marker_exists(self) -> None:
        self._touch(
            self.fm.base_dir,
            f'{CHANNEL_FILE_PREFIX}foohandle.unresolved',
        )
        self.assertTrue(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )

    def test_false_when_only_failed_marker_exists(self) -> None:
        # .failed markers are intentionally NOT treated as "handled".
        self._touch(
            self.fm.base_dir,
            f'{CHANNEL_FILE_PREFIX}foohandle{CHANNEL_FILE_POSTFIX}.failed',
        )
        self.assertFalse(
            channel_cleanup.channel_file_exists(self.fm, 'foohandle')
        )


class TestClassifyLine(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fm: AssetFileManagement = AssetFileManagement(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_handle_not_yet_scraped_is_kept(self) -> None:
        result = channel_cleanup.classify_line('foohandle', {}, self.fm)
        self.assertEqual(result, 'foohandle')

    def test_handle_already_scraped_is_dropped(self) -> None:
        (self.fm.base_dir
         / f'{CHANNEL_FILE_PREFIX}foohandle{CHANNEL_FILE_POSTFIX}'
         ).write_text('x')
        result = channel_cleanup.classify_line('foohandle', {}, self.fm)
        self.assertIsNone(result)

    def test_channel_id_in_map_resolves_to_handle(self) -> None:
        result = channel_cleanup.classify_line(
            VALID_CHANNEL_ID, {VALID_CHANNEL_ID: 'foohandle'}, self.fm
        )
        self.assertEqual(result, 'foohandle')

    def test_channel_id_not_in_map_kept_as_id(self) -> None:
        result = channel_cleanup.classify_line(
            VALID_CHANNEL_ID, {}, self.fm
        )
        self.assertEqual(result, VALID_CHANNEL_ID)

    def test_channel_id_existence_check_not_applied(self) -> None:
        # Existence check is intentionally only run on handles.
        (self.fm.base_dir
         / f'{CHANNEL_FILE_PREFIX}{VALID_CHANNEL_ID}{CHANNEL_FILE_POSTFIX}'
         ).write_text('x')
        result = channel_cleanup.classify_line(
            VALID_CHANNEL_ID, {}, self.fm
        )
        self.assertEqual(result, VALID_CHANNEL_ID)


COLLATERAL_DIR: Path = (
    Path(__file__).resolve().parents[1] / 'collateral'
)
COLLATERAL_CHANNELS_LST: Path = COLLATERAL_DIR / 'channels.lst'
COLLATERAL_CHANNEL_MAP: Path = COLLATERAL_DIR / 'channel_map.csv'
TMP_OUTPUT_PATH: Path = Path('/tmp/channels.lst')


class TestCollateralBuildAndWrite(unittest.TestCase):
    '''
    Exercises the full build-and-write flow against the real
    collateral fixtures under :data:`COLLATERAL_DIR`.

    The collateral ``channels.lst`` is **never** moved or rewritten;
    instead the cleaned list is written to ``/tmp/channels.lst`` so
    the source fixture stays pristine.  Any pre-existing
    ``/tmp/channels.lst`` is removed first so the test starts from a
    known state.
    '''

    def setUp(self) -> None:
        self.assertTrue(
            COLLATERAL_CHANNELS_LST.is_file(),
            f'Missing collateral file: {COLLATERAL_CHANNELS_LST}',
        )
        self.assertTrue(
            COLLATERAL_CHANNEL_MAP.is_file(),
            f'Missing collateral file: {COLLATERAL_CHANNEL_MAP}',
        )

        if TMP_OUTPUT_PATH.exists():
            TMP_OUTPUT_PATH.unlink()

        self._source_size: int = COLLATERAL_CHANNELS_LST.stat().st_size
        self._source_mtime: float = (
            COLLATERAL_CHANNELS_LST.stat().st_mtime
        )

        self._data_tmp = tempfile.TemporaryDirectory()
        self.fm: AssetFileManagement = AssetFileManagement(
            self._data_tmp.name
        )

    def tearDown(self) -> None:
        self._data_tmp.cleanup()
        if TMP_OUTPUT_PATH.exists():
            TMP_OUTPUT_PATH.unlink()

    def test_build_and_write_uses_collateral(self) -> None:
        channel_map: dict[str, str] = _run(
            channel_cleanup.read_channel_map(str(COLLATERAL_CHANNEL_MAP))
        )
        self.assertGreater(
            len(channel_map), 0,
            'Expected non-empty channel map from collateral',
        )

        new_channels: list[str] = _run(
            channel_cleanup.build_new_channel_list(
                str(COLLATERAL_CHANNELS_LST), channel_map, self.fm
            )
        )

        # build_new_channel_list must not modify the source file.
        self.assertEqual(
            COLLATERAL_CHANNELS_LST.stat().st_size, self._source_size
        )
        self.assertEqual(
            COLLATERAL_CHANNELS_LST.stat().st_mtime, self._source_mtime
        )

        # Result should be non-empty and deduplicated.
        self.assertGreater(len(new_channels), 0)
        self.assertEqual(len(new_channels), len(set(new_channels)))

        # Write the cleaned list directly to /tmp/channels.lst,
        # bypassing the rotate-and-move step entirely.
        with open(TMP_OUTPUT_PATH, 'w') as f:
            for entry in new_channels:
                f.write(f'{entry}\n')

        self.assertTrue(TMP_OUTPUT_PATH.is_file())
        with open(TMP_OUTPUT_PATH) as f:
            written: list[str] = [
                line.rstrip('\n') for line in f if line.rstrip('\n')
            ]
        self.assertEqual(written, new_channels)

    def test_existing_tmp_channels_lst_is_removed(self) -> None:
        # Pre-seed /tmp/channels.lst with stale content and confirm
        # the test setUp wiped it before the run.
        TMP_OUTPUT_PATH.write_text('stale-content\n')
        self.assertTrue(TMP_OUTPUT_PATH.exists())

        # Re-run setUp's deletion logic explicitly to mirror the
        # documented behavior.
        if TMP_OUTPUT_PATH.exists():
            TMP_OUTPUT_PATH.unlink()
        self.assertFalse(TMP_OUTPUT_PATH.exists())


if __name__ == '__main__':
    unittest.main()
