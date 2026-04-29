'''
Unit tests for scrape_exchange.file_management.AssetFileManagement.
'''

import os
import unittest
import tempfile

from scrape_exchange.file_management import (
    AssetFileManagement,
    DEFAULT_PREFIX_RANKINGS,
    MARKER_SUFFIXES,
)


class TestAssetFileManagementInit(unittest.TestCase):

    def test_uploaded_dir_created(self):
        with tempfile.TemporaryDirectory() as base:
            AssetFileManagement(base)
            self.assertTrue(os.path.isdir(os.path.join(base, 'uploaded')))

    def test_uploaded_dir_already_exists(self):
        with tempfile.TemporaryDirectory() as base:
            os.makedirs(os.path.join(base, 'uploaded'))
            AssetFileManagement(base)  # must not raise
            self.assertTrue(os.path.isdir(os.path.join(base, 'uploaded')))

    def test_default_prefix_rankings_used_when_none_given(self):
        with tempfile.TemporaryDirectory() as base:
            fm = AssetFileManagement(base)
            self.assertEqual(fm.prefix_rankings, DEFAULT_PREFIX_RANKINGS)

    def test_custom_prefix_rankings_stored(self):
        custom = {'rss': ['rss-min-', 'rss-full-']}
        with tempfile.TemporaryDirectory() as base:
            fm = AssetFileManagement(base, prefix_rankings=custom)
            self.assertEqual(fm.prefix_rankings, custom)


class TestSyncWithUploaded(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.fm = AssetFileManagement(self.base)
        self.uploaded = os.path.join(self.base, 'uploaded')

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, directory: str, filename: str, mtime: float) -> str:
        path = os.path.join(directory, filename)
        with open(path, 'w') as f:
            f.write('x')
        os.utime(path, (mtime, mtime))
        return path

    def test_returns_false_when_base_file_missing(self):
        result = self.fm.sync_with_uploaded('video-dlp-ABC.json.br')
        self.assertFalse(result)

    def test_returns_false_when_uploaded_file_missing(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        result = self.fm.sync_with_uploaded('video-dlp-ABC.json.br')
        self.assertFalse(result)
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_deletes_base_when_uploaded_is_newer(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 2000.0)
        result = self.fm.sync_with_uploaded('video-dlp-ABC.json.br')
        self.assertTrue(result)
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_keeps_base_when_uploaded_is_older(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        result = self.fm.sync_with_uploaded('video-dlp-ABC.json.br')
        self.assertFalse(result)
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_keeps_base_when_mtimes_are_equal(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        result = self.fm.sync_with_uploaded('video-dlp-ABC.json.br')
        self.assertFalse(result)
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))


class TestCleanupLowerRanked(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.fm = AssetFileManagement(self.base)
        self.uploaded = os.path.join(self.base, 'uploaded')

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, directory: str, filename: str, mtime: float) -> str:
        path = os.path.join(directory, filename)
        with open(path, 'w') as f:
            f.write('x')
        os.utime(path, (mtime, mtime))
        return path

    def test_returns_empty_for_unknown_prefix(self):
        self._write(self.base, 'unknown-ABC.json.br', 1000.0)
        deleted = self.fm.cleanup_lower_ranked('unknown-ABC.json.br')
        self.assertEqual(deleted, [])

    def test_no_op_when_only_lower_ranked_exists(self):
        # Only video-min exists; no video-dlp to supersede it.
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        deleted = self.fm.cleanup_lower_ranked('video-min-ABC.json.br')
        self.assertEqual(deleted, [])
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    def test_deletes_lower_ranked_in_base_when_higher_ranked_is_newer(self):
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        deleted = self.fm.cleanup_lower_ranked('video-dlp-ABC.json.br')
        self.assertEqual(len(deleted), 1)
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_deletes_lower_ranked_in_uploaded_when_higher_ranked_is_newer(
        self
    ):
        self._write(self.uploaded, 'video-min-ABC.json.br', 1000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        deleted = self.fm.cleanup_lower_ranked('video-dlp-ABC.json.br')
        self.assertEqual(len(deleted), 1)
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'video-min-ABC.json.br')
        ))

    def test_keeps_lower_ranked_when_higher_ranked_is_older(self):
        self._write(self.base, 'video-min-ABC.json.br', 2000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        deleted = self.fm.cleanup_lower_ranked('video-min-ABC.json.br')
        self.assertEqual(deleted, [])
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    def test_keeps_lower_ranked_when_mtime_equal_to_higher_ranked(self):
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        deleted = self.fm.cleanup_lower_ranked('video-min-ABC.json.br')
        self.assertEqual(deleted, [])
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    def test_deletes_multiple_lower_ranked_files_across_dirs(self):
        # video-min in both base and uploaded, video-dlp newer in base
        self._write(self.base, 'video-min-ABC.json.br', 500.0)
        self._write(self.uploaded, 'video-min-ABC.json.br', 800.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        deleted = self.fm.cleanup_lower_ranked('video-dlp-ABC.json.br')
        self.assertEqual(len(deleted), 2)
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'video-min-ABC.json.br')
        ))
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_does_not_delete_higher_ranked_files(self):
        # Even when called with the lower-ranked filename, higher-ranked
        # must survive.
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        self.fm.cleanup_lower_ranked('video-min-ABC.json.br')
        # video-min should be deleted since video-dlp is newer
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))
        # video-dlp must NOT be deleted
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))

    def test_custom_three_tier_ranking(self):
        custom = {'item': ['item-raw-', 'item-mid-', 'item-full-']}
        fm = AssetFileManagement(self.base, prefix_rankings=custom)

        self._write(self.base, 'item-raw-X.json', 100.0)
        self._write(self.base, 'item-mid-X.json', 200.0)
        self._write(self.base, 'item-full-X.json', 300.0)

        deleted = fm.cleanup_lower_ranked('item-full-X.json')
        self.assertEqual(len(deleted), 2)
        self.assertFalse(
            os.path.exists(os.path.join(self.base, 'item-raw-X.json'))
        )
        self.assertFalse(
            os.path.exists(os.path.join(self.base, 'item-mid-X.json'))
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.base, 'item-full-X.json'))
        )

    def test_different_identifiers_not_affected(self):
        # Files for a different video ID must never be touched.
        self._write(self.base, 'video-min-AAA.json.br', 1000.0)
        self._write(self.base, 'video-dlp-BBB.json.br', 2000.0)
        deleted = self.fm.cleanup_lower_ranked('video-dlp-BBB.json.br')
        self.assertEqual(deleted, [])
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-AAA.json.br')
        ))


class TestReadWriteFile(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_raw(self, directory: str, filename: str, mtime: float) -> str:
        '''Write a brotli-compressed placeholder with a specific mtime.'''
        import brotli as _brotli
        import orjson as _orjson
        path = os.path.join(directory, filename)
        data = _brotli.compress(
            _orjson.dumps({'placeholder': True}), quality=1
        )
        with open(path, 'wb') as f:
            f.write(data)
        os.utime(path, (mtime, mtime))
        return path

    async def test_write_then_read_roundtrip(self):
        data = {'video_id': 'ABC123', 'title': 'Test video', 'views': 42}
        await self.fm.write_file('video-dlp-ABC123.json.br', data)
        result = await self.fm.read_file('video-dlp-ABC123.json.br')
        self.assertEqual(result, data)

    async def test_write_creates_file_in_base_dir(self):
        await self.fm.write_file('video-min-XYZ.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-XYZ.json.br')
        ))

    async def test_write_overwrites_existing_file(self):
        await self.fm.write_file('video-dlp-XYZ.json.br', {'v': 1})
        await self.fm.write_file('video-dlp-XYZ.json.br', {'v': 2})
        result = await self.fm.read_file('video-dlp-XYZ.json.br')
        self.assertEqual(result['v'], 2)

    async def test_written_file_is_brotli_compressed(self):
        import aiofiles
        import brotli
        await self.fm.write_file('video-dlp-XYZ.json.br', {'k': 'v'})
        async with aiofiles.open(
            os.path.join(self.base, 'video-dlp-XYZ.json.br'), 'rb'
        ) as f:
            raw = await f.read()
        self.assertIn(b'"k"', brotli.decompress(raw))

    async def test_read_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            await self.fm.read_file('nonexistent.json.br')

    async def test_read_preserves_nested_structures(self):
        data = {'tags': ['a', 'b'], 'meta': {'count': 3, 'flag': True}}
        await self.fm.write_file('video-dlp-NEST.json.br', data)
        self.assertEqual(
            await self.fm.read_file('video-dlp-NEST.json.br'), data
        )

    # -- post-write cleanup ---------------------------------------------------

    async def test_write_deletes_older_lower_ranked_in_base(self):
        self._write_raw(self.base, 'video-min-ABC.json.br', 1000.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    async def test_write_deletes_older_lower_ranked_in_uploaded(self):
        self._write_raw(self.uploaded, 'video-min-ABC.json.br', 1000.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'video-min-ABC.json.br')
        ))

    async def test_write_deletes_older_equal_ranked_in_uploaded(self):
        self._write_raw(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'video-dlp-ABC.json.br')
        ))

    async def test_write_keeps_newer_equal_ranked_in_uploaded(self):
        # uploaded copy is newer than what we are about to write — keep it
        self._write_raw(self.uploaded, 'video-dlp-ABC.json.br', 9_999_999_999.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.uploaded, 'video-dlp-ABC.json.br')
        ))

    async def test_write_keeps_newer_lower_ranked_file(self):
        # Lower-ranked file is newer than what we write — must not be deleted.
        self._write_raw(self.base, 'video-min-ABC.json.br', 9_999_999_999.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    async def test_write_single_prefix_group_cleans_uploaded_copy(self):
        # 'channel' is a single-prefix group: writing channel-foo.json.br
        # should still clean up an older uploaded copy of the same filename.
        self._write_raw(self.uploaded, 'channel-foo.json.br', 1000.0)
        await self.fm.write_file('channel-foo.json.br', {'name': 'foo'})
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'channel-foo.json.br')
        ))

    async def test_write_truly_unknown_prefix_cleans_uploaded_copy(self):
        # Filenames whose prefix matches no registered group must still have
        # their equal-priority uploaded copy cleaned up.
        self._write_raw(self.uploaded, 'mystery-zzz.json.br', 1000.0)
        await self.fm.write_file('mystery-zzz.json.br', {'k': 1})
        self.assertFalse(os.path.exists(
            os.path.join(self.uploaded, 'mystery-zzz.json.br')
        ))

    async def test_write_does_not_delete_different_identifier(self):
        self._write_raw(self.base, 'video-min-OTHER.json.br', 1000.0)
        await self.fm.write_file('video-dlp-ABC.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-OTHER.json.br')
        ))


class TestIsMarker(unittest.TestCase):

    def test_recognizes_all_marker_suffixes(self):
        for suffix in MARKER_SUFFIXES:
            self.assertTrue(
                AssetFileManagement.is_marker(f'channel-foo{suffix}'),
                f'expected {suffix} to be recognized as a marker',
            )

    def test_data_files_are_not_markers(self):
        self.assertFalse(AssetFileManagement.is_marker('channel-foo.json.br'))
        self.assertFalse(AssetFileManagement.is_marker('video-dlp-ABC.json.br'))
        self.assertFalse(AssetFileManagement.is_marker('something-else'))

    def test_filename_with_marker_substring_but_wrong_suffix(self):
        # The marker suffix must be at the end of the filename.
        self.assertFalse(
            AssetFileManagement.is_marker('channel-foo.failed.json.br')
        )


class TestMarkerWriteBehavior(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    def _write_raw(self, directory: str, filename: str, mtime: float) -> str:
        import brotli as _brotli
        import orjson as _orjson
        path = os.path.join(directory, filename)
        data = _brotli.compress(
            _orjson.dumps({'placeholder': True}), quality=1
        )
        with open(path, 'wb') as f:
            f.write(data)
        os.utime(path, (mtime, mtime))
        return path

    async def test_writing_marker_does_not_delete_data_file(self):
        # An older real data file must survive writing a newer marker.
        self._write_raw(self.base, 'channel-foo.json.br', 1000.0)
        await self.fm.write_file(
            'channel-foo.not_found', {'placeholder': True}
        )
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'channel-foo.json.br')
        ))

    async def test_writing_marker_does_not_delete_uploaded_copy(self):
        self._write_raw(self.uploaded, 'channel-foo.json.br', 1000.0)
        await self.fm.write_file(
            'channel-foo.failed', {'placeholder': True}
        )
        self.assertTrue(os.path.exists(
            os.path.join(self.uploaded, 'channel-foo.json.br')
        ))

    async def test_writing_lower_ranked_keeps_higher_ranked(self):
        # The crucial invariant: writing video-min must NEVER delete the
        # corresponding video-dlp file, regardless of mtimes.
        self._write_raw(self.base, 'video-dlp-ABC.json.br', 1000.0)
        await self.fm.write_file('video-min-ABC.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-dlp-ABC.json.br')
        ))
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br')
        ))

    async def test_writing_lower_ranked_keeps_higher_ranked_in_uploaded(
        self
    ):
        self._write_raw(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        await self.fm.write_file('video-min-ABC.json.br', {'x': 1})
        self.assertTrue(os.path.exists(
            os.path.join(self.uploaded, 'video-dlp-ABC.json.br')
        ))


class TestMarkerHelpers(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    async def test_mark_not_found_creates_empty_marker(self):
        path = await self.fm.mark_not_found('channel-foo')
        self.assertTrue(os.path.exists(path))
        self.assertEqual(
            str(path),
            os.path.join(self.base, 'channel-foo.not_found'),
        )
        with open(path) as f:
            self.assertEqual(f.read(), '')

    async def test_mark_not_found_writes_content(self):
        path = await self.fm.mark_not_found('channel-foo', content='foo\n')
        with open(path) as f:
            self.assertEqual(f.read(), 'foo\n')

    async def test_mark_unresolved_creates_marker(self):
        path = await self.fm.mark_unresolved(
            'channel-UCxyz', content='UCxyz\n'
        )
        self.assertEqual(
            str(path),
            os.path.join(self.base, 'channel-UCxyz.unresolved'),
        )
        with open(path) as f:
            self.assertEqual(f.read(), 'UCxyz\n')

    async def test_mark_not_found_does_not_delete_data_file(self):
        # Even if a stale data file exists for the same identifier, the
        # marker write must not remove it.
        with open(os.path.join(self.base, 'channel-foo.json.br'), 'w') as f:
            f.write('x')
        await self.fm.mark_not_found('channel-foo')
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'channel-foo.json.br')
        ))

    async def test_mark_uploaded_moves_file(self):
        src = os.path.join(self.base, 'channel-foo.json.br')
        with open(src, 'w') as f:
            f.write('x')
        dst = await self.fm.mark_uploaded('channel-foo.json.br')
        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(dst))
        self.assertEqual(
            str(dst),
            os.path.join(self.uploaded, 'channel-foo.json.br'),
        )

    async def test_mark_uploaded_missing_source_raises(self):
        with self.assertRaises((FileNotFoundError, OSError)):
            await self.fm.mark_uploaded('does-not-exist.json.br')

    async def test_mark_failed_renames_with_suffix(self):
        src = os.path.join(self.base, 'channel-foo.json.br')
        with open(src, 'w') as f:
            f.write('x')
        new_name = await self.fm.mark_failed('channel-foo.json.br')
        self.assertEqual(new_name, 'channel-foo.json.br.failed')
        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'channel-foo.json.br.failed')
        ))

    async def test_touch_marker_rejects_non_marker_filename(self):
        with self.assertRaises(ValueError):
            await self.fm._touch_marker('channel-foo.json.br', None)

    async def test_mark_unavailable_renames_with_suffix(self):
        src = os.path.join(self.base, 'video-min-ABC.json.br')
        with open(src, 'w') as f:
            f.write('x')
        new_name = await self.fm.mark_unavailable('video-min-ABC.json.br')
        self.assertEqual(new_name, 'video-min-ABC.json.br.unavailable')
        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(
            os.path.join(self.base, 'video-min-ABC.json.br.unavailable')
        ))

    async def test_mark_unavailable_missing_source_raises(self):
        with self.assertRaises((FileNotFoundError, OSError)):
            await self.fm.mark_unavailable('does-not-exist.json.br')

    async def test_delete_removes_file_in_base(self):
        path = os.path.join(self.base, 'channel-foo.json.br')
        with open(path, 'w') as f:
            f.write('x')
        await self.fm.delete('channel-foo.json.br')
        self.assertFalse(os.path.exists(path))

    async def test_delete_missing_raises_by_default(self):
        with self.assertRaises(FileNotFoundError):
            await self.fm.delete('does-not-exist.json.br')

    async def test_delete_fail_ok_false_suppresses_missing(self):
        with self.assertLogs(
            'scrape_exchange.file_management', level='WARNING',
        ):
            await self.fm.delete(
                'does-not-exist.json.br', fail_ok=False
            )  # must not raise

    async def test_delete_fail_ok_false_suppresses_arbitrary_oserror(self):
        # An OSError other than FileNotFoundError must also be swallowed.
        import unittest.mock as mock
        with mock.patch(
            'aiofiles.os.remove',
            side_effect=PermissionError('nope'),
        ):
            with self.assertLogs(
                'scrape_exchange.file_management', level='WARNING',
            ):
                await self.fm.delete(
                    'channel-foo.json.br', fail_ok=False
                )  # must not raise

    async def test_delete_fail_ok_true_propagates_oserror(self):
        import unittest.mock as mock
        with mock.patch(
            'aiofiles.os.remove',
            side_effect=PermissionError('nope'),
        ):
            with self.assertRaises(PermissionError):
                await self.fm.delete(
                    'channel-foo.json.br', fail_ok=True
                )

    async def test_delete_does_not_touch_uploaded_dir(self):
        # delete() only operates on base_dir; an uploaded copy must survive.
        uploaded_path = os.path.join(self.uploaded, 'channel-foo.json.br')
        base_path = os.path.join(self.base, 'channel-foo.json.br')
        with open(uploaded_path, 'w') as f:
            f.write('x')
        with open(base_path, 'w') as f:
            f.write('x')
        await self.fm.delete('channel-foo.json.br')
        self.assertFalse(os.path.exists(
            os.path.join(self.base, 'channel-foo.json.br')
        ))
        self.assertTrue(os.path.exists(
            os.path.join(self.uploaded, 'channel-foo.json.br')
        ))


class TestIsSuperseded(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, directory: str, filename: str, mtime: float) -> str:
        path = os.path.join(directory, filename)
        with open(path, 'w') as f:
            f.write('x')
        os.utime(path, (mtime, mtime))
        return path

    def test_returns_false_when_base_file_missing(self):
        self.assertFalse(self.fm.is_superseded('video-dlp-ABC.json.br'))

    def test_returns_false_when_no_uploaded_variant(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self.assertFalse(self.fm.is_superseded('video-dlp-ABC.json.br'))

    # Same-name uploaded copy --------------------------------------------

    def test_same_name_uploaded_newer_returns_true(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 2000.0)
        self.assertTrue(self.fm.is_superseded('video-dlp-ABC.json.br'))

    def test_same_name_uploaded_equal_mtime_returns_true(self):
        # >= semantics: ties go to the uploaded copy.
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        self.assertTrue(self.fm.is_superseded('video-dlp-ABC.json.br'))

    def test_same_name_uploaded_older_returns_false(self):
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        self.assertFalse(self.fm.is_superseded('video-dlp-ABC.json.br'))

    # Higher-ranked variant in uploaded ----------------------------------

    def test_higher_ranked_uploaded_newer_returns_true(self):
        # video-min-X (lower) in base, video-dlp-X (higher) in uploaded.
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 2000.0)
        self.assertTrue(self.fm.is_superseded('video-min-ABC.json.br'))

    def test_higher_ranked_uploaded_equal_mtime_returns_true(self):
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        self.assertTrue(self.fm.is_superseded('video-min-ABC.json.br'))

    def test_higher_ranked_uploaded_older_returns_false(self):
        # The min file was refreshed (e.g. by RSS) after the dlp upload —
        # not superseded, the local data is fresher.
        self._write(self.base, 'video-min-ABC.json.br', 2000.0)
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1000.0)
        self.assertFalse(self.fm.is_superseded('video-min-ABC.json.br'))

    # Higher-ranked variant in base (NOT considered) ---------------------

    def test_higher_ranked_in_base_does_not_count(self):
        # is_superseded() only checks the uploaded directory for higher
        # variants — base→base supersession is cleanup_lower_ranked's job.
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.base, 'video-dlp-ABC.json.br', 2000.0)
        self.assertFalse(self.fm.is_superseded('video-min-ABC.json.br'))

    # Highest-ranked file --------------------------------------------------

    def test_highest_ranked_file_only_checks_same_name(self):
        # video-dlp-X has no higher rank in the 'video' group.
        self._write(self.base, 'video-dlp-ABC.json.br', 1000.0)
        self.assertFalse(self.fm.is_superseded('video-dlp-ABC.json.br'))
        # And a same-name uploaded copy still counts.
        self._write(self.uploaded, 'video-dlp-ABC.json.br', 1500.0)
        self.assertTrue(self.fm.is_superseded('video-dlp-ABC.json.br'))

    # Different identifiers ------------------------------------------------

    def test_different_identifier_does_not_count(self):
        self._write(self.base, 'video-min-ABC.json.br', 1000.0)
        self._write(self.uploaded, 'video-dlp-OTHER.json.br', 2000.0)
        self.assertFalse(self.fm.is_superseded('video-min-ABC.json.br'))

    # Unknown prefix -------------------------------------------------------

    def test_unknown_prefix_only_checks_same_name(self):
        self._write(self.base, 'mystery-ABC.json.br', 1000.0)
        self.assertFalse(self.fm.is_superseded('mystery-ABC.json.br'))
        self._write(self.uploaded, 'mystery-ABC.json.br', 1500.0)
        self.assertTrue(self.fm.is_superseded('mystery-ABC.json.br'))


class TestListHelpers(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    def _touch(self, directory: str, name: str) -> None:
        with open(os.path.join(directory, name), 'w') as f:
            f.write('x')

    def test_list_base_returns_all_files_when_unfiltered(self):
        self._touch(self.base, 'a.txt')
        self._touch(self.base, 'b.txt')
        self._touch(self.uploaded, 'c.txt')
        self.assertEqual(set(self.fm.list_base()), {'a.txt', 'b.txt'})

    def test_list_base_filters_by_prefix(self):
        self._touch(self.base, 'channel-foo.json.br')
        self._touch(self.base, 'video-min-bar.json.br')
        self.assertEqual(
            self.fm.list_base(prefix='channel-'),
            ['channel-foo.json.br'],
        )

    def test_list_base_filters_by_suffix(self):
        self._touch(self.base, 'channel-foo.json.br')
        self._touch(self.base, 'channel-foo.not_found')
        self.assertEqual(
            self.fm.list_base(suffix='.json.br'),
            ['channel-foo.json.br'],
        )

    def test_list_base_filters_by_prefix_and_suffix(self):
        self._touch(self.base, 'channel-foo.json.br')
        self._touch(self.base, 'channel-foo.not_found')
        self._touch(self.base, 'video-min-bar.json.br')
        self.assertEqual(
            self.fm.list_base(prefix='channel-', suffix='.json.br'),
            ['channel-foo.json.br'],
        )

    def test_list_base_skips_subdirectories(self):
        # The uploaded subdirectory is itself a file system entry under
        # base_dir but should not appear in list_base().
        self._touch(self.base, 'a.txt')
        self.assertEqual(self.fm.list_base(), ['a.txt'])

    def test_list_uploaded_returns_only_uploaded_files(self):
        self._touch(self.base, 'a.txt')
        self._touch(self.uploaded, 'b.txt')
        self._touch(self.uploaded, 'c.txt')
        self.assertEqual(set(self.fm.list_uploaded()), {'b.txt', 'c.txt'})

    def test_list_uploaded_filters(self):
        self._touch(self.uploaded, 'channel-foo.json.br')
        self._touch(self.uploaded, 'video-min-bar.json.br')
        self.assertEqual(
            self.fm.list_uploaded(prefix='channel-'),
            ['channel-foo.json.br'],
        )

    def test_list_base_empty_when_no_match(self):
        self._touch(self.base, 'a.txt')
        self.assertEqual(self.fm.list_base(prefix='nope'), [])


class TestReadUploaded(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    async def test_read_uploaded_roundtrip(self):
        # Write to base, mark uploaded, then read from uploaded.
        data = {'channel_id': 'UCxyz', 'name': 'foo'}
        await self.fm.write_file('channel-foo.json.br', data)
        await self.fm.mark_uploaded('channel-foo.json.br')
        result = await self.fm.read_uploaded('channel-foo.json.br')
        self.assertEqual(result, data)

    async def test_read_uploaded_missing_raises(self):
        with self.assertRaises(FileNotFoundError):
            await self.fm.read_uploaded('nonexistent.json.br')


class TestDeleteUploaded(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.uploaded = os.path.join(self.base, 'uploaded')
        self.fm = AssetFileManagement(self.base)

    def tearDown(self):
        self._tmp.cleanup()

    async def test_delete_uploaded_removes_file_in_uploaded(self):
        path = os.path.join(self.uploaded, 'channel-foo.json.br')
        with open(path, 'w') as f:
            f.write('x')
        await self.fm.delete_uploaded('channel-foo.json.br')
        self.assertFalse(os.path.exists(path))

    async def test_delete_uploaded_does_not_touch_base(self):
        # Symmetric guarantee with delete(): only the targeted directory.
        base_path = os.path.join(self.base, 'channel-foo.json.br')
        uploaded_path = os.path.join(self.uploaded, 'channel-foo.json.br')
        with open(base_path, 'w') as f:
            f.write('x')
        with open(uploaded_path, 'w') as f:
            f.write('x')
        await self.fm.delete_uploaded('channel-foo.json.br')
        self.assertTrue(os.path.exists(base_path))
        self.assertFalse(os.path.exists(uploaded_path))

    async def test_delete_uploaded_missing_raises_by_default(self):
        with self.assertRaises(FileNotFoundError):
            await self.fm.delete_uploaded('does-not-exist.json.br')

    async def test_delete_uploaded_fail_ok_false_suppresses(self):
        with self.assertLogs(
            'scrape_exchange.file_management', level='WARNING',
        ):
            await self.fm.delete_uploaded(
                'does-not-exist.json.br', fail_ok=False
            )  # must not raise


import asyncio


class TestMarkInvalid(unittest.TestCase):

    def test_invalid_suffix_is_marker(self) -> None:
        self.assertTrue(
            AssetFileManagement.is_marker('foo.json.br.invalid'),
        )
        self.assertIn('.invalid', MARKER_SUFFIXES)

    def test_mark_invalid_renames_file(self) -> None:
        with tempfile.TemporaryDirectory() as base:
            fm: AssetFileManagement = AssetFileManagement(base)
            src: str = os.path.join(base, 'foo.json.br')
            with open(src, 'wb') as f:
                f.write(b'payload')
            new_name: str = asyncio.run(
                fm.mark_invalid('foo.json.br'),
            )
            self.assertEqual(new_name, 'foo.json.br.invalid')
            self.assertFalse(os.path.exists(src))
            self.assertTrue(
                os.path.exists(os.path.join(
                    base, 'foo.json.br.invalid',
                )),
            )


class TestAtomicWriteBytes(unittest.TestCase):
    '''
    The atomic-write helper is the mitigation for the corrupted-
    brotli-files-after-kill failure mode that the YouTube scrapers
    hit in production. Cover the happy path, the temp-file
    cleanup-on-error path, and the no-orphan-temp-file invariant.
    '''

    def test_writes_payload_and_no_temp_files_remain(self) -> None:
        from scrape_exchange.file_management import (
            atomic_write_bytes,
        )

        with tempfile.TemporaryDirectory() as base:
            target: str = os.path.join(base, 'foo.json.br')
            asyncio.run(atomic_write_bytes(target, b'payload'))
            self.assertEqual(
                open(target, 'rb').read(), b'payload',
            )
            # No ``foo.json.br.tmp.*`` orphan temp files remain
            # in the directory.
            entries: list[str] = os.listdir(base)
            self.assertEqual(entries, ['foo.json.br'])

    def test_overwrites_existing_file_atomically(self) -> None:
        from scrape_exchange.file_management import (
            atomic_write_bytes,
        )

        with tempfile.TemporaryDirectory() as base:
            target: str = os.path.join(base, 'foo.json.br')
            with open(target, 'wb') as f:
                f.write(b'old')
            asyncio.run(atomic_write_bytes(target, b'new'))
            self.assertEqual(
                open(target, 'rb').read(), b'new',
            )

    def test_temp_file_cleaned_up_on_rename_failure(
        self,
    ) -> None:
        '''When the rename step raises, the temp file must not
        be left behind.'''
        from unittest.mock import patch
        from scrape_exchange.file_management import (
            atomic_write_bytes,
        )
        import aiofiles.os as aios

        with tempfile.TemporaryDirectory() as base:
            target: str = os.path.join(base, 'foo.json.br')

            async def boom(*_args, **_kwargs):
                raise OSError('rename blew up')

            with patch.object(aios, 'rename', new=boom):
                with self.assertRaises(OSError):
                    asyncio.run(
                        atomic_write_bytes(target, b'payload'),
                    )
            entries: list[str] = os.listdir(base)
            # Both the target and the temp file should be absent.
            self.assertEqual(entries, [])


if __name__ == '__main__':
    unittest.main()
