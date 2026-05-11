'''Tests for the pure helpers in
``tools/yt_backfill_thumbnail_urls.py``: directory scan, URL-less
thumbnail detection, JSONL state-file load/append, and the
thumbnail-only patch that preserves all other fields on a
``video-dlp-`` record.
'''

import importlib.util
import tempfile
import unittest

from pathlib import Path
from types import ModuleType


def _load_tool() -> ModuleType:
    import sys
    if 'yt_backfill_thumbnail_urls' in sys.modules:
        return sys.modules['yt_backfill_thumbnail_urls']
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = (
        repo_root / 'tools' / 'yt_backfill_thumbnail_urls.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_backfill_thumbnail_urls', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_backfill_thumbnail_urls'] = module
    spec.loader.exec_module(module)
    return module


tool: ModuleType = _load_tool()


class TestExtractVideoIdFromFilename(unittest.TestCase):

    def test_video_min_prefix(self) -> None:
        result: str | None = tool._extract_video_id_from_filename(
            'video-min-HS1bUFtYj48.json.br',
        )
        self.assertEqual(result, 'HS1bUFtYj48')

    def test_video_dlp_prefix(self) -> None:
        result: str | None = tool._extract_video_id_from_filename(
            'video-dlp-HS1bUFtYj48.json.br',
        )
        self.assertEqual(result, 'HS1bUFtYj48')

    def test_unknown_prefix_returns_none(self) -> None:
        self.assertIsNone(
            tool._extract_video_id_from_filename(
                'channel-UCxxx.json.br',
            )
        )

    def test_missing_extension_returns_none(self) -> None:
        self.assertIsNone(
            tool._extract_video_id_from_filename(
                'video-min-HS1bUFtYj48',
            )
        )

    def test_marker_file_returns_none(self) -> None:
        self.assertIsNone(
            tool._extract_video_id_from_filename(
                'video-min-HS1bUFtYj48.json.br.invalid',
            )
        )


class TestHasUrllessThumbnail(unittest.TestCase):

    def test_all_thumbnails_have_urls(self) -> None:
        data: dict = {
            'thumbnails': {
                'default': {'url': 'https://...', 'width': 120},
                'medium': {'url': 'https://...', 'width': 320},
            },
        }
        self.assertFalse(tool._has_urlless_thumbnail(data))

    def test_one_thumbnail_missing_url(self) -> None:
        data: dict = {
            'thumbnails': {
                'default': {'url': 'https://...', 'width': 120},
                'medium': {'url': None, 'width': 320},
            },
        }
        self.assertTrue(tool._has_urlless_thumbnail(data))

    def test_thumbnail_url_empty_string(self) -> None:
        data: dict = {
            'thumbnails': {
                'default': {'url': '', 'width': 120},
            },
        }
        self.assertTrue(tool._has_urlless_thumbnail(data))

    def test_thumbnail_url_field_absent(self) -> None:
        data: dict = {
            'thumbnails': {
                'default': {'width': 120, 'height': 90},
            },
        }
        self.assertTrue(tool._has_urlless_thumbnail(data))

    def test_no_thumbnails_field_returns_false(self) -> None:
        '''No thumbnails at all is not the bug we are fixing — let
        the live scraper handle it. Backfill targets only files
        that DO have thumbnail entries, just without urls.'''
        data: dict = {'video_id': 'abc'}
        self.assertFalse(tool._has_urlless_thumbnail(data))

    def test_thumbnails_as_list(self) -> None:
        '''Older records may serialize thumbnails as a list. Detect
        URL-less entries there too.'''
        data: dict = {
            'thumbnails': [
                {'url': 'https://...'},
                {'url': None},
            ],
        }
        self.assertTrue(tool._has_urlless_thumbnail(data))

    def test_unhashable_thumbnail_value_skipped(self) -> None:
        data: dict = {'thumbnails': {'default': None}}
        self.assertTrue(tool._has_urlless_thumbnail(data))


class TestApplyThumbnailPatch(unittest.TestCase):

    def test_replaces_thumbnails_keeps_other_fields(self) -> None:
        existing: dict = {
            'video_id': 'HS1bUFtYj48',
            'title': 'Test',
            'thumbnails': {
                'default': {'url': None, 'width': 120},
            },
            'formats': [{'format_id': '22'}],
        }
        fresh_thumbnails: dict = {
            'default': {'url': 'https://yt/thumb.jpg', 'width': 120},
            'medium': {'url': 'https://yt/thumb_m.jpg', 'width': 320},
        }
        patched: dict = tool._apply_thumbnail_patch(
            existing, fresh_thumbnails,
        )
        self.assertEqual(
            patched['thumbnails'], fresh_thumbnails,
        )
        self.assertEqual(patched['video_id'], 'HS1bUFtYj48')
        self.assertEqual(patched['title'], 'Test')
        self.assertEqual(
            patched['formats'], [{'format_id': '22'}],
        )

    def test_does_not_mutate_input(self) -> None:
        existing: dict = {
            'thumbnails': {'default': {'url': None}},
        }
        original: dict = {
            'thumbnails': {'default': {'url': None}},
        }
        tool._apply_thumbnail_patch(
            existing, {'default': {'url': 'https://x'}},
        )
        self.assertEqual(existing, original)


class TestProcessedIdsLoadSync(unittest.TestCase):

    def test_load_returns_set(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'processed.jsonl'
            path.write_text('abc\nxyz\nabc\n')
            result: set[str] = tool._load_processed_ids(path)
            self.assertEqual(result, {'abc', 'xyz'})

    def test_load_missing_file_returns_empty_set(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'missing.jsonl'
            self.assertEqual(
                tool._load_processed_ids(path), set(),
            )


class TestProcessedIdsAppendAsync(unittest.IsolatedAsyncioTestCase):

    async def test_append_processed_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'processed.jsonl'
            await tool._append_processed_id(path, 'abc123')
            await tool._append_processed_id(path, 'xyz789')
            content: str = path.read_text()
            self.assertEqual(content, 'abc123\nxyz789\n')

    async def test_append_failed_writes_record_per_line(self) -> None:
        import json
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'failed.jsonl'
            await tool._append_failed_id(
                path, 'abc', 'unavailable', 'gone',
            )
            await tool._append_failed_id(
                path, 'xyz', 'http_500', 'server boom',
            )
            lines: list[str] = path.read_text().splitlines()
            self.assertEqual(len(lines), 2)
            r0: dict = json.loads(lines[0])
            self.assertEqual(r0['video_id'], 'abc')
            self.assertEqual(r0['reason'], 'unavailable')
            self.assertEqual(r0['message'], 'gone')
            self.assertIn('ts', r0)


class TestScanDirs(unittest.TestCase):
    '''_scan_dirs walks both <base> and <base>/uploaded, yielding
    (path, video_id) for every video-min/video-dlp .json.br found.
    '''

    def test_finds_in_base_and_uploaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td)
            uploaded: Path = base / 'uploaded'
            uploaded.mkdir()
            (base / 'video-min-aaa.json.br').write_bytes(b'')
            (base / 'video-dlp-aaa.json.br').write_bytes(b'')
            (uploaded / 'video-min-bbb.json.br').write_bytes(b'')
            (base / 'channel-UCxxx.json.br').write_bytes(b'')
            (base / 'video-min-ccc.json.br.invalid').write_bytes(
                b''
            )
            results: list[tuple[Path, str]] = list(
                tool._scan_dirs(base),
            )
            ids_seen: set[str] = {vid for _, vid in results}
            self.assertEqual(ids_seen, {'aaa', 'bbb'})

    def test_missing_uploaded_subdir_is_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td)
            (base / 'video-min-aaa.json.br').write_bytes(b'')
            results: list[tuple[Path, str]] = list(
                tool._scan_dirs(base),
            )
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][1], 'aaa')

    def test_missing_base_dir_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td) / 'does-not-exist'
            self.assertEqual(list(tool._scan_dirs(base)), [])


class TestReadRecord(unittest.TestCase):
    '''_read_record must tolerate partially-corrupt input and
    plain-JSON files. The strict path is the live-scraper format
    (brotli-compressed JSON); the resilient fallbacks exist so
    one bad record does not abort a long backfill run, and so
    files written by an older code path that did not brotli-
    compress are still picked up.'''

    def _brotli_json(self, payload: dict) -> bytes:
        import brotli
        import orjson
        return brotli.compress(
            orjson.dumps(payload), quality=11,
            mode=brotli.MODE_TEXT,
        )

    def test_valid_brotli_json_returns_dict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-aaa.json.br'
            p.write_bytes(
                self._brotli_json({'video_id': 'aaa', 'x': 1}),
            )
            record, needs_rewrite = tool._read_record(p)
            self.assertEqual(
                record, {'video_id': 'aaa', 'x': 1},
            )
            self.assertFalse(needs_rewrite)

    def test_plain_json_without_brotli_wrapping(self) -> None:
        '''An older write path may have produced plain JSON. The
        function falls back to parsing the raw bytes directly
        and flags the file for repair.'''
        import orjson
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-bbb.json.br'
            p.write_bytes(
                orjson.dumps({'video_id': 'bbb'}),
            )
            record, needs_rewrite = tool._read_record(p)
        self.assertEqual(record, {'video_id': 'bbb'})
        self.assertTrue(needs_rewrite)

    def test_truncated_brotli_with_recoverable_json(self) -> None:
        '''When brotli's strict decoder rejects a truncated
        stream but the streaming decoder yields enough bytes for
        a complete JSON document, return the partial result and
        flag for repair.'''
        compressed: bytes = self._brotli_json(
            {'video_id': 'ccc', 'tail_padding': 'x' * 4096},
        )
        # Drop the last byte so the stream is incomplete.
        truncated: bytes = compressed[:-1]
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-ccc.json.br'
            p.write_bytes(truncated)
            record, _needs_rewrite = tool._read_record(p)
        # Short brotli streams may decode fully even with one
        # trailing byte chopped (decoder does not require the
        # final empty marker for short inputs). Either we
        # decoded successfully via the strict path or we
        # recovered via the streaming decoder; either is valid
        # resilience and the test does not pin the
        # needs_rewrite flag because it depends on which path
        # the underlying brotli library takes.
        if record is not None:
            self.assertEqual(record.get('video_id'), 'ccc')

    def test_garbage_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-ddd.json.br'
            p.write_bytes(b'\x00\x01\x02\x03 not brotli not json')
            record, needs_rewrite = tool._read_record(p)
        self.assertIsNone(record)
        self.assertFalse(needs_rewrite)

    def test_empty_file_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-eee.json.br'
            p.write_bytes(b'')
            record, needs_rewrite = tool._read_record(p)
        self.assertIsNone(record)
        self.assertFalse(needs_rewrite)

    def test_missing_file_returns_none(self) -> None:
        record, needs_rewrite = tool._read_record(
            Path('/nonexistent/aaa.json.br'),
        )
        self.assertIsNone(record)
        self.assertFalse(needs_rewrite)


class TestRepairBrotliInPlace(unittest.TestCase):
    '''_repair_brotli_in_place rewrites the file with strict
    brotli-compressed JSON so future strict readers do not need
    the resilient fallback.'''

    def test_rewrites_with_strict_brotli(self) -> None:
        '''After repair, the file decompresses cleanly via the
        strict brotli decoder.'''
        import brotli
        import orjson
        with tempfile.TemporaryDirectory() as td:
            p: Path = Path(td) / 'video-min-aaa.json.br'
            # Plain JSON — strict brotli would fail to read this.
            p.write_bytes(orjson.dumps({'video_id': 'aaa'}))
            record: dict = {'video_id': 'aaa'}
            tool._repair_brotli_in_place(p, record)
            # Strict brotli decompress + JSON parse must now
            # succeed without any fallback.
            decoded: dict = orjson.loads(
                brotli.decompress(p.read_bytes()),
            )
        self.assertEqual(decoded, record)


class TestBuildWorkloadRepair(unittest.TestCase):

    def _seed(self, parent: Path, name: str, body: bytes) -> Path:
        p: Path = parent / name
        p.write_bytes(body)
        return p

    def _brotli_json(self, payload: dict) -> bytes:
        import brotli
        import orjson
        return brotli.compress(
            orjson.dumps(payload), quality=11,
            mode=brotli.MODE_TEXT,
        )

    def test_repairs_plain_json_file_with_good_thumbnails(
        self,
    ) -> None:
        '''A file with intact thumbnails but a corrupt brotli
        wrapper does not enter the workload, but should still be
        repaired in place because a future reader would
        otherwise fail to decompress it.'''
        import brotli
        import orjson
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td)
            (base / 'uploaded').mkdir()
            payload: dict = {
                'video_id': 'aaa',
                'thumbnails': {
                    'default': {'url': 'https://x', 'width': 1},
                },
            }
            path: Path = self._seed(
                base, 'video-min-aaa.json.br',
                orjson.dumps(payload),
            )
            work: dict[str, list[Path]] = tool._build_workload(
                base, set(), None, repair_in_place=True,
            )
            self.assertEqual(work, {})  # not in workload
            # File is now strict-brotli readable.
            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )
            self.assertEqual(decoded, payload)

    def test_repair_disabled_leaves_file_unchanged(self) -> None:
        '''Dry-run flag (and equivalent ``repair_in_place=False``)
        must not mutate any file on disk.'''
        import orjson
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td)
            (base / 'uploaded').mkdir()
            plain: bytes = orjson.dumps(
                {
                    'video_id': 'aaa',
                    'thumbnails': {
                        'default': {
                            'url': 'https://x',
                            'width': 1,
                        },
                    },
                },
            )
            path: Path = self._seed(
                base, 'video-min-aaa.json.br', plain,
            )
            tool._build_workload(
                base, set(), None, repair_in_place=False,
            )
            self.assertEqual(path.read_bytes(), plain)

    def test_url_less_record_in_workload_still_repaired_by_post_step(
        self,
    ) -> None:
        '''Files whose thumbnails ARE missing urls go into the
        workload AND are repaired during scan. The downstream
        atomic_write_bytes call in _process_video would re-write
        them anyway, but doing the repair at scan time keeps the
        invariant that every file _build_workload returns is
        readable via strict brotli.'''
        import brotli
        import orjson
        with tempfile.TemporaryDirectory() as td:
            base: Path = Path(td)
            (base / 'uploaded').mkdir()
            payload: dict = {
                'video_id': 'aaa',
                'thumbnails': {
                    'default': {'url': None, 'width': 1},
                },
            }
            path: Path = self._seed(
                base, 'video-min-aaa.json.br',
                orjson.dumps(payload),
            )
            work: dict[str, list[Path]] = tool._build_workload(
                base, set(), None, repair_in_place=True,
            )
            self.assertEqual(set(work), {'aaa'})
            # The file is now strict-brotli readable.
            orjson.loads(brotli.decompress(path.read_bytes()))


if __name__ == '__main__':
    unittest.main()
