'''Tests for tools/delete_uploaded_video_invalid_files.py.'''

import tempfile
import unittest

from io import StringIO
from pathlib import Path
from unittest.mock import patch

from tools.delete_uploaded_video_invalid_files import (
    DEFAULT_VIDEO_DIR,
    CleanupResult,
    CleanupSettings,
    cleanup_invalid_files,
    extract_invalid_video_id,
    load_uploaded_ids,
)


class TestExtractInvalidVideoId(unittest.TestCase):

    def test_extracts_min_and_dlp_ids(self) -> None:
        self.assertEqual(
            extract_invalid_video_id(
                'video-min-aaaaaaaaaaa.json.br.invalid',
            ),
            'aaaaaaaaaaa',
        )
        self.assertEqual(
            extract_invalid_video_id(
                'video-dlp-bbbbbbbbbbb.json.br.invalid',
            ),
            'bbbbbbbbbbb',
        )

    def test_rejects_non_invalid_or_malformed_names(self) -> None:
        names: list[str] = [
            'video-min-aaaaaaaaaaa.json.br',
            'video-min-short.json.br.invalid',
            'video-foo-aaaaaaaaaaa.json.br.invalid',
            'channel-aaaaaaaaaaa.json.br.invalid',
        ]
        for name in names:
            with self.subTest(name=name):
                self.assertIsNone(extract_invalid_video_id(name))


class TestCleanupInvalidFiles(unittest.TestCase):

    def test_container_video_env_does_not_override_host_default(
        self,
    ) -> None:
        with patch.dict(
            'os.environ',
            {'YOUTUBE_VIDEO_DATA_DIR': '/data/videos'},
        ):
            settings = CleanupSettings(_cli_parse_args=[])

        self.assertEqual(settings.video_dir, DEFAULT_VIDEO_DIR)

    def test_load_uploaded_ids_ignores_blanks_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'uploaded_videos.lst'
            path.write_text(
                'aaaaaaaaaaa\n\ninvalid\nbbbbbbbbbbb\n',
                encoding='utf-8',
            )
            err = StringIO()
            with patch('sys.stderr', err):
                self.assertEqual(
                    load_uploaded_ids(path),
                    {'aaaaaaaaaaa', 'bbbbbbbbbbb'},
                )
            self.assertIn(
                'ignored 1 invalid video_id lines',
                err.getvalue(),
            )
            self.assertIn("3:'invalid'", err.getvalue())

    def test_dry_run_counts_without_deleting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root: Path = Path(tmp)
            match: Path = (
                root / 'video-min-aaaaaaaaaaa.json.br.invalid'
            )
            keep: Path = (
                root / 'video-dlp-bbbbbbbbbbb.json.br.invalid'
            )
            malformed: Path = (
                root / 'video-min-short.json.br.invalid'
            )
            regular: Path = root / 'video-min-aaaaaaaaaaa.json.br'
            for path in (match, keep, malformed, regular):
                path.write_text('', encoding='utf-8')

            result = cleanup_invalid_files(
                {'aaaaaaaaaaa'},
                root,
                delete=False,
                unmatched_list=root / 'video_ids.lst',
            )

            self.assertEqual(result.scanned, 3)
            self.assertEqual(result.invalid_files, 2)
            self.assertEqual(result.matched, 1)
            self.assertEqual(result.unmatched, 1)
            self.assertEqual(result.deleted, 0)
            self.assertTrue(match.exists())
            self.assertTrue(keep.exists())
            self.assertTrue(malformed.exists())
            self.assertTrue(regular.exists())
            self.assertEqual(
                (root / 'video_ids.lst').read_text(encoding='utf-8'),
                'bbbbbbbbbbb\n',
            )

    def test_delete_removes_only_uploaded_invalid_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            match: Path = (
                root / 'video-dlp-aaaaaaaaaaa.json.br.invalid'
            )
            keep: Path = (
                root / 'video-min-bbbbbbbbbbb.json.br.invalid'
            )
            for path in (match, keep):
                path.write_text('', encoding='utf-8')

            result = cleanup_invalid_files(
                {'aaaaaaaaaaa'},
                root,
                delete=True,
                unmatched_list=root / 'video_ids.lst',
            )

            self.assertEqual(result.matched, 1)
            self.assertEqual(result.unmatched, 1)
            self.assertEqual(result.deleted, 1)
            self.assertFalse(match.exists())
            self.assertTrue(keep.exists())
            self.assertEqual(
                (root / 'video_ids.lst').read_text(encoding='utf-8'),
                'bbbbbbbbbbb\n',
            )

    def test_unmatched_file_contains_sorted_unique_video_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root: Path = Path(tmp)
            for name in (
                'video-dlp-ccccccccccc.json.br.invalid',
                'video-min-bbbbbbbbbbb.json.br.invalid',
                'video-dlp-bbbbbbbbbbb.json.br.invalid',
            ):
                (root / name).write_text('', encoding='utf-8')

            result: CleanupResult = cleanup_invalid_files(
                set(),
                root,
                delete=False,
                unmatched_list=root / 'video_ids.lst',
            )

            self.assertEqual(result.unmatched, 2)
            self.assertEqual(
                (root / 'video_ids.lst').read_text(encoding='utf-8'),
                'bbbbbbbbbbb\nccccccccccc\n',
            )


if __name__ == '__main__':
    unittest.main()
