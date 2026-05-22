'''Tests for tools/yt_reporter.py.'''

import csv
import importlib.util
import os
import sqlite3
import tempfile
import unittest

from pathlib import Path
from types import ModuleType

import brotli
import orjson


def _load_tool() -> ModuleType:
    import sys
    if 'yt_reporter' in sys.modules:
        return sys.modules['yt_reporter']
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_reporter.py'
    spec = importlib.util.spec_from_file_location(
        'yt_reporter', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_reporter'] = module
    spec.loader.exec_module(module)
    return module


tool: ModuleType = _load_tool()


def _write_br(path: Path, payload: dict) -> None:
    path.write_bytes(brotli.compress(orjson.dumps(payload)))


def _channel(
    channel_id: str = 'UCabc',
    channel_handle: str = 'handle',
    view_count: int = 1,
) -> dict:
    return {
        'channel_id': channel_id,
        'channel_handle': channel_handle,
        'url': f'https://www.youtube.com/@{channel_handle}',
        'verified': False,
        'subscriber_count': 1,
        'view_count': view_count,
        'channel_thumbnails': [
            {'url': 'https://img.test/c.jpg', 'width': 1, 'height': 1},
        ],
    }


def _video(
    video_id: str = 'video1234567',
    channel_id: str = 'UCabc',
    channel_handle: str = 'handle',
) -> dict:
    return {
        'video_id': video_id,
        'channel_id': channel_id,
        'channel_handle': channel_handle,
        'channel_url': f'https://www.youtube.com/@{channel_handle}',
        'embed_url': f'https://www.youtube.com/embed/{video_id}',
        'formats': {
            '18': {
                'format_id': '18',
                'url': 'https://media.test/v.mp4',
            },
        },
        'thumbnails': {
            'default': {
                'url': 'https://img.test/v.jpg',
                'width': 1,
                'height': 1,
            },
        },
    }


class TestYtReporter(unittest.TestCase):

    def test_classify_entries(self) -> None:
        self.assertEqual(
            tool._classify_entry(
                'video-min-abc.json.br', 'video', 'root',
            ),
            ('video', 'video-min-'),
        )
        self.assertEqual(
            tool._classify_entry(
                'video-dlp-abc.json.br', 'video', 'uploaded',
            ),
            ('video', 'video-dlp-'),
        )
        self.assertEqual(
            tool._classify_entry(
                'abcdefghijk', 'video', 'priority',
            ),
            ('video_priority_pending_id', 'bare-video-id'),
        )
        self.assertIsNone(
            tool._classify_entry('abcdefghijk', 'video', 'root'),
        )
        self.assertIsNone(
            tool._classify_entry('abcdefghijk', 'channel', 'priority'),
        )
        self.assertEqual(
            tool._classify_entry(
                'video-min-abc.json.br.failed', 'video', 'root',
            ),
            ('video_marker', '.failed'),
        )
        self.assertEqual(
            tool._classify_entry('channel-abc.json.br', 'channel', 'root'),
            ('channel', 'channel-'),
        )

    def test_reporter_settings_parse_cli_args(self) -> None:
        settings = tool.ReporterSettings(
            _env_file=None,
            _cli_parse_args=[
                '--video-data-dir', '/tmp/videos',
                '--channel-data-dir', '/tmp/channels',
                '--db-path', '/tmp/report.sqlite3',
                '--report-path', '/tmp/report.csv',
                '--workers', '2',
                '--force',
                '--limit', '5',
            ],
        )

        self.assertEqual(settings.video_data_dir, '/tmp/videos')
        self.assertEqual(settings.channel_data_dir, '/tmp/channels')
        self.assertEqual(settings.db_path, Path('/tmp/report.sqlite3'))
        self.assertEqual(settings.report_path, Path('/tmp/report.csv'))
        self.assertEqual(settings.workers, 2)
        self.assertTrue(settings.force)
        self.assertEqual(settings.limit, 5)

    def test_csv_report_creates_header_then_appends_rows(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'report.csv'
            first = {'files.video.root.video-min-': 1}
            second = {
                'files.video.root.video-min-': 3,
                'issues.video_missing_channel_id': 2,
            }
            tool._write_csv_report(path, first, existed_at_start=False)
            tool._write_csv_report(path, second, existed_at_start=True)
            with path.open(newline='', encoding='utf-8') as fh:
                rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), 2)
        self.assertIn('generated_at', rows[0])
        self.assertNotIn('stats_json', rows[0])
        self.assertEqual(rows[0]['files.video.root.video-min-'], '1')
        self.assertEqual(rows[1]['files.video.root.video-min-'], '3')
        self.assertNotIn('issues.video_missing_channel_id', rows[1])

    def test_run_reporter_persists_and_uses_incremental_processing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel_dir = root / 'channels'
            video_dir = root / 'videos'
            for directory in (
                channel_dir, channel_dir / 'uploaded',
                channel_dir / 'priority', video_dir,
                video_dir / 'uploaded', video_dir / 'priority',
            ):
                directory.mkdir(parents=True)
            channel_path = channel_dir / 'channel-handle.json.br'
            video_path = video_dir / 'video-min-video1234567.json.br'
            _write_br(channel_path, _channel())
            _write_br(video_path, _video())
            (video_dir / 'priority' / 'abcdefghijk').write_text('')

            db_path = root / 'report.sqlite3'
            report_path = root / 'report.csv'
            first = tool.run_reporter(
                channel_dir, video_dir, db_path, report_path,
                workers=1,
            )
            second = tool.run_reporter(
                channel_dir, video_dir, db_path, report_path,
                workers=1,
            )
            old_mtime = video_path.stat().st_mtime
            os.utime(video_path, (old_mtime + 10, old_mtime + 10))
            third = tool.run_reporter(
                channel_dir, video_dir, db_path, report_path,
                workers=1,
            )
            channel_path.unlink()
            fourth = tool.run_reporter(
                channel_dir, video_dir, db_path, report_path,
                workers=1,
            )

            conn = sqlite3.connect(db_path)
            try:
                channel_rows = conn.execute(
                    'SELECT channel_id, channel_handle FROM channel_index',
                ).fetchall()
            finally:
                conn.close()
            with report_path.open(newline='', encoding='utf-8') as fh:
                report_rows = list(csv.DictReader(fh))

        self.assertEqual(first['processed.channel'], 1)
        self.assertEqual(first['processed.video'], 1)
        self.assertEqual(second['processed.channel'], 0)
        self.assertEqual(second['processed.video'], 0)
        self.assertEqual(third['processed.channel'], 0)
        self.assertEqual(third['processed.video'], 1)
        self.assertEqual(fourth['processed.channel'], 0)
        self.assertEqual(fourth['processed.video'], 0)
        self.assertEqual(
            fourth['issues.video_channel_not_in_channel_db'], 1,
        )
        self.assertEqual(
            first['files.video_priority_pending_id.priority.bare-video-id'],
            1,
        )
        self.assertEqual(channel_rows, [])
        self.assertEqual(len(report_rows), 4)
        self.assertNotIn('stats_json', report_rows[-1])

    def test_model_checks_report_requested_issues(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel_dir = root / 'channels'
            video_dir = root / 'videos'
            channel_dir.mkdir()
            video_dir.mkdir()
            (channel_dir / 'uploaded').mkdir()
            (channel_dir / 'priority').mkdir()
            (video_dir / 'uploaded').mkdir()
            (video_dir / 'priority').mkdir()
            _write_br(
                channel_dir / 'channel-bad.json.br',
                {
                    'channel_id': None,
                    'channel_handle': None,
                    'url': None,
                    'channel_thumbnails': [],
                    'view_count': 0,
                },
            )
            bad_video = _video(channel_id='UCmissing')
            bad_video['channel_handle'] = None
            bad_video['channel_url'] = None
            bad_video['embed_url'] = None
            bad_video['formats'] = {}
            bad_video['thumbnails'] = {
                'default': {'url': None, 'width': 1, 'height': 1},
            }
            _write_br(video_dir / 'video-dlp-badvideo123.json.br', bad_video)
            stats = tool.run_reporter(
                channel_dir, video_dir, root / 'r.sqlite3',
                root / 'r.csv', workers=1,
            )

        self.assertEqual(stats['issues.channel_missing_channel_id'], 1)
        self.assertEqual(stats['issues.channel_missing_channel_handle'], 1)
        self.assertEqual(stats['issues.channel_missing_channel_thumbnails'], 1)
        self.assertEqual(stats['issues.channel_view_count_zero'], 1)
        self.assertEqual(stats['issues.video_missing_channel_handle'], 1)
        self.assertEqual(stats['issues.video_missing_channel_url'], 1)
        self.assertEqual(stats['issues.video_missing_embed_url'], 1)
        self.assertEqual(stats['issues.video_missing_formats'], 1)
        self.assertEqual(stats['issues.video_thumbnail_missing_url'], 1)
        self.assertEqual(stats['issues.video_channel_not_in_channel_db'], 1)


if __name__ == '__main__':
    unittest.main()
