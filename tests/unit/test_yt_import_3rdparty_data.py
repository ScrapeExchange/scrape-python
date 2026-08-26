'''Unit tests for ``tools/yt_import_3rdparty_data.py``.

Focused coverage of the file-sink path that replaces the
former Redis-direct enqueue. End-to-end coverage of the
Kaggle / Hugging Face downloaders is out of scope here.
'''

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.yt_import_3rdparty_data import (
    _ChannelFileSink,
    _VideoFileSink,
    ImportKaggleTrendingSettings,
    import_file,
)


class TestImportSettingsDefaults(unittest.TestCase):
    '''The legacy ``redis_dsn`` setting is gone; in its place
    the import tool now exposes two output paths that match
    the input formats consumed by yt_channel_queue.py and
    yt_video_queue.py respectively.
    '''

    def test_redis_dsn_setting_removed(self) -> None:
        with patch.dict('os.environ', {}, clear=True):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertFalse(hasattr(s, 'redis_dsn'))

    def test_channels_out_default(self) -> None:
        with patch.dict('os.environ', {}, clear=True):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertEqual(
            s.channels_out, 'import-channels.jsonl',
        )

    def test_videos_out_default(self) -> None:
        with patch.dict('os.environ', {}, clear=True):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertEqual(s.videos_out, 'import-videos.lst')

    def test_videos_out_accepts_env_var(self) -> None:
        with patch.dict(
            'os.environ',
            {'VIDEOS_OUT': '/data/videos.lst'},
            clear=True,
        ):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertEqual(s.videos_out, '/data/videos.lst')


class TestChannelFileSink(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.path: Path = Path(
            self._tmp.name, 'channels.jsonl',
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    def _read_lines(self) -> list[dict]:
        with open(self.path, encoding='utf-8') as fh:
            return [
                json.loads(line)
                for line in fh.read().splitlines()
                if line.strip()
            ]

    async def test_enqueue_scheduled_writes_jsonl_record(
        self,
    ) -> None:
        sink: _ChannelFileSink = _ChannelFileSink(self.path)
        try:
            await sink.enqueue_scheduled(
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                source='kaggle-import',
            )
        finally:
            sink.close()
        rows: list[dict] = self._read_lines()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0],
            {
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'channel_handle': None,
                'source': 'kaggle-import',
            },
        )

    async def test_enqueue_unresolved_writes_jsonl_record(
        self,
    ) -> None:
        sink: _ChannelFileSink = _ChannelFileSink(self.path)
        try:
            await sink.enqueue_unresolved(
                'SomeHandle', source='kaggle-import',
            )
        finally:
            sink.close()
        rows: list[dict] = self._read_lines()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0],
            {
                'channel_id': None,
                'channel_handle': 'SomeHandle',
                'source': 'kaggle-import',
            },
        )

    async def test_appends_across_calls(self) -> None:
        sink: _ChannelFileSink = _ChannelFileSink(self.path)
        try:
            await sink.enqueue_scheduled(
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                source='a',
            )
            await sink.enqueue_unresolved(
                'bhandle', source='b',
            )
            await sink.enqueue_scheduled(
                'UCcccccccccccccccccccccc',
                source='c',
            )
        finally:
            sink.close()
        rows: list[dict] = self._read_lines()
        self.assertEqual(len(rows), 3)
        self.assertEqual(
            [r.get('channel_id') for r in rows],
            [
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                None,
                'UCcccccccccccccccccccccc',
            ],
        )

    async def test_close_is_idempotent(self) -> None:
        sink: _ChannelFileSink = _ChannelFileSink(self.path)
        sink.close()
        sink.close()  # must not raise

    async def test_creates_parent_directory(self) -> None:
        nested: Path = Path(
            self._tmp.name, 'a', 'b', 'channels.jsonl',
        )
        sink: _ChannelFileSink = _ChannelFileSink(nested)
        try:
            await sink.enqueue_scheduled(
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                source='nested',
            )
        finally:
            sink.close()
        self.assertTrue(nested.exists())

    async def test_record_shape_matches_yt_channel_queue_import(
        self,
    ) -> None:
        '''yt_channel_queue.py import parses each line as a
        JSON object with ``channel_id`` and ``channel_handle``
        fields. The sink must produce exactly that shape so
        the downstream import succeeds without translation.
        '''
        sink: _ChannelFileSink = _ChannelFileSink(self.path)
        try:
            await sink.enqueue_scheduled(
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                source='match-test',
            )
            await sink.enqueue_unresolved(
                'newhandle', source='match-test',
            )
        finally:
            sink.close()
        from tools.yt_channel_queue import (
            _parse_import_line,
        )
        with open(self.path, encoding='utf-8') as fh:
            lines: list[str] = fh.read().splitlines()
        parsed_resolved: tuple[str, str] | None = (
            _parse_import_line(lines[0])
        )
        parsed_unresolved: tuple[str, str] | None = (
            _parse_import_line(lines[1])
        )
        self.assertEqual(
            parsed_resolved,
            ('UCaaaaaaaaaaaaaaaaaaaaaa', ''),
        )
        self.assertEqual(
            parsed_unresolved, ('', 'newhandle'),
        )


class TestVideoFileSink(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.path: Path = Path(
            self._tmp.name, 'import-videos.lst',
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_enqueue_writes_one_id_per_line(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.path)
        await sink.enqueue('dQw4w9WgXcQ', source='kaggle')
        await sink.enqueue('abc12345678', source='kaggle')
        sink.close()
        self.assertEqual(
            self.path.read_text(encoding='utf-8'),
            'dQw4w9WgXcQ\nabc12345678\n',
        )

    async def test_enqueue_accepts_queue_channel_context(
        self,
    ) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.path)
        await sink.enqueue(
            'dQw4w9WgXcQ',
            source='kaggle',
            channel_id='UCaaaaaaaaaaaaaaaaaaaaaa',
            channel_handle='example',
            channel_url='https://www.youtube.com/@example',
            channel_is_verified=True,
        )
        sink.close()
        self.assertEqual(
            self.path.read_text(encoding='utf-8'),
            'dQw4w9WgXcQ\n',
        )

    async def test_enqueue_dedupes_within_run(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.path)
        await sink.enqueue('dQw4w9WgXcQ', source='a')
        await sink.enqueue('dQw4w9WgXcQ', source='b')
        sink.close()
        # The same id seen across datasets is written once.
        self.assertEqual(
            self.path.read_text(encoding='utf-8'),
            'dQw4w9WgXcQ\n',
        )

    async def test_creates_parent_directory_if_missing(
        self,
    ) -> None:
        target: Path = Path(
            self._tmp.name, 'fresh', 'level', 'videos.lst',
        )
        sink: _VideoFileSink = _VideoFileSink(target)
        await sink.enqueue('abc12345678', source='nested')
        sink.close()
        self.assertTrue(target.parent.is_dir())
        self.assertEqual(
            target.read_text(encoding='utf-8'),
            'abc12345678\n',
        )

    async def test_file_format_matches_yt_video_queue_add(
        self,
    ) -> None:
        '''yt_video_queue.py add --file parses the file with
        ``_parse_video_ids``, keeping each line matching the
        11-character video_id regex. The sink must produce
        exactly that text shape.
        '''
        sink: _VideoFileSink = _VideoFileSink(self.path)
        await sink.enqueue('aaaaaaaaaaa', source='match')
        await sink.enqueue('bbbbbbbbbbb', source='match')
        sink.close()

        from tools.yt_video_queue import _parse_video_ids
        parsed: list[str] = _parse_video_ids(
            self.path.read_text(encoding='utf-8'),
        )
        self.assertEqual(
            parsed, ['aaaaaaaaaaa', 'bbbbbbbbbbb'],
        )

    async def test_close_is_idempotent(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.path)
        sink.close()
        sink.close()  # must not raise


class TestImportFile(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.root: Path = Path(self._tmp.name)

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_import_file_accepts_video_channel_context(
        self,
    ) -> None:
        data_path: Path = self.root / 'rows.csv'
        data_path.write_text(
            '\n'.join([
                'video_id,channel_id,channel_handle,channel_url,'
                'channel_is_verified',
                'dQw4w9WgXcQ,UCaaaaaaaaaaaaaaaaaaaaaa,example,'
                'https://www.youtube.com/@example,true',
            ]) + '\n',
            encoding='utf-8',
        )
        videos_path: Path = self.root / 'videos.lst'
        channels_path: Path = self.root / 'channels.jsonl'
        video_sink: _VideoFileSink = _VideoFileSink(videos_path)
        channel_sink: _ChannelFileSink = _ChannelFileSink(
            channels_path,
        )
        try:
            stats = await import_file(
                data_path,
                video_sink,
                channel_sink,
                'fixture',
                set(),
            )
        finally:
            video_sink.close()
            channel_sink.close()

        self.assertEqual(stats.errors, 0)
        self.assertEqual(stats.enqueued_videos, 1)
        self.assertEqual(
            videos_path.read_text(encoding='utf-8'),
            'dQw4w9WgXcQ\n',
        )


if __name__ == '__main__':
    unittest.main()
