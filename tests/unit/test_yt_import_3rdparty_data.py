'''Unit tests for ``tools/yt_import_3rdparty_data.py``.

Focused coverage of the file-sink path that replaces the
former Redis-direct enqueue. End-to-end coverage of the
Kaggle / Hugging Face downloaders is out of scope here.
'''

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.yt_import_3rdparty_data import (
    _ChannelFileSink,
    _VideoFileSink,
    _video_sentinel_dir,
    ImportKaggleTrendingSettings,
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
            s.channels_out, 'imported-channels.jsonl',
        )

    def test_video_data_dir_default(self) -> None:
        with patch.dict('os.environ', {}, clear=True):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertEqual(s.video_data_dir, 'imported-videos')

    def test_video_data_dir_accepts_youtube_video_data_dir(
        self,
    ) -> None:
        with patch.dict(
            'os.environ',
            {'YOUTUBE_VIDEO_DATA_DIR': '/data/videos'},
            clear=True,
        ):
            s: ImportKaggleTrendingSettings = (
                ImportKaggleTrendingSettings(
                    _cli_parse_args=[],
                    _env_file=None,
                )
            )
        self.assertEqual(s.video_data_dir, '/data/videos')


class TestVideoSentinelDir(unittest.TestCase):

    def test_uses_new_child_directory(self) -> None:
        self.assertEqual(
            _video_sentinel_dir(Path('/data/videos')),
            Path('/data/videos/new'),
        )


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
        self.directory: Path = Path(
            self._tmp.name, 'imported-videos',
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_enqueue_touches_named_file(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.directory)
        await sink.enqueue('dQw4w9WgXcQ', source='kaggle')
        self.assertTrue(
            (self.directory / 'dQw4w9WgXcQ').is_file(),
        )

    async def test_enqueue_is_idempotent(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.directory)
        await sink.enqueue('dQw4w9WgXcQ', source='a')
        await sink.enqueue('dQw4w9WgXcQ', source='b')
        # Just one file exists; touch(exist_ok=True) didn't
        # raise on the duplicate enqueue.
        entries: list[str] = sorted(
            os.listdir(self.directory),
        )
        self.assertEqual(entries, ['dQw4w9WgXcQ'])

    async def test_creates_directory_if_missing(
        self,
    ) -> None:
        target: Path = Path(
            self._tmp.name, 'fresh', 'level', 'videos',
        )
        sink: _VideoFileSink = _VideoFileSink(target)
        await sink.enqueue('abc12345678', source='nested')
        self.assertTrue(target.is_dir())
        self.assertTrue((target / 'abc12345678').is_file())

    async def test_file_format_matches_yt_video_queue_import(
        self,
    ) -> None:
        '''yt_video_queue.py import iterates the directory and
        enqueues each filename whose suffix matches the 11-
        character video_id regex. The sink must produce
        exactly that on-disk shape.
        '''
        sink: _VideoFileSink = _VideoFileSink(self.directory)
        await sink.enqueue('aaaaaaaaaaa', source='match')
        await sink.enqueue('bbbbbbbbbbb', source='match')

        from tools.yt_video_queue import _VIDEO_ID_RE
        matching: list[str] = sorted(
            entry for entry in os.listdir(self.directory)
            if _VIDEO_ID_RE.match(entry)
        )
        self.assertEqual(
            matching, ['aaaaaaaaaaa', 'bbbbbbbbbbb'],
        )

    async def test_close_is_idempotent(self) -> None:
        sink: _VideoFileSink = _VideoFileSink(self.directory)
        sink.close()
        sink.close()  # must not raise


if __name__ == '__main__':
    unittest.main()
