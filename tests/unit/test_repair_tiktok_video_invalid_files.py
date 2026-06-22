'''
Tests for tools/repair_tiktok_video_invalid_files.py.
'''

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

import brotli
from jsonschema import Draft202012Validator

from tools import repair_tiktok_video_invalid_files as tool


def _write_brotli_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_bytes(
        brotli.compress(json.dumps(payload).encode('utf-8')),
    )


def _read_brotli_json(path: Path) -> dict[str, Any]:
    data: Any = json.loads(
        brotli.decompress(path.read_bytes()).decode('utf-8'),
    )
    assert isinstance(data, dict)
    return data


class TestRepairTikTokVideoInvalidFiles(unittest.TestCase):

    def _record(self, video_id: str) -> dict[str, Any]:
        return {
            'video_id': video_id,
            'username': 'creator',
            'url': f'https://www.tiktok.com/@creator/video/{video_id}',
            'codec': 'h264',
            'bitrate': 12345,
            'aspect_ratio': 0.5625,
            'video_height': 1280,
            'video_ratio': '720p',
            'video_width': 720,
            'volume_loudness': -14.2,
            'volume_peak': 0.79433,
            'author_avatar_urls': [
                {
                    'name': 'medium',
                    'url': 'https://example/avatar-medium.jpeg',
                },
            ],
        }

    def test_repair_file_removes_stream_fields_and_adds_avatar(
        self,
    ) -> None:
        validator: Draft202012Validator = tool.load_validator(
            tool.DEFAULT_SCHEMA_PATH,
        )
        record: dict[str, Any] = {
            'video_id': '123',
            'username': 'creator',
            'url': 'https://www.tiktok.com/@creator/video/123',
            'codec': 'h264',
            'bitrate': 12345,
            'aspect_ratio': 0.5625,
            'video_height': 1280,
            'video_ratio': '720p',
            'video_width': 720,
            'volume_loudness': -14.2,
            'volume_peak': 0.79433,
            'author_avatar_urls': [
                {
                    'name': 'thumb',
                    'url': 'https://example/avatar-thumb.jpeg',
                },
                {
                    'name': 'medium',
                    'url': 'https://example/avatar-medium.jpeg',
                },
                {
                    'name': 'large',
                    'url': 'https://example/avatar-large.jpeg',
                },
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            root: Path = Path(td)
            source: Path = root / 'tiktok-video-123.json.br.invalid'
            target: Path = root / 'tiktok-video-123.json.br'
            _write_brotli_json(source, record)

            ok: bool
            message: str
            ok, message = tool.repair_file(source, validator)

            self.assertTrue(ok, message)
            self.assertFalse(source.exists())
            self.assertTrue(target.exists())
            repaired: dict[str, Any] = _read_brotli_json(target)
            for field_name in tool.RETIRED_TOP_LEVEL_FIELDS:
                self.assertNotIn(field_name, repaired)
            self.assertEqual(
                repaired['author_avatar_url'],
                'https://example/avatar-medium.jpeg',
            )
            self.assertIsNone(tool.validate_record(repaired, validator))

    def test_run_limit_caps_successful_repairs(self) -> None:
        validator: Draft202012Validator = tool.load_validator(
            tool.DEFAULT_SCHEMA_PATH,
        )
        with tempfile.TemporaryDirectory() as td:
            root: Path = Path(td)
            for video_id in ('001', '002', '003'):
                source: Path = (
                    root / f'tiktok-video-{video_id}.json.br.invalid'
                )
                _write_brotli_json(source, self._record(video_id))

            stats: tool.RepairStats = tool.run(
                root,
                validator,
                limit=2,
            )

            self.assertEqual(stats.seen, 2)
            self.assertEqual(stats.repaired, 2)
            self.assertEqual(stats.failed, 0)
            self.assertEqual(stats.skipped, 0)
            repaired_files: list[Path] = sorted(
                root.glob('*.json.br'),
            )
            invalid_files: list[Path] = sorted(
                root.glob('*.invalid'),
            )
            self.assertEqual(len(repaired_files), 2)
            self.assertEqual(len(invalid_files), 1)

    def test_run_all_json_br_repairs_valid_and_invalid_files(self) -> None:
        validator: Draft202012Validator = tool.load_validator(
            tool.DEFAULT_SCHEMA_PATH,
        )
        with tempfile.TemporaryDirectory() as td:
            root: Path = Path(td)
            valid_source: Path = root / 'tiktok-video-001.json.br'
            invalid_source: Path = (
                root / 'tiktok-video-002.json.br.invalid'
            )
            unrelated: Path = root / 'tiktok-video-003.json'
            _write_brotli_json(valid_source, self._record('001'))
            _write_brotli_json(invalid_source, self._record('002'))
            _write_brotli_json(unrelated, self._record('003'))

            stats: tool.RepairStats = tool.run(
                root,
                validator,
                all_json_br=True,
            )

            self.assertEqual(stats.seen, 2)
            self.assertEqual(stats.repaired, 2)
            self.assertEqual(stats.failed, 0)
            self.assertTrue(valid_source.exists())
            self.assertFalse(invalid_source.exists())
            self.assertTrue((root / 'tiktok-video-002.json.br').exists())
            self.assertTrue(unrelated.exists())

            repaired_valid: dict[str, Any] = _read_brotli_json(
                valid_source,
            )
            repaired_invalid: dict[str, Any] = _read_brotli_json(
                root / 'tiktok-video-002.json.br',
            )
            for field_name in tool.RETIRED_TOP_LEVEL_FIELDS:
                self.assertNotIn(field_name, repaired_valid)
                self.assertNotIn(field_name, repaired_invalid)

    def test_repair_file_preserves_envelope_shape(self) -> None:
        validator: Draft202012Validator = tool.load_validator(
            tool.DEFAULT_SCHEMA_PATH,
        )
        record: dict[str, Any] = self._record('123')
        envelope: dict[str, Any] = {
            'schema': 'drand-tiktok-video',
            'version': '0.0.1',
            'data': record,
            'codec': 'envelope-codec',
        }
        with tempfile.TemporaryDirectory() as td:
            root: Path = Path(td)
            source: Path = root / 'tiktok-video-123.json.br.invalid'
            target: Path = root / 'tiktok-video-123.json.br'
            _write_brotli_json(source, envelope)

            ok: bool
            message: str
            ok, message = tool.repair_file(source, validator)

            self.assertTrue(ok, message)
            repaired: dict[str, Any] = _read_brotli_json(target)
            self.assertEqual(repaired['schema'], 'drand-tiktok-video')
            self.assertEqual(repaired['version'], '0.0.1')
            self.assertEqual(repaired['codec'], 'envelope-codec')
            self.assertIsInstance(repaired['data'], dict)
            data: dict[str, Any] = repaired['data']
            for field_name in tool.RETIRED_TOP_LEVEL_FIELDS:
                self.assertNotIn(field_name, data)
            self.assertEqual(
                data['author_avatar_url'],
                'https://example/avatar-medium.jpeg',
            )
            self.assertIsNone(tool.validate_record(data, validator))

    def test_repair_file_requires_flag_for_non_invalid_json_br(
        self,
    ) -> None:
        validator: Draft202012Validator = tool.load_validator(
            tool.DEFAULT_SCHEMA_PATH,
        )
        with tempfile.TemporaryDirectory() as td:
            source: Path = Path(td) / 'tiktok-video-001.json.br'
            _write_brotli_json(source, self._record('001'))

            ok: bool
            message: str
            ok, message = tool.repair_file(source, validator)

            self.assertFalse(ok)
            self.assertIn('filename does not end with .invalid', message)


if __name__ == '__main__':
    unittest.main()
