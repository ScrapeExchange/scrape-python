'''
Unit tests for the schema-review tool's per-file decision logic.

The tool's ``review_file`` is a pure function over the filesystem
+ two compiled validators, so it can be tested without spinning
up the HTTP fetch path. Cover the four outcome shapes: success,
schema-validation failure, parse failure, and unrecognised
prefix.
'''

import os
import tempfile
import unittest
from pathlib import Path

import brotli
import orjson

from scrape_exchange.schema_validator import SchemaValidator
from tools.yt_review import _list_targets, review_file


_CHANNEL_SCHEMA: dict = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object',
    'properties': {
        'channel_id': {'type': 'string'},
        'channel_handle': {'type': 'string'},
        'url': {'type': 'string'},
    },
    'required': ['channel_id', 'channel_handle', 'url'],
    'additionalProperties': False,
}

_VIDEO_SCHEMA: dict = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object',
    'properties': {
        'video_id': {'type': 'string'},
        'channel_handle': {'type': 'string'},
        'url': {'type': 'string'},
    },
    'required': ['video_id', 'url'],
    'additionalProperties': False,
}


def _validators() -> tuple[SchemaValidator, SchemaValidator]:
    return (
        SchemaValidator(_CHANNEL_SCHEMA),
        SchemaValidator(_VIDEO_SCHEMA),
    )


def _write_brotli(path: Path, data: dict) -> None:
    payload: bytes = brotli.compress(orjson.dumps(data))
    path.write_bytes(payload)


class TestReviewFile(unittest.TestCase):

    def test_channel_brotli_valid_returns_success(self) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channel-foo.json.br'
            _write_brotli(path, {
                'channel_id': 'UC1234567890abcdefghij',
                'channel_handle': 'foo',
                'url': 'https://youtube.com/@foo',
            })
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertEqual(line, 'channel-foo.json.br: success')

    def test_channel_missing_required_reports_validation_error(
        self,
    ) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channel-foo.json.br'
            _write_brotli(path, {
                'channel_handle': 'foo',
                'url': 'https://youtube.com/@foo',
            })
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertTrue(
            line.startswith('channel-foo.json.br: failure:'),
        )
        # Schema-derived reason mentions the missing field.
        self.assertIn('channel_id', line)

    def test_video_plain_json_valid(self) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'video-abcXYZ.json'
            path.write_bytes(orjson.dumps({
                'video_id': 'abcXYZ',
                'url': 'https://youtu.be/abcXYZ',
            }))
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertEqual(line, 'video-abcXYZ.json: success')

    def test_brotli_corrupt_reports_decompression_failure(
        self,
    ) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channel-foo.json.br'
            # Half-written brotli stream — what a killed scraper
            # used to leave behind. Truncate a real payload to
            # force a decompression error.
            payload: bytes = brotli.compress(
                orjson.dumps({'channel_id': 'UC'}),
            )
            path.write_bytes(payload[: len(payload) // 2])
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertTrue(line.startswith('channel-foo.json.br:'))
        self.assertIn('failure', line)
        self.assertIn('brotli', line.lower())

    def test_unrecognised_prefix_skipped(self) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'random-file.json'
            path.write_bytes(orjson.dumps({}))
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertIn('skipped', line)

    def test_unsupported_extension_reports_failure(self) -> None:
        channel_validator, video_validator = _validators()
        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channel-foo.txt'
            path.write_bytes(b'not json')
            line: str = review_file(
                path, channel_validator, video_validator,
            )
        self.assertIn('failure', line)
        self.assertIn('unsupported extension', line)


class TestListTargets(unittest.TestCase):

    def test_filters_by_prefix_and_skips_subdirs(self) -> None:
        with tempfile.TemporaryDirectory() as base:
            base_path: Path = Path(base)
            (base_path / 'channel-a.json.br').write_bytes(b'')
            (base_path / 'video-b.json').write_bytes(b'')
            (base_path / 'random.json').write_bytes(b'')
            os.makedirs(base_path / 'channel-subdir', exist_ok=True)

            targets: list[Path] = _list_targets(base_path)

        names: list[str] = [t.name for t in targets]
        self.assertEqual(
            names, ['channel-a.json.br', 'video-b.json'],
        )


if __name__ == '__main__':
    unittest.main()
