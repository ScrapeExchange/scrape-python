'''Tests for scrape_exchange.brotli read/write/recovery.'''

import json
import secrets
import string
import tempfile
import unittest
from pathlib import Path

import brotli

from scrape_exchange.brotli import (
    _close_truncated_json,
    brotli_read,
    brotli_write,
)


class TestBrotliWrite(unittest.TestCase):

    def test_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p: Path = Path(d) / 'x.json.br'
            data: dict = {'a': 1, 'b': ['x', 'y']}
            brotli_write(p, data)
            self.assertEqual(brotli_read(p), data)

    def test_unicode_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p: Path = Path(d) / 'u.json.br'
            data: dict = {'title': 'ποŋ'}
            brotli_write(p, data)
            self.assertEqual(brotli_read(p), data)

    def test_atomic_no_leftover_on_failure(self) -> None:
        '''Bad parent directory should not leave a partial tmp file
        in cwd; mkstemp targets ``parent``, so missing parent
        raises before any temp is created.'''
        with self.assertRaises(FileNotFoundError):
            brotli_write(
                Path('/tmp/__nope__/x.json.br'), {'a': 1},
            )


class TestBrotliReadFastPath(unittest.TestCase):

    def test_clean_file_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p: Path = Path(d) / 'c.json.br'
            brotli_write(p, {'a': 1})
            mtime: int = p.stat().st_mtime_ns
            self.assertEqual(brotli_read(p), {'a': 1})
            self.assertEqual(p.stat().st_mtime_ns, mtime)


class TestBrotliReadRecovery(unittest.TestCase):

    def test_truncation_recovered_and_rewritten(self) -> None:
        alphabet = string.ascii_letters + string.digits
        # Put video_ids first so they survive in salvage even if
        # later fields don't.
        data: dict = {
            'channel_handle': 'big',
            'video_ids': [f'v{n:010d}' for n in range(50)],
            'random_blob': ''.join(
                secrets.choice(alphabet) for _ in range(8000)
            ),
        }
        clean: bytes = brotli.compress(
            json.dumps(data).encode(),
        )
        # Append junk LARGER than a chunk so chunked decoder
        # emits multiple chunks before bailing.
        corrupt: bytes = clean + b'\xff' * 5000
        with tempfile.TemporaryDirectory() as d:
            p: Path = Path(d) / 'corrupt.json.br'
            p.write_bytes(corrupt)
            recovered: dict = brotli_read(p)
            self.assertEqual(
                len(recovered.get('video_ids', [])), 50,
            )
            # Subsequent read takes the fast path:
            re_read: dict = brotli_read(p)
            self.assertEqual(recovered, re_read)

    def test_unrecoverable_raises(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p: Path = Path(d) / 'garbage.json.br'
            p.write_bytes(b'totally not brotli')
            with self.assertRaises(brotli.error):
                brotli_read(p)


class TestCloseTruncatedJson(unittest.TestCase):

    def test_complete_dict(self) -> None:
        self.assertEqual(
            _close_truncated_json('{"a": 1, "b": 2}'),
            {'a': 1, 'b': 2},
        )

    def test_truncated_at_key(self) -> None:
        self.assertEqual(
            _close_truncated_json('{"a": 1, "b'),
            {'a': 1},
        )

    def test_truncated_mid_string_value(self) -> None:
        self.assertEqual(
            _close_truncated_json('{"a": 1, "b": "xyz'),
            {'a': 1},
        )

    def test_string_array_value_kept(self) -> None:
        self.assertEqual(
            _close_truncated_json(
                '{"video_ids": ["aaa", "bbb"',
            ),
            {'video_ids': ['aaa', 'bbb']},
        )

    def test_array_after_complete_dict_value(self) -> None:
        self.assertEqual(
            _close_truncated_json(
                '{"obj": {"x": 1, "y": 2}, "v": [',
            ),
            {'obj': {'x': 1, 'y': 2}},
        )

    def test_not_a_dict(self) -> None:
        self.assertIsNone(
            _close_truncated_json('not json'),
        )

    def test_just_opening_brace(self) -> None:
        self.assertIsNone(_close_truncated_json('{'))


if __name__ == '__main__':
    unittest.main()
