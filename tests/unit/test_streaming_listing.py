'''Tests for the chunked, resumable directory listing.'''

import os
import tempfile
import unittest
from pathlib import Path

from scrape_exchange.streaming_listing import (
    StreamingListing,
    count_matching_files,
)


def _touch(directory: str, *names: str) -> None:
    for name in names:
        Path(directory, name).write_text('x')


class _FailingIterator:
    '''scandir stand-in whose iteration fails with OSError.'''

    def __init__(self) -> None:
        self.closed: bool = False

    def __iter__(self) -> '_FailingIterator':
        return self

    def __next__(self) -> os.DirEntry:
        raise OSError('directory read failed')

    def close(self) -> None:
        self.closed = True


class TestStreamingListing(unittest.IsolatedAsyncioTestCase):

    async def test_returns_all_files_in_chunks_then_ends_pass(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _touch(tmp, 'a', 'b', 'c', 'd', 'e')
            listing: StreamingListing = StreamingListing(tmp, chunk_size=2)
            seen: list[str] = []
            ended: bool = False
            calls: int = 0
            while not ended:
                names, ended = await listing.next_chunk()
                seen.extend(names)
                calls += 1
            listing.close()

        self.assertEqual(sorted(seen), ['a', 'b', 'c', 'd', 'e'])
        self.assertLessEqual(calls, 4)

    async def test_next_call_after_pass_end_starts_new_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _touch(tmp, 'a')
            listing: StreamingListing = StreamingListing(tmp, chunk_size=10)
            first, ended = await listing.next_chunk()
            self.assertTrue(ended)
            _touch(tmp, 'b')
            second, ended_again = await listing.next_chunk()
            listing.close()

        self.assertEqual(first, ['a'])
        self.assertEqual(sorted(second), ['a', 'b'])
        self.assertTrue(ended_again)

    async def test_skips_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _touch(tmp, 'a')
            os.mkdir(os.path.join(tmp, 'uploaded'))
            os.mkdir(os.path.join(tmp, '.bulk'))
            listing: StreamingListing = StreamingListing(tmp)
            names, _ended = await listing.next_chunk()
            listing.close()

        self.assertEqual(names, ['a'])

    async def test_close_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _touch(tmp, 'a', 'b')
            listing: StreamingListing = StreamingListing(tmp, chunk_size=1)
            await listing.next_chunk()
            listing.close()
            listing.close()

    async def test_read_error_closes_iterator_and_reraises(self) -> None:
        iterator: _FailingIterator = _FailingIterator()
        listing: StreamingListing = StreamingListing('.', chunk_size=10)
        listing._iterator = iterator
        with self.assertRaises(OSError):
            await listing.next_chunk()

        self.assertTrue(iterator.closed)
        self.assertIsNone(listing._iterator)


class TestCountMatchingFiles(unittest.TestCase):

    def test_counts_only_matching_regular_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _touch(tmp, 'asset-1.json.br', 'asset-2.json.br', 'x.tmp')
            os.mkdir(os.path.join(tmp, 'uploaded'))
            count: int = count_matching_files(
                tmp, lambda name: name.endswith('.json.br'),
            )

        self.assertEqual(count, 2)


if __name__ == '__main__':
    unittest.main()
