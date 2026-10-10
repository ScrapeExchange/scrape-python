'''
Chunked, resumable listing of one directory.

Upload directories hold millions of files. Building the full name list
costs ~1 GB of strings and ~18 s per listing; :class:`StreamingListing`
keeps one ``os.scandir`` iterator open and hands out a few thousand
names at a time, so memory stays at about one chunk and the first batch
can start immediately. :func:`count_matching_files` counts without
keeping any names.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import os
from collections.abc import Callable, Iterator
from pathlib import Path

LISTING_CHUNK_SIZE: int = 5_000


class StreamingListing:
    '''Hand out the regular-file names of *directory* in chunks.

    One pass walks the directory once. :meth:`next_chunk` reports the
    end of a pass by returning ``True`` as its second value; the call
    after that starts a new pass, so files created during a pass are
    seen by the next one at the latest. Reads run in a worker thread.
    '''

    def __init__(
        self, directory: str | Path, chunk_size: int = LISTING_CHUNK_SIZE,
    ) -> None:
        if chunk_size < 1:
            raise ValueError('chunk_size must be positive')
        self._directory: str = str(directory)
        self._chunk_size: int = chunk_size
        self._iterator: Iterator[os.DirEntry] | None = None

    async def next_chunk(self) -> tuple[list[str], bool]:
        '''Up to ``chunk_size`` names and whether the pass ended.'''
        return await asyncio.to_thread(self._read_chunk)

    def _read_chunk(self) -> tuple[list[str], bool]:
        if self._iterator is None:
            self._iterator = os.scandir(self._directory)
        names: list[str] = []
        entry: os.DirEntry
        try:
            for entry in self._iterator:
                if not entry.is_file():
                    continue
                names.append(entry.name)
                if len(names) >= self._chunk_size:
                    return names, False
        except OSError:
            # Do not leave a broken pass open; the next call starts
            # a new one.
            self.close()
            raise
        self.close()
        return names, True

    def close(self) -> None:
        '''Close the open pass, if any. Safe to call repeatedly.'''
        iterator: Iterator[os.DirEntry] | None = self._iterator
        self._iterator = None
        if iterator is not None:
            iterator.close()


def count_matching_files(
    directory: str | Path, predicate: Callable[[str], bool],
) -> int:
    '''Number of regular files in *directory* whose name matches
    *predicate*. Keeps no names; run it in a worker thread.'''
    count: int = 0
    entry: os.DirEntry
    with os.scandir(directory) as iterator:
        for entry in iterator:
            if predicate(entry.name) and entry.is_file():
                count += 1
    return count
