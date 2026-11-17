'''
SQLite index of the ``.low_subs`` video files in a video data directory.

Each row maps one ``.low_subs`` file to its video and channel, so the
video re-check only opens a file once, when it first sees it, and can
find a released channel's files without reading the directory's
millions of files again.

A scan stamps every row it sees with the scan number; rows the scan did
not see belong to files that are gone and are pruned afterwards. This
keeps memory bounded: the directory listing is processed in chunks and
never held as one set.

The index is a local cache: deleting it only makes the next run re-read
every file.
'''

import sqlite3

# SQLite's default limit on host parameters per statement is 999.
CHUNK_SIZE: int = 500


def _chunks(items: list[str]) -> list[list[str]]:
    return [
        items[start:start + CHUNK_SIZE]
        for start in range(0, len(items), CHUNK_SIZE)
    ]


class LowSubsIndex:
    '''The ``.low_subs`` file index stored at *path*.'''

    def __init__(self, path: str) -> None:
        self._conn: sqlite3.Connection | None = sqlite3.connect(path)
        self._conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS files (
                filename TEXT PRIMARY KEY,
                video_id TEXT,
                channel_id TEXT,
                seen INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS files_channel
                ON files (channel_id);
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            '''
        )
        row: tuple | None = self._conn.execute(
            "SELECT value FROM state WHERE key = 'scan'",
        ).fetchone()
        self._scan: int = int(row[0]) if row else 0

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError('LowSubsIndex is closed')
        return self._conn

    def start_scan(self) -> None:
        '''Begin a directory scan: later :meth:`mark_seen` and
        :meth:`add` calls stamp rows with the new scan number.'''
        self._scan += 1
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO state (key, value) "
                "VALUES ('scan', ?)",
                (self._scan,),
            )

    def mark_seen(self, filenames: list[str]) -> set[str]:
        '''Stamp the indexed rows among *filenames* as seen in this
        scan; returns the names that were already indexed.'''
        known: set[str] = set()
        chunk: list[str]
        with self.conn:
            for chunk in _chunks(filenames):
                marks: str = ','.join('?' * len(chunk))
                known.update(
                    name for (name,) in self.conn.execute(
                        f'SELECT filename FROM files '
                        f'WHERE filename IN ({marks})',
                        chunk,
                    )
                )
                self.conn.execute(
                    f'UPDATE files SET seen = ? '
                    f'WHERE filename IN ({marks})',
                    [self._scan, *chunk],
                )
        return known

    def add(self, rows: list[tuple[str, str, str | None]]) -> None:
        '''Index ``(filename, video_id, channel_id)`` rows, stamped as
        seen in this scan.'''
        with self.conn:
            self.conn.executemany(
                'INSERT OR REPLACE INTO files '
                '(filename, video_id, channel_id, seen) '
                'VALUES (?, ?, ?, ?)',
                [(*row, self._scan) for row in rows],
            )

    def remove(self, filenames: list[str]) -> None:
        chunk: list[str]
        with self.conn:
            for chunk in _chunks(filenames):
                marks: str = ','.join('?' * len(chunk))
                self.conn.execute(
                    f'DELETE FROM files WHERE filename IN ({marks})',
                    chunk,
                )

    def files_for_channel(self, channel_id: str) -> list[str]:
        return [
            name for (name,) in self.conn.execute(
                'SELECT filename FROM files WHERE channel_id = ?',
                (channel_id,),
            )
        ]

    def prune_unseen(self) -> int:
        '''Drop the rows the current scan did not see: their files are
        gone. Returns the number dropped.'''
        with self.conn:
            cursor: sqlite3.Cursor = self.conn.execute(
                'DELETE FROM files WHERE seen != ?', (self._scan,),
            )
        return cursor.rowcount

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
