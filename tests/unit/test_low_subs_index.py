'''
The SQLite index of ``.low_subs`` video files, mapping each file to its
channel so the video re-check does not re-open millions of files.
'''

import os
import tempfile
import unittest

from scrape_exchange.youtube.low_subs_index import LowSubsIndex

A1: str = 'video-min-aaaaaaaaaaa.json.br.low_subs'
A2: str = 'video-min-bbbbbbbbbbb.json.br.low_subs'
B1: str = 'video-min-ccccccccccc.json.br.low_subs'


class TestLowSubsIndex(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.path: str = os.path.join(self.tmp.name, 'index.sqlite')
        self.index: LowSubsIndex = LowSubsIndex(self.path)
        self.addCleanup(self.index.close)
        self.index.add([
            (A1, 'aaaaaaaaaaa', 'UCa'),
            (A2, 'bbbbbbbbbbb', 'UCa'),
            (B1, 'ccccccccccc', 'UCb'),
        ])

    def test_mark_seen_returns_indexed(self) -> None:
        self.index.start_scan()
        self.assertEqual(
            self.index.mark_seen(
                [A1, B1, 'video-min-zzz.json.br.low_subs'],
            ),
            {A1, B1},
        )

    def test_files_for_channel(self) -> None:
        self.assertEqual(
            sorted(self.index.files_for_channel('UCa')), [A1, A2],
        )
        self.assertEqual(self.index.files_for_channel('UCz'), [])

    def test_remove(self) -> None:
        self.index.remove([A1])
        self.assertEqual(self.index.files_for_channel('UCa'), [A2])
        self.assertEqual(self.index.mark_seen([A1]), set())

    def test_prune_drops_unseen(self) -> None:
        self.index.start_scan()
        self.index.mark_seen([A1, B1])
        self.assertEqual(self.index.prune_unseen(), 1)
        self.assertEqual(self.index.files_for_channel('UCa'), [A1])

    def test_rows_added_during_scan_survive_prune(self) -> None:
        self.index.start_scan()
        name: str = 'video-min-ddddddddddd.json.br.low_subs'
        self.index.add([(name, 'ddddddddddd', 'UCd')])
        self.index.prune_unseen()
        self.assertEqual(self.index.files_for_channel('UCd'), [name])

    def test_reopen_persists(self) -> None:
        self.index.close()
        reopened: LowSubsIndex = LowSubsIndex(self.path)
        self.addCleanup(reopened.close)
        self.assertEqual(
            reopened.mark_seen([A1, A2, B1]), {A1, A2, B1},
        )

    def test_null_channel_id_stored(self) -> None:
        name: str = 'video-min-ddddddddddd.json.br.low_subs'
        self.index.add([(name, 'ddddddddddd', None)])
        self.assertEqual(self.index.mark_seen([name]), {name})

    def test_mark_seen_handles_many_names(self) -> None:
        names: list[str] = [f'video-min-{i:011d}' for i in range(1200)]
        self.index.add([(name, name, 'UCc') for name in names])
        self.assertEqual(len(self.index.mark_seen(names)), 1200)

if __name__ == '__main__':
    unittest.main()
