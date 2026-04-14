'''
Unit tests for tools/merge_channel_maps.py.
'''

import os
import tempfile
import unittest

from tools.merge_channel_maps import (
    read_channel_map,
    write_channel_map,
)


class TestReadChannelMap(unittest.TestCase):

    def test_reads_valid_csv(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            fd.write('UC123,@handle1\n')
            fd.write('UC456,@handle2\n')
            path: str = fd.name

        try:
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(result, {
                'UC123': '@handle1',
                'UC456': '@handle2',
            })
        finally:
            os.unlink(path)

    def test_skips_blank_and_comment_lines(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            fd.write('# header comment\n')
            fd.write('\n')
            fd.write('UC123,@handle1\n')
            fd.write('  \n')
            path: str = fd.name

        try:
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(result, {'UC123': '@handle1'})
        finally:
            os.unlink(path)

    def test_skips_malformed_lines(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            fd.write('UC123,@handle1\n')
            fd.write('malformed_no_comma\n')
            fd.write('UC456,@handle2\n')
            path: str = fd.name

        try:
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(result, {
                'UC123': '@handle1',
                'UC456': '@handle2',
            })
        finally:
            os.unlink(path)

    def test_last_writer_wins_on_duplicate_ids(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            fd.write('UC123,@old_handle\n')
            fd.write('UC123,@new_handle\n')
            path: str = fd.name

        try:
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(
                result, {'UC123': '@new_handle'},
            )
        finally:
            os.unlink(path)

    def test_strips_whitespace(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            fd.write('  UC123 , @handle1  \n')
            path: str = fd.name

        try:
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(result, {'UC123': '@handle1'})
        finally:
            os.unlink(path)


class TestWriteChannelMap(unittest.TestCase):

    def test_writes_sorted_by_handle(self) -> None:
        channel_map: dict[str, str] = {
            'UC999': '@zebra',
            'UC111': '@alpha',
            'UC555': '@middle',
        }
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False,
        ) as fd:
            path: str = fd.name

        try:
            write_channel_map(path, channel_map)
            with open(path, 'r') as fd:
                lines: list[str] = fd.readlines()
            self.assertEqual(lines, [
                'UC111,@alpha\n',
                'UC555,@middle\n',
                'UC999,@zebra\n',
            ])
        finally:
            os.unlink(path)

    def test_roundtrip(self) -> None:
        original: dict[str, str] = {
            'UC1': '@one',
            'UC2': '@two',
            'UC3': '@three',
        }
        with tempfile.NamedTemporaryFile(
            suffix='.csv', delete=False,
        ) as fd:
            path: str = fd.name

        try:
            write_channel_map(path, original)
            result: dict[str, str] = read_channel_map(path)
            self.assertEqual(result, original)
        finally:
            os.unlink(path)


class TestMainMerge(unittest.TestCase):

    def test_merges_and_renames(self) -> None:
        tmpdir: str = tempfile.mkdtemp()
        file1: str = os.path.join(tmpdir, 'map.csv')
        file2: str = os.path.join(tmpdir, 'map2.csv')

        with open(file1, 'w') as fd:
            fd.write('UC1,@one\n')
            fd.write('UC2,@two\n')

        with open(file2, 'w') as fd:
            fd.write('UC2,@two_updated\n')
            fd.write('UC3,@three\n')

        try:
            from unittest.mock import patch
            import sys
            with patch.object(
                sys, 'argv',
                ['merge_channel_maps.py', file1, file2],
            ):
                from tools.merge_channel_maps import main
                main()

            # Original file1 should be renamed
            self.assertFalse(
                any(
                    f == 'map.csv'
                    for f in os.listdir(tmpdir)
                    if not f.startswith('map-')
                    and f != 'map.csv'
                ),
            )

            # The merged output should be at file1's path
            self.assertTrue(os.path.isfile(file1))
            result: dict[str, str] = read_channel_map(
                file1,
            )
            self.assertEqual(result, {
                'UC1': '@one',
                'UC2': '@two_updated',
                'UC3': '@three',
            })

            # Backup should exist
            backup_files: list[str] = [
                f for f in os.listdir(tmpdir)
                if f.startswith('map-')
            ]
            self.assertEqual(len(backup_files), 1)
        finally:
            import shutil
            shutil.rmtree(tmpdir)


if __name__ == '__main__':
    unittest.main()
