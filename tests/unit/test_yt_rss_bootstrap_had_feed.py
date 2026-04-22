'''
Unit tests for tools/yt_rss_bootstrap_had_feed.py —
the one-off had_feed seeder.
'''

import os
import tempfile
import unittest

from tools.yt_rss_bootstrap_had_feed import (
    discover_channel_ids,
)


class TestDiscoverChannelIds(unittest.TestCase):

    def _touch(self, path: str) -> None:
        os.makedirs(
            os.path.dirname(path), exist_ok=True,
        )
        with open(path, 'w') as f:
            f.write('')

    def test_extracts_channel_id_from_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._touch(os.path.join(
                tmp,
                'channel-UCabcdefghijklmnopqrstuv.json.br',
            ))
            ids: list[str] = discover_channel_ids(tmp)
            self.assertEqual(
                ids,
                ['UCabcdefghijklmnopqrstuv'],
            )

    def test_walks_subdirectories_recursively(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._touch(os.path.join(
                tmp,
                'channel-UCaaaaaaaaaaaaaaaaaaaaaa.json.br',
            ))
            self._touch(os.path.join(
                tmp, 'uploaded',
                'channel-UCbbbbbbbbbbbbbbbbbbbbbb.json.br',
            ))
            ids: list[str] = discover_channel_ids(tmp)
            self.assertEqual(
                ids,
                [
                    'UCaaaaaaaaaaaaaaaaaaaaaa',
                    'UCbbbbbbbbbbbbbbbbbbbbbb',
                ],
            )

    def test_duplicates_across_dirs_collapse(self) -> None:
        '''A channel file that appears both in the root
        and in /uploaded must not be counted twice.'''
        with tempfile.TemporaryDirectory() as tmp:
            name: str = (
                'channel-UCcccccccccccccccccccccc.json.br'
            )
            self._touch(os.path.join(tmp, name))
            self._touch(os.path.join(
                tmp, 'uploaded', name,
            ))
            ids: list[str] = discover_channel_ids(tmp)
            self.assertEqual(
                ids, ['UCcccccccccccccccccccccc'],
            )

    def test_ignores_non_channel_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._touch(os.path.join(
                tmp,
                'channel-UCddddddddddddddddddddd.json.br',
            ))  # 21 chars — too short
            self._touch(os.path.join(
                tmp, 'video-min-abc123.json.br',
            ))
            self._touch(os.path.join(
                tmp,
                'channel-UCeeeeeeeeeeeeeeeeeeeeee.json',
            ))  # wrong extension
            self._touch(os.path.join(
                tmp,
                'channel-UCfffffffffffffffffffffff.json.br',
            ))  # 23 chars — too long
            ids: list[str] = discover_channel_ids(tmp)
            self.assertEqual(ids, [])

    def test_returns_sorted_unique_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            names: list[str] = [
                'channel-UCzzzzzzzzzzzzzzzzzzzzzz.json.br',
                'channel-UCaaaaaaaaaaaaaaaaaaaaaa.json.br',
                'channel-UCmmmmmmmmmmmmmmmmmmmmmm.json.br',
            ]
            for n in names:
                self._touch(os.path.join(tmp, n))
            ids: list[str] = discover_channel_ids(tmp)
            self.assertEqual(
                ids,
                [
                    'UCaaaaaaaaaaaaaaaaaaaaaa',
                    'UCmmmmmmmmmmmmmmmmmmmmmm',
                    'UCzzzzzzzzzzzzzzzzzzzzzz',
                ],
            )

    def test_empty_directory_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                discover_channel_ids(tmp), [],
            )


if __name__ == '__main__':
    unittest.main()
