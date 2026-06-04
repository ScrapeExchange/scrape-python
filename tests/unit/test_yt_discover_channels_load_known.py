'''Unit tests for ``tools.yt_discover_channels.load_known_channels``.

The function has two branches:

* ``REDIS_DSN`` set → pull all (channel_id, handle) pairs from
  the ``creator_map`` and return the set of lowercased,
  ``@``-stripped handles.
* ``REDIS_DSN`` unset → fall back to reading
  ``settings.channel_list`` (the legacy file path).

Both branches are exercised here.
'''

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from scrape_exchange.creator_map import FileCreatorMap

from tools.yt_discover_channels import (
    _normalise_known_handle,
    load_known_channels,
)


class TestNormaliseKnownHandle(unittest.TestCase):

    def test_strips_leading_at(self) -> None:
        self.assertEqual(
            _normalise_known_handle('@LinusTechTips'),
            'linustechtips',
        )

    def test_strips_url_prefix(self) -> None:
        self.assertEqual(
            _normalise_known_handle(
                'https://www.youtube.com/@veritasium',
            ),
            'veritasium',
        )

    def test_lowercases(self) -> None:
        self.assertEqual(
            _normalise_known_handle('MixedCaseHandle'),
            'mixedcasehandle',
        )

    def test_returns_empty_for_at_mid_string(self) -> None:
        # Trailing-@ left over after stripping is treated
        # as malformed; matches the legacy file-loader's
        # 'skip' behaviour.
        self.assertEqual(
            _normalise_known_handle('foo@bar'), '',
        )

    def test_strips_whitespace(self) -> None:
        self.assertEqual(
            _normalise_known_handle('  @SomeHandle  '),
            'somehandle',
        )


class TestLoadKnownChannelsRedisBranch(
    unittest.IsolatedAsyncioTestCase,
):
    '''When ``settings.redis_dsn`` is truthy, the function
    walks the creator_map (any CreatorMap backend works;
    we use FileCreatorMap against a temp file for
    simplicity) and ignores ``settings.channel_list``.
    '''

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.cmap_path: str = os.path.join(
            self._tmp.name, 'creator_map.csv',
        )
        self.cmap: FileCreatorMap = FileCreatorMap(
            self.cmap_path,
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_returns_normalised_handles_from_creator_map(
        self,
    ) -> None:
        await self.cmap.put(
            'UCaaaaaaaaaaaaaaaaaaaaaa', '@LinusTechTips',
        )
        await self.cmap.put(
            'UCbbbbbbbbbbbbbbbbbbbbbb', 'Veritasium',
        )
        await self.cmap.put(
            'UCcccccccccccccccccccccc', '@MKBHD',
        )
        settings: MagicMock = MagicMock()
        settings.redis_dsn = 'redis://localhost:6379/0'
        settings.channel_list = '/nonexistent/path.lst'

        known: set[str] = await load_known_channels(
            settings, self.cmap,
        )

        self.assertEqual(known, {
            'linustechtips', 'veritasium', 'mkbhd',
        })

    async def test_empty_creator_map_returns_empty_set(
        self,
    ) -> None:
        settings: MagicMock = MagicMock()
        settings.redis_dsn = 'redis://localhost:6379/0'
        settings.channel_list = '/nonexistent/path.lst'

        known: set[str] = await load_known_channels(
            settings, self.cmap,
        )
        self.assertEqual(known, set())

    async def test_redis_branch_ignores_channel_list_file(
        self,
    ) -> None:
        '''A populated channel_list file must NOT be
        consulted when REDIS_DSN is set — the function
        is supposed to delegate to creator_map entirely.
        '''
        lst_path: str = os.path.join(
            self._tmp.name, 'channels.lst',
        )
        with open(lst_path, 'w') as fh:
            fh.write('@from-file-only\n')
        settings: MagicMock = MagicMock()
        settings.redis_dsn = 'redis://localhost:6379/0'
        settings.channel_list = lst_path
        # creator_map has different content
        await self.cmap.put(
            'UCxxxxxxxxxxxxxxxxxxxxxx', '@from-redis',
        )

        known: set[str] = await load_known_channels(
            settings, self.cmap,
        )
        self.assertIn('from-redis', known)
        self.assertNotIn('from-file-only', known)


class TestLoadKnownChannelsFileBranch(
    unittest.IsolatedAsyncioTestCase,
):
    '''When ``settings.redis_dsn`` is falsy, the function
    falls back to reading ``settings.channel_list`` (the
    legacy file path). Creator_map is not consulted.
    '''

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_reads_file_when_redis_dsn_unset(
        self,
    ) -> None:
        lst_path: str = os.path.join(
            self._tmp.name, 'channels.lst',
        )
        with open(lst_path, 'w') as fh:
            fh.write('@FirstChannel\n')
            fh.write('SecondChannel\n')
            fh.write('# comment line ignored\n')
            fh.write('\n')
        settings: MagicMock = MagicMock()
        settings.redis_dsn = None
        settings.channel_list = lst_path
        # creator_map MUST NOT be consulted; pass a
        # MagicMock that would error on get_all.
        cmap: MagicMock = MagicMock()
        cmap.get_all = MagicMock(
            side_effect=AssertionError(
                'creator_map.get_all() must not be called '
                'in the file branch',
            ),
        )

        known: set[str] = await load_known_channels(
            settings, cmap,
        )
        self.assertEqual(
            known, {'firstchannel', 'secondchannel'},
        )

    async def test_missing_file_returns_empty_set(
        self,
    ) -> None:
        settings: MagicMock = MagicMock()
        settings.redis_dsn = ''
        settings.channel_list = '/nonexistent/path.lst'
        cmap: MagicMock = MagicMock()
        known: set[str] = await load_known_channels(
            settings, cmap,
        )
        self.assertEqual(known, set())


if __name__ == '__main__':
    unittest.main()
