'''
Unit tests for tools/yt_discover_channels.py's
update_creator_map — the discovery-time writer to the
shared creator_map (file- or Redis-backed).

The tests use :class:`FileCreatorMap` against a temp
directory so no fakeredis dependency is needed; the
function talks to the :class:`CreatorMap` abstraction
and is backend-agnostic.
'''

import os
import tempfile
import unittest

from unittest.mock import patch

from scrape_exchange.creator_map import (
    FileCreatorMap,
    RedisCreatorMap,
)

from tools.yt_discover_channels import (
    DiscoverSettings,
    _handle_from_target,
    build_creator_map,
    update_creator_map,
)


class TestHandleFromTarget(unittest.TestCase):
    '''The helper that decides whether a scrape target is a
    genuine handle worth writing to creator_map.'''

    def test_plain_handle_returned_as_is(self) -> None:
        self.assertEqual(
            _handle_from_target('RickAstleyYT'),
            'RickAstleyYT',
        )

    def test_at_prefix_stripped(self) -> None:
        self.assertEqual(
            _handle_from_target('@RickAstleyYT'),
            'RickAstleyYT',
        )

    def test_channel_prefix_stripped(self) -> None:
        '''A ``channel/UC...`` target resolves to the
        UC-id after stripping, which is then rejected as
        not a handle.'''
        self.assertIsNone(
            _handle_from_target(
                'channel/UCuAXFkgsw1L7xaCfnd5JJOw',
            ),
        )

    def test_bare_uc_id_is_not_a_handle(self) -> None:
        self.assertIsNone(
            _handle_from_target(
                'UCuAXFkgsw1L7xaCfnd5JJOw',
            ),
        )

    def test_empty_string_returns_none(self) -> None:
        self.assertIsNone(_handle_from_target(''))

    def test_channel_prefix_plus_handle(self) -> None:
        '''Unusual but possible: a channel/Name target
        (non-UC) is stripped to the name and kept.'''
        self.assertEqual(
            _handle_from_target('channel/SomeHandle'),
            'SomeHandle',
        )


class TestUpdateCreatorMap(
    unittest.IsolatedAsyncioTestCase,
):
    '''update_creator_map writes to the supplied
    :class:`CreatorMap` only when given a real handle
    and a non-None map instance. The test uses
    :class:`FileCreatorMap` against a temp directory —
    :class:`RedisCreatorMap` shares the same
    abstraction, so the same contract holds.'''

    async def asyncSetUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addAsyncCleanup(self._tmp.cleanup)
        self._map_path: str = os.path.join(
            self._tmp.name, 'creator_map.csv',
        )
        self._cmap: FileCreatorMap = FileCreatorMap(
            self._map_path,
        )

    async def test_writes_handle_keyed_by_channel_id(
        self,
    ) -> None:
        await update_creator_map(
            self._cmap,
            'RickAstleyYT',
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        value: str | None = await self._cmap.get(
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        self.assertEqual(value, 'RickAstleyYT')

    async def test_at_prefix_stripped_before_write(
        self,
    ) -> None:
        await update_creator_map(
            self._cmap,
            '@RickAstleyYT',
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        value: str | None = await self._cmap.get(
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        self.assertEqual(value, 'RickAstleyYT')

    async def test_none_map_is_noop(self) -> None:
        # Must not raise.
        await update_creator_map(
            None,
            'RickAstleyYT',
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )

    async def test_missing_channel_id_is_noop(self) -> None:
        await update_creator_map(
            self._cmap, 'RickAstleyYT', None,
        )
        size: int = await self._cmap.size()
        self.assertEqual(size, 0)

    async def test_empty_channel_id_is_noop(self) -> None:
        await update_creator_map(
            self._cmap, 'RickAstleyYT', '',
        )
        size: int = await self._cmap.size()
        self.assertEqual(size, 0)

    async def test_bare_uc_id_target_is_noop(self) -> None:
        '''Writing UC-id → UC-id is never useful.'''
        await update_creator_map(
            self._cmap,
            'UCuAXFkgsw1L7xaCfnd5JJOw',
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        size: int = await self._cmap.size()
        self.assertEqual(size, 0)

    async def test_channel_prefix_uc_target_is_noop(
        self,
    ) -> None:
        '''Same for ``channel/UC...`` — after stripping
        the prefix we have a UC-id, which is not a
        handle.'''
        await update_creator_map(
            self._cmap,
            'channel/UCuAXFkgsw1L7xaCfnd5JJOw',
            'UCuAXFkgsw1L7xaCfnd5JJOw',
        )
        size: int = await self._cmap.size()
        self.assertEqual(size, 0)

    async def test_subsequent_write_overwrites(self) -> None:
        '''If the handle changed (e.g. a rename), the next
        discovery overwrites the stored value.'''
        await update_creator_map(
            self._cmap, 'OldHandle',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        await update_creator_map(
            self._cmap, 'NewHandle',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        value: str | None = await self._cmap.get(
            'UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        self.assertEqual(value, 'NewHandle')


class TestBuildCreatorMap(
    unittest.IsolatedAsyncioTestCase,
):
    '''build_creator_map returns a FileCreatorMap when no
    REDIS_DSN is configured, and a RedisCreatorMap — after
    a successful HLEN probe — when it is. A failed probe
    with REDIS_DSN set must sys.exit(1) rather than fall
    back to the file backend.'''

    async def test_no_redis_dsn_returns_file_backend(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings: DiscoverSettings = DiscoverSettings(
                _env_file=None,
                _cli_parse_args=[],
                redis_dsn=None,
                creator_map_file=os.path.join(
                    tmp, 'creator_map.csv',
                ),
            )
            cmap = await build_creator_map(settings)
            self.assertIsInstance(cmap, FileCreatorMap)

    async def test_redis_probe_success_returns_redis(
        self,
    ) -> None:
        settings: DiscoverSettings = DiscoverSettings(
            _env_file=None,
            _cli_parse_args=[],
            redis_dsn='redis://fake:6379/0',
        )
        with patch.object(
            RedisCreatorMap, 'size',
            return_value=0,
        ):
            cmap = await build_creator_map(settings)
        self.assertIsInstance(cmap, RedisCreatorMap)

    async def test_redis_probe_failure_exits(self) -> None:
        settings: DiscoverSettings = DiscoverSettings(
            _env_file=None,
            _cli_parse_args=[],
            redis_dsn='redis://unreachable:6379/0',
        )

        async def _boom(_self: object) -> int:
            raise ConnectionError('nope')

        with patch.object(
            RedisCreatorMap, 'size', new=_boom,
        ):
            with self.assertRaises(SystemExit) as ctx:
                await build_creator_map(settings)
        self.assertEqual(ctx.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
