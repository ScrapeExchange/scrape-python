'''Unit tests for tools.yt_resolve_channel_ids.'''

import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.redis_claim import RedisClaim
from tools.yt_resolve_channel_ids import (
    ResolveChannelIdsSettings,
    _make_filter_api_page_fn,
    _mode_setting_conflict,
    run_resolver,
)


def _write_jsonl(
    path: str, records: list[dict],
) -> None:
    with open(path, 'w') as f:
        for r in records:
            f.write(json.dumps(r) + '\n')


def _write_lst(path: str, lines: list[str]) -> None:
    with open(path, 'w') as f:
        for line in lines:
            f.write(line + '\n')


class TestRunResolverEmptyInput(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
        self.tmpdir: str = tempfile.mkdtemp()

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_run_with_empty_inputs_exits_clean(
        self,
    ) -> None:
        empty_path: str = os.path.join(
            self.tmpdir, 'empty.jsonl',
        )
        open(empty_path, 'w').close()
        settings: ResolveChannelIdsSettings = (
            ResolveChannelIdsSettings(
                _env_file=None,
                _cli_parse_args=[],
                input_paths=[empty_path],
                redis_dsn=None,
                concurrency=2,
                channel_data_directory=self.tmpdir,
                worker_id='test-worker',
            )
        )
        stats: dict[str, int] = await run_resolver(
            settings,
            redis_client=self.redis,
            creator_map_backend=self.cm,
        )
        self.assertEqual(stats['processed'], 0)
        self.assertEqual(stats['resolved'], 0)
        self.assertEqual(stats['skipped'], 0)


class TestInputParsing(
    unittest.IsolatedAsyncioTestCase,
):

    def setUp(self) -> None:
        self.tmpdir: str = tempfile.mkdtemp()

    async def test_jsonl_input_yields_channel_ids(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _iter_channel_ids_from_paths,
        )
        path: str = os.path.join(
            self.tmpdir, 'in.jsonl',
        )
        _write_jsonl(path, [
            {
                'channel_id': 'UC111',
                'channel': 'alpha',
            },
            {'channel_id': 'UC222'},
            {'channel': 'gamma'},
        ])
        ids: set[str] = _iter_channel_ids_from_paths(
            [path],
        )
        self.assertEqual(ids, {'UC111', 'UC222'})

    async def test_lst_input_yields_uc_ids(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _iter_channel_ids_from_paths,
        )
        path: str = os.path.join(
            self.tmpdir, 'in.lst',
        )
        _write_lst(path, [
            '# header',
            'UCabcdefghijklmnopqrstuv',
            '@somehandle',
            'UCxyzxyzxyzxyzxyzxyzxyzx',
        ])
        ids: set[str] = _iter_channel_ids_from_paths(
            [path],
        )
        self.assertEqual(
            ids,
            {
                'UCabcdefghijklmnopqrstuv',
                'UCxyzxyzxyzxyzxyzxyzxyzx',
            },
        )

    async def test_missing_path_silently_skipped(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _iter_channel_ids_from_paths,
        )
        ids: set[str] = _iter_channel_ids_from_paths(
            [os.path.join(self.tmpdir, 'nope.lst')],
        )
        self.assertEqual(ids, set())


class TestCreatorMapFilter(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
        self.tmpdir: str = tempfile.mkdtemp()

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_already_resolved_ids_are_skipped(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _filter_unresolved,
        )
        await self.cm.put('UC111', 'alpha')
        unresolved: set[str] = (
            await _filter_unresolved(
                {'UC111', 'UC222', 'UC333'},
                creator_map_backend=self.cm,
            )
        )
        self.assertEqual(
            unresolved, {'UC222', 'UC333'},
        )

    async def test_empty_input_returns_empty(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _filter_unresolved,
        )
        unresolved: set[str] = (
            await _filter_unresolved(
                set(),
                creator_map_backend=self.cm,
            )
        )
        self.assertEqual(unresolved, set())


class TestResolveOne(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
        self.tmpdir: str = tempfile.mkdtemp()
        self.claim: RedisClaim = RedisClaim(
            self.redis,
            key_prefix='youtube:resolving:',
            ttl_seconds=60,
            owner='test',
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_successful_resolution_writes_creator_map(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )

        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value='resolvedhandle'),
        ):
            outcome: str = await _resolve_one(
                'UC111',
                creator_map_backend=self.cm,
                claim=self.claim,
                channel_data_directory=self.tmpdir,
            )

        self.assertEqual(outcome, 'resolved')
        self.assertEqual(
            await self.cm.get('UC111'),
            'resolvedhandle',
        )

    async def test_lost_claim_skips(self) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )
        await self.redis.set(
            'youtube:resolving:UC222', 'other-owner',
        )
        outcome: str = await _resolve_one(
            'UC222',
            creator_map_backend=self.cm,
            claim=self.claim,
            channel_data_directory=self.tmpdir,
        )
        self.assertEqual(outcome, 'lost_claim')

    async def test_already_resolved_after_recheck(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )

        await self.cm.put('UC333', 'preresolved')
        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value='shouldnotcall'),
        ) as mock:
            outcome: str = await _resolve_one(
                'UC333',
                creator_map_backend=self.cm,
                claim=self.claim,
                channel_data_directory=self.tmpdir,
            )
        self.assertEqual(outcome, 'already_resolved')
        mock.assert_not_called()

    async def test_unresolvable_marker_short_circuits(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )
        marker: str = os.path.join(
            self.tmpdir, 'channel-UC444.unresolved',
        )
        with open(marker, 'w') as f:
            f.write('UC444\n')
        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value='nope'),
        ) as mock:
            outcome: str = await _resolve_one(
                'UC444',
                creator_map_backend=self.cm,
                claim=self.claim,
                channel_data_directory=self.tmpdir,
            )
        self.assertEqual(outcome, 'unresolvable')
        mock.assert_not_called()

    async def test_innertube_returns_none_marks_unresolvable(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )
        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value=None),
        ):
            outcome: str = await _resolve_one(
                'UC555',
                creator_map_backend=self.cm,
                claim=self.claim,
                channel_data_directory=self.tmpdir,
            )
        self.assertEqual(outcome, 'unresolvable')
        marker: str = os.path.join(
            self.tmpdir, 'channel-UC555.unresolved',
        )
        self.assertTrue(os.path.exists(marker))

    async def test_release_after_resolution(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _resolve_one,
        )
        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value='handle'),
        ):
            await _resolve_one(
                'UC666',
                creator_map_backend=self.cm,
                claim=self.claim,
                channel_data_directory=self.tmpdir,
            )
        # After release, the claim key is gone.
        self.assertIsNone(
            await self.redis.get(
                'youtube:resolving:UC666',
            ),
        )


class TestRunResolverEndToEnd(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
        self.tmpdir: str = tempfile.mkdtemp()

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_resolves_unresolved_writes_creator_map(
        self,
    ) -> None:
        path: str = os.path.join(
            self.tmpdir, 'in.jsonl',
        )
        _write_jsonl(path, [
            {'channel_id': 'UC111'},
            {'channel_id': 'UC222'},
        ])

        async def fake_resolve(
            cid: str, *args, **kwargs,
        ) -> str:
            return f'handle-for-{cid.lower()}'

        settings: ResolveChannelIdsSettings = (
            ResolveChannelIdsSettings(
                _env_file=None,
                _cli_parse_args=[],
                input_paths=[path],
                redis_dsn=None,
                concurrency=4,
                channel_data_directory=self.tmpdir,
                worker_id='test',
            )
        )
        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(side_effect=fake_resolve),
        ):
            stats: dict[str, int] = await run_resolver(
                settings,
                redis_client=self.redis,
                creator_map_backend=self.cm,
            )
        self.assertEqual(stats['processed'], 2)
        self.assertEqual(stats['resolved'], 2)
        self.assertEqual(
            await self.cm.get('UC111'),
            'handle-for-uc111',
        )
        self.assertEqual(
            await self.cm.get('UC222'),
            'handle-for-uc222',
        )


class TestWarmFromApi(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        from scrape_exchange.youtube.exchange_channels_set import (
            RedisExchangeChannelsSet,
        )
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.s: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(self.redis)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_warm_stops_after_no_new_handles(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _warm_from_api,
        )

        pages: list[list[str]] = [
            ['alpha', 'bravo', 'charlie'],
            ['alpha', 'bravo', 'charlie'],
            ['alpha', 'bravo', 'charlie'],
            ['delta'],
        ]
        index: list[int] = [0]

        async def fake_page(
            cursor: str | None,
        ) -> tuple[list[str], str | None]:
            i: int = index[0]
            index[0] = i + 1
            return pages[i], f'cursor{i + 1}'

        await _warm_from_api(
            self.s,
            page_fn=fake_page,
            stop_after_no_new_pages=2,
        )

        members: dict[str, bool] = (
            await self.s.contains_many(
                ['alpha', 'bravo', 'charlie', 'delta'],
            )
        )
        self.assertEqual(
            members,
            {
                'alpha': True,
                'bravo': True,
                'charlie': True,
                'delta': False,
            },
        )

    async def test_warm_terminates_on_empty_page(
        self,
    ) -> None:
        from tools.yt_resolve_channel_ids import (
            _warm_from_api,
        )

        pages: list[list[str]] = [
            ['alpha'],
            [],
        ]
        index: list[int] = [0]

        async def fake_page(
            cursor: str | None,
        ) -> tuple[list[str], str | None]:
            i: int = index[0]
            index[0] = i + 1
            return pages[i], None

        added: int = await _warm_from_api(
            self.s,
            page_fn=fake_page,
            stop_after_no_new_pages=3,
        )
        self.assertEqual(added, 1)
        self.assertEqual(await self.s.size(), 1)


class TestModeSettingConflict(unittest.TestCase):
    '''_mode_setting_conflict catches mode/flag mismatches at
    startup so a typo'd run doesn't silently no-op (e.g. user
    passes --warmer-username but forgets --mode warm-exchange-set
    and the resolve mode then exits cleanly with no input).'''

    def _settings(
        self, args: list[str],
    ) -> ResolveChannelIdsSettings:
        return ResolveChannelIdsSettings(
            _env_file=None,
            _cli_parse_args=args,
        )

    def test_default_resolve_no_flags_is_ok(self) -> None:
        s: ResolveChannelIdsSettings = self._settings([])
        self.assertIsNone(_mode_setting_conflict(s))

    def test_warmer_mode_with_warmer_flags_is_ok(self) -> None:
        s: ResolveChannelIdsSettings = self._settings([
            '--mode', 'warm-exchange-set',
            '--warmer-username', 'drand',
        ])
        self.assertIsNone(_mode_setting_conflict(s))

    def test_warmer_flag_in_resolve_mode_errors(
        self,
    ) -> None:
        s: ResolveChannelIdsSettings = self._settings([
            '--warmer-username', 'drand',
        ])
        err: str | None = _mode_setting_conflict(s)
        self.assertIsNotNone(err)
        self.assertIn('warm-exchange-set', err)
        self.assertIn('warmer_username', err)

    def test_warmer_stop_flag_in_resolve_mode_errors(
        self,
    ) -> None:
        s: ResolveChannelIdsSettings = self._settings([
            '--warmer-stop-after-no-new-pages', '5',
        ])
        err: str | None = _mode_setting_conflict(s)
        self.assertIsNotNone(err)
        self.assertIn(
            'warmer_stop_after_no_new_pages', err,
        )

    def test_input_paths_in_warmer_mode_errors(
        self,
    ) -> None:
        s: ResolveChannelIdsSettings = self._settings([
            '--mode', 'warm-exchange-set',
            '--input-paths', '["/tmp/x.jsonl"]',
        ])
        err: str | None = _mode_setting_conflict(s)
        self.assertIsNotNone(err)
        self.assertIn('input_paths', err)
        self.assertIn('--mode resolve', err)

    def test_resolve_mode_with_input_paths_is_ok(
        self,
    ) -> None:
        s: ResolveChannelIdsSettings = self._settings([
            '--input-paths', '["/tmp/x.jsonl"]',
        ])
        self.assertIsNone(_mode_setting_conflict(s))


class TestMakeFilterApiPageFn(
    unittest.IsolatedAsyncioTestCase,
):
    '''Regression tests for the filter-API page parser. The
    server's GetDataResponseModel returns the channel handle in
    ``node.platform_creator_id`` (per the schema's x-scrape-field
    markers) — NOT ``channel_handle``. Reading the wrong key
    silently yields zero handles per page and the warmer's
    ``if not page: break`` exits after one POST.'''

    async def test_extracts_handle_from_platform_creator_id(
        self,
    ) -> None:
        from unittest.mock import AsyncMock, MagicMock

        client: MagicMock = MagicMock()
        client.post = AsyncMock()
        resp: MagicMock = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={
            'edges': [
                {'node': {'platform_creator_id': '@alpha'}},
                {'node': {'platform_creator_id': 'bravo'}},
                {'node': {'platform_creator_id': None}},
                {'node': {}},
            ],
            'page_info': {
                'has_next_page': True,
                'end_cursor': 'cursor-2',
            },
        })
        client.post.return_value = resp

        page_fn = _make_filter_api_page_fn(
            client, 'https://e.example', 'someuser',
        )
        handles, next_cursor = await page_fn(None)

        self.assertEqual(handles, ['alpha', 'bravo'])
        self.assertEqual(next_cursor, 'cursor-2')

    async def test_no_next_page_returns_none_cursor(
        self,
    ) -> None:
        from unittest.mock import AsyncMock, MagicMock

        client: MagicMock = MagicMock()
        client.post = AsyncMock()
        resp: MagicMock = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={
            'edges': [
                {'node': {'platform_creator_id': 'gamma'}},
            ],
            'page_info': {'has_next_page': False},
        })
        client.post.return_value = resp

        page_fn = _make_filter_api_page_fn(
            client, 'https://e.example', 'someuser',
        )
        handles, next_cursor = await page_fn('cursor-1')

        self.assertEqual(handles, ['gamma'])
        self.assertIsNone(next_cursor)

    async def test_passes_after_cursor_in_body(
        self,
    ) -> None:
        from unittest.mock import AsyncMock, MagicMock

        client: MagicMock = MagicMock()
        client.post = AsyncMock()
        resp: MagicMock = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={
            'edges': [],
            'page_info': {'has_next_page': False},
        })
        client.post.return_value = resp

        page_fn = _make_filter_api_page_fn(
            client, 'https://e.example', 'u',
        )
        await page_fn('the-cursor')

        _, kwargs = client.post.call_args
        body: dict = kwargs['json']
        self.assertEqual(body['after'], 'the-cursor')
        self.assertEqual(body['username'], 'u')
        self.assertEqual(body['platform'], 'youtube')
        self.assertEqual(body['entity'], 'channel')
