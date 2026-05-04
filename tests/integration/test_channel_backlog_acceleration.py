'''End-to-end: pre-resolver -> creator_map ->
_select_new_channels_via_set -> upload -> SET.

Lives under tests/integration because it threads four real
modules together (pre-resolver, creator_map, exchange-set
wrapper, bulk-upload reconciler). Uses fakeredis instead of a
docker container so the suite stays runnable without
infrastructure; the multi-module wiring is the actual coverage
this test adds over the per-module unit tests.
'''

import asyncio
import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis

from scrape_exchange.bulk_upload import apply_bulk_results
from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from tools.yt_channel_scrape import (
    _handle_from_channel_filename,
    _select_new_channels_via_set,
)
from tools.yt_resolve_channel_ids import (
    ResolveChannelIdsSettings,
    run_resolver,
)


class _StubFM:
    def __init__(self) -> None:
        self.uploaded: list[str] = []

    async def mark_uploaded(self, filename: str) -> None:
        self.uploaded.append(filename)


class TestEndToEndBacklogDrain(
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
        self.es: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(self.redis)
        )
        self.tmpdir: str = tempfile.mkdtemp()

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_end_to_end_drain(self) -> None:
        await self.cm.put('UC1', 'one')

        path: str = os.path.join(
            self.tmpdir, 'in.jsonl',
        )
        with open(path, 'w') as f:
            for cid in ['UC1', 'UC2', 'UC3']:
                f.write(
                    json.dumps({'channel_id': cid}) + '\n',
                )

        async def fake_resolve(
            cid: str, *args, **kwargs,
        ) -> str:
            return f'h-{cid.lower()}'

        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(side_effect=fake_resolve),
        ):
            stats: dict[str, int] = await run_resolver(
                ResolveChannelIdsSettings(
                    _env_file=None,
                    _cli_parse_args=[],
                    input_paths=[path],
                    redis_dsn=None,
                    concurrency=4,
                    channel_data_directory=self.tmpdir,
                    worker_id='resolver',
                ),
                redis_client=self.redis,
                creator_map_backend=self.cm,
            )

        self.assertEqual(stats['resolved'], 2)
        self.assertEqual(
            await self.cm.get('UC2'), 'h-uc2',
        )
        self.assertEqual(
            await self.cm.get('UC3'), 'h-uc3',
        )

        await self.es.add_many({'h-uc2'})

        candidates: list[str] = ['h-uc2', 'h-uc3', 'h-uc4']
        selected: set[str] = (
            await _select_new_channels_via_set(
                candidates, self.es,
                max_new_channels=10,
                already_resolved_count=0,
            )
        )
        self.assertEqual(selected, {'h-uc3', 'h-uc4'})

        fm: _StubFM = _StubFM()
        await apply_bulk_results(
            [
                ('UC3', 'channel-h-uc3.json.br'),
                ('UC4', 'channel-h-uc4.json.br'),
            ],
            [
                {
                    'platform_content_id': 'UC3',
                    'status': 'success',
                },
                {
                    'platform_content_id': 'UC4',
                    'status': 'failed',
                },
            ],
            fm,
            batch_id='b', job_id='j',
            exchange_set=self.es,
            handle_from_filename=_handle_from_channel_filename,
        )

        members: dict[str, bool] = (
            await self.es.contains_many(
                ['h-uc2', 'h-uc3', 'h-uc4'],
            )
        )
        self.assertEqual(
            members,
            {
                'h-uc2': True,
                'h-uc3': True,
                'h-uc4': False,
            },
        )

    async def test_concurrent_resolvers_do_not_double_call(
        self,
    ) -> None:
        path1: str = os.path.join(
            self.tmpdir, 'a.jsonl',
        )
        path2: str = os.path.join(
            self.tmpdir, 'b.jsonl',
        )
        for p in (path1, path2):
            with open(p, 'w') as f:
                f.write(
                    json.dumps(
                        {'channel_id': 'UC9'},
                    ) + '\n',
                )

        call_count: int = 0
        lock: asyncio.Lock = asyncio.Lock()

        async def fake_resolve(
            cid: str, *args, **kwargs,
        ) -> str:
            nonlocal call_count
            async with lock:
                call_count += 1
            await asyncio.sleep(0.05)
            return 'singleton'

        with patch(
            'tools.yt_resolve_channel_ids'
            '.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(side_effect=fake_resolve),
        ):
            await asyncio.gather(
                run_resolver(
                    ResolveChannelIdsSettings(
                        _env_file=None,
                        _cli_parse_args=[],
                        input_paths=[path1],
                        redis_dsn=None,
                        concurrency=2,
                        channel_data_directory=(
                            self.tmpdir
                        ),
                        worker_id='A',
                    ),
                    redis_client=self.redis,
                    creator_map_backend=self.cm,
                ),
                run_resolver(
                    ResolveChannelIdsSettings(
                        _env_file=None,
                        _cli_parse_args=[],
                        input_paths=[path2],
                        redis_dsn=None,
                        concurrency=2,
                        channel_data_directory=(
                            self.tmpdir
                        ),
                        worker_id='B',
                    ),
                    redis_client=self.redis,
                    creator_map_backend=self.cm,
                ),
            )
        self.assertEqual(call_count, 1)
        self.assertEqual(
            await self.cm.get('UC9'), 'singleton',
        )
