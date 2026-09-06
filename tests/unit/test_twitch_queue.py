'''Twitch queue operations against in-memory Redis.'''

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import fakeredis.aioredis

from scrape_exchange.creator_queue import RedisCreatorQueue
from scrape_exchange.queue_admin import get_adapter
from scrape_exchange.twitch.endpoints import PROFILE_BASE_URL


class TestTwitchQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.settings: SimpleNamespace = SimpleNamespace(
            redis_dsn='redis://localhost', worker_id='test',
            twitch_creator_priority_queues='24:1000,168:0',
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def adapter(self):
        with patch(
            'scrape_exchange.creator_queue.redis_from_url',
            return_value=self.redis,
        ):
            return get_adapter('twitch', 'creator', self.settings)

    async def test_add_deduplicate_and_rescrape_normalized_handles(
        self,
    ) -> None:
        adapter = self.adapter()
        self.assertEqual(await adapter.add([
            ('@Example', 0), (f'{PROFILE_BASE_URL}/example/about', 0),
            ('https://scrape.exchange/other', 0),
        ]), 1)
        record: dict = await adapter.show('@Example')
        self.assertEqual(record['creator_id'], 'example')
        self.assertEqual(await adapter.rescrape(['@Example']), 1)
        await adapter.remove('@Example')
        self.assertEqual((await adapter.show('example'))['state'], 'removed')
        keys: list[str] = await self.redis.keys('*')
        self.assertTrue(all(key.startswith('scrape:twitch:') for key in keys))

    async def test_import_rejects_null_instead_of_username_none(self) -> None:
        adapter = self.adapter()
        with tempfile.TemporaryDirectory() as directory:
            path: Path = Path(directory) / 'creators.jsonl'
            path.write_text(
                '{"username":null}\n{"username":"Example"}\n'
                '@example\n# comment\n\nbad/name\n',
            )
            report = await adapter.import_members(str(path))
        self.assertEqual(report.added, 1)
        self.assertEqual(report.duplicates, 1)
        self.assertEqual(report.invalid, 2)

    async def test_unknown_success_count_preserves_last_observation(
        self,
    ) -> None:
        with patch(
            'scrape_exchange.creator_queue.redis_from_url',
            return_value=self.redis,
        ):
            queue: RedisCreatorQueue = RedisCreatorQueue(
                'redis://localhost', 'test', 'twitch',
                key_namespace='scrape',
            )
        await queue.record_scrape_success('example', follower_count=123)
        await queue.record_scrape_success('example', follower_count=None)
        state: dict = await queue.get_scrape_state('example')
        self.assertEqual(state['last_follower_count'], 123)
        await queue.record_scrape_success('new', follower_count=None)
        self.assertNotIn(
            'last_follower_count', await queue.get_scrape_state('new'),
        )
