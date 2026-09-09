'''Redis creator queue scheduling, polling and recovery for OnlyFans.'''

import asyncio
import tempfile
import time
import unittest
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
from pydantic import ValidationError

from scrape_exchange.creator_queue import RedisCreatorQueue
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.onlyfans.onlyfans_browser import ProfileBlockedError
from scrape_exchange.onlyfans.onlyfans_creator import extract_profile
from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from scrape_exchange.queue_admin import OperatorQueue, get_adapter
from tests.unit.test_onlyfans_creator import public_profile
from tools.of_creator_scrape import (
    build_queue,
    process_queued_creator,
    queue_worker,
)


class TestOnlyFansQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.directory: tempfile.TemporaryDirectory[str] = (
            tempfile.TemporaryDirectory()
        )
        self.settings: OnlyFansScraperSettings = OnlyFansScraperSettings(
            _env_file=None, _cli_parse_args=[],
            redis_dsn='redis://localhost', proxies_env=None, proxy_files=None,
            creator_data_directory=self.directory.name,
            creator_priority_queues='1:100,24:0',
            creator_retry_interval_seconds=300,
        )
        with patch('scrape_exchange.creator_queue.redis_from_url',
                   return_value=self.redis):
            self.queue: RedisCreatorQueue = build_queue(self.settings, 'test')
        self.fm: AssetFileManagement = AssetFileManagement(self.directory.name)
        self.context: MagicMock = MagicMock()
        self.limiter: MagicMock = MagicMock()
        self.owner: str = 'test:task-1'

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()
        self.directory.cleanup()

    async def claim(self, weight: int = 0) -> None:
        await self.queue.add_member('example', 'example', weight)
        claimed: list[tuple[str, str, float]] = await self.queue.claim_batch(
            1, self.owner,
        )
        self.assertEqual(claimed[0][0], 'example')

    async def process(self, **values: Any) -> str:
        with patch('tools.of_creator_scrape.fetch_profile', AsyncMock(
            return_value=extract_profile(public_profile(**values), 'example'),
        )):
            return await process_queued_creator(
                'example', self.context, self.queue, self.fm, self.settings,
                self.limiter, 'http://localhost:8888', self.owner,
            )

    async def test_success_reschedules_by_likes_not_fans(self) -> None:
        await self.claim()
        before: float = time.time()
        self.assertEqual(await self.process(
            favoritedCount=100, subscribersCount=0,
        ), 'scraped')
        record: dict[str, Any] = await self.queue.show_member('example')
        self.assertEqual(record['tier'], 1)
        self.assertGreaterEqual(record['score'], before + 3600)
        self.assertLess(record['score'], time.time() + 3601)
        self.assertEqual(await self.queue.claim_batch(1, 'other'), [])
        saved: dict[str, Any] = await self.fm.read_file(
            'onlyfans-creator-example.json.br',
        )
        self.assertEqual(saved['like_count'], 100)
        state: dict[str, Any] = await self.queue.get_scrape_state('example')
        self.assertEqual(state['last_like_count'], 100)
        self.assertNotIn('last_follower_count', state)

    async def test_zero_likes_moves_to_catch_all_despite_many_fans(
        self,
    ) -> None:
        await self.claim(100)
        before: float = time.time()
        await self.process(favoritedCount=0, subscribersCount=1000000)
        record: dict[str, Any] = await self.queue.show_member('example')
        self.assertEqual(record['tier'], 2)
        self.assertGreaterEqual(record['score'], before + 86400)

    async def test_unknown_likes_preserve_tier_and_previous_observation(
        self,
    ) -> None:
        await self.claim(100)
        await self.queue.record_scrape_success(
            'example', follower_count=None, evidence={'last_like_count': 123},
        )
        await self.process(favoritedCount=None)
        self.assertEqual(await self.queue.get_tier('example'), 1)
        self.assertEqual((await self.queue.get_scrape_state('example'))[
            'last_like_count'
        ], 123)

    async def test_failed_scrape_is_retained_for_retry_without_output(
        self,
    ) -> None:
        await self.claim()
        before: float = time.time()
        with patch('tools.of_creator_scrape.fetch_profile', AsyncMock(
            side_effect=ProfileBlockedError('blocked'),
        )):
            outcome: str = await process_queued_creator(
                'example', self.context, self.queue, self.fm, self.settings,
                self.limiter, None, self.owner,
            )
        self.assertEqual(outcome, 'rate_limit')
        record: dict[str, Any] = await self.queue.show_member('example')
        self.assertGreaterEqual(record['score'], before + 300)
        self.assertEqual(list(Path(self.directory.name).glob('*.json.br')), [])

    async def test_lost_claim_does_not_write_or_reschedule(self) -> None:
        await self.claim()
        await self.redis.set('scrape:onlyfans:claim:example', 'another-owner')
        self.assertEqual(await self.process(), 'claim_lost')
        self.assertEqual(await self.redis.get(
            'scrape:onlyfans:claim:example',
        ), 'another-owner')
        self.assertEqual(await self.queue.queue_size(), 0)
        self.assertEqual(list(Path(self.directory.name).glob('*.json.br')), [])

    async def test_claim_lost_during_scrape_does_not_overwrite_new_owner(
        self,
    ) -> None:
        await self.claim()

        async def fetch(*args: Any) -> Any:
            await self.redis.set('scrape:onlyfans:claim:example', 'replacement')
            return extract_profile(public_profile(), 'example')

        with patch('tools.of_creator_scrape.fetch_profile', fetch):
            outcome: str = await process_queued_creator(
                'example', self.context, self.queue, self.fm, self.settings,
                self.limiter, None, self.owner,
            )
        self.assertEqual(outcome, 'claim_lost')
        self.assertEqual(await self.queue.queue_size(), 0)
        self.assertEqual(list(Path(self.directory.name).glob('*.json.br')), [])

    async def test_worker_saves_due_creator_then_waits_for_future_work(
        self,
    ) -> None:
        await self.queue.add_member('example', 'example', 0)
        proxies: list[str | None] = []

        @asynccontextmanager
        async def browser(
            proxy: str | None, settings: OnlyFansScraperSettings,
        ) -> AsyncIterator[MagicMock]:
            proxies.append(proxy)
            yield self.context

        delay: AsyncMock = AsyncMock(side_effect=asyncio.CancelledError)
        fetch: AsyncMock = AsyncMock(return_value=extract_profile(
            public_profile(favoritedCount=100), 'example',
        ))
        with (
            patch('tools.of_creator_scrape.anonymous_browser', browser),
            patch('tools.of_creator_scrape.fetch_profile', fetch),
            patch('tools.of_creator_scrape.sleep', delay),
            self.assertRaises(asyncio.CancelledError),
        ):
            await queue_worker(
                ['http://localhost:8888'], self.queue, self.fm,
                self.settings, self.limiter, self.owner,
            )
        self.assertEqual(proxies, ['http://localhost:8888'])
        self.assertEqual(fetch.await_args.args[3], 'http://localhost:8888')
        self.assertEqual(delay.await_args.args, (60,))
        self.assertEqual(await self.queue.get_tier('example'), 1)
        self.assertEqual((await self.fm.read_file(
            'onlyfans-creator-example.json.br',
        ))['like_count'], 100)

    async def test_browser_startup_failure_retains_creator_for_retry(
        self,
    ) -> None:
        await self.queue.add_member('example', 'example', 0)
        before: float = time.time()
        with (
            patch('tools.of_creator_scrape.anonymous_browser',
                  side_effect=RuntimeError('browser failed')),
            patch('tools.of_creator_scrape.sleep',
                  AsyncMock(side_effect=asyncio.CancelledError)),
            self.assertRaises(asyncio.CancelledError),
        ):
            await queue_worker(
                [None], self.queue, self.fm, self.settings, self.limiter,
                self.owner,
            )
        record: dict[str, Any] = await self.queue.show_member('example')
        self.assertEqual(record['state'], 'queued')
        self.assertGreaterEqual(record['score'], before + 300)

    async def test_expired_claim_can_be_recovered(self) -> None:
        await self.claim()
        await self.redis.delete('scrape:onlyfans:claim:example')
        await self.queue.scan_and_recover_orphans_with_fleet_lock()
        claimed: list[tuple[str, str, float]] = await self.queue.claim_batch(
            1, 'replacement',
        )
        self.assertEqual(claimed[0][0], 'example')

    async def test_cancelled_scrape_leaves_claim_for_recovery(self) -> None:
        await self.claim()
        started: asyncio.Event = asyncio.Event()

        async def fetch(*args: Any, **kwargs: Any) -> None:
            started.set()
            await asyncio.Event().wait()

        with patch('tools.of_creator_scrape.fetch_profile', fetch):
            task: asyncio.Task[str] = asyncio.create_task(
                process_queued_creator(
                    'example', self.context, self.queue, self.fm,
                    self.settings, self.limiter, None, self.owner,
                ),
            )
            await asyncio.wait_for(started.wait(), timeout=5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(
            await self.redis.get('scrape:onlyfans:claim:example'), self.owner,
        )
        self.assertEqual(
            list(Path(self.directory.name).glob('*.json.br')), [],
        )
        await self.redis.delete('scrape:onlyfans:claim:example')
        await self.queue.scan_and_recover_orphans_with_fleet_lock()
        claimed: list[tuple[str, str, float]] = await self.queue.claim_batch(
            1, 'replacement',
        )
        self.assertEqual(claimed[0][0], 'example')

    async def test_no_due_work_polls_every_sixty_seconds_without_browser(
        self,
    ) -> None:
        await self.queue.add_member('example', 'example', 0)
        await self.queue.reschedule_in('example', 3600)
        delays: list[float] = []

        async def sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 2:
                raise asyncio.CancelledError

        with (
            patch('tools.of_creator_scrape.sleep', sleep),
            patch('tools.of_creator_scrape.anonymous_browser') as browser,
            self.assertRaises(asyncio.CancelledError),
        ):
            await queue_worker(
                [None], self.queue, self.fm, self.settings, self.limiter,
                self.owner,
            )
        self.assertEqual(delays, [60, 60])
        browser.assert_not_called()

    async def test_operator_and_scraper_use_same_namespace_and_like_tiers(
        self,
    ) -> None:
        settings: SimpleNamespace = SimpleNamespace(
            redis_dsn='redis://localhost', worker_id='operator',
            onlyfans_creator_priority_queues='1:100,24:0',
        )
        with patch('scrape_exchange.creator_queue.redis_from_url',
                   return_value=self.redis):
            adapter: OperatorQueue = get_adapter(
                'onlyfans', 'creator', settings,
            )
        self.assertEqual(await adapter.add([('@Example', 100)]), 1)
        self.assertEqual((await adapter.show('@Example'))['tier'], 1)
        claimed: list[tuple[str, str, float]] = await self.queue.claim_batch(
            1, self.owner,
        )
        self.assertEqual(claimed[0][0], 'example')
        self.assertTrue(all(
            key.startswith('scrape:onlyfans:')
            for key in await self.redis.keys('*')
        ))


class TestOnlyFansQueueSettings(unittest.TestCase):
    def test_rejects_invalid_tiers_and_short_claim_lifetime(self) -> None:
        changes: dict[str, Any]
        for changes in (
            {'creator_priority_queues': '1:100'},
            {'creator_priority_queues': '0:100,24:0'},
            {'creator_priority_queues': 'nan:100,24:0'},
            {'creator_priority_queues': '1:0,24:100'},
            {'creator_claim_ttl_seconds': 90},
        ):
            with self.subTest(changes=changes), self.assertRaises(
                ValidationError,
            ):
                OnlyFansScraperSettings(
                    _env_file=None, _cli_parse_args=[], **changes,
                )
