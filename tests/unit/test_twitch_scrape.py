'''Daemon processing with real queue state and compressed files.'''

import asyncio
import tempfile
import unittest
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis

from scrape_exchange.creator_queue import RedisCreatorQueue
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileRateLimitError,
    ProfileUnavailableError,
)
from tools.tw_creator_scrape import (
    _maintenance,
    build_queue,
    process_creator,
)


class TestTwitchScrape(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[],
            redis_dsn='redis://localhost',
            creator_data_directory=self.directory.name,
        )
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        with patch('scrape_exchange.creator_queue.redis_from_url',
                   return_value=self.redis):
            self.queue: RedisCreatorQueue = build_queue(self.settings, 'test')
        self.fm: AssetFileManagement = AssetFileManagement(
            self.directory.name,
            prefix_rankings={'creator': ['twitch-creator-']},
        )
        self.creator: TwitchCreator = TwitchCreator(
            username='example', user_id='123', url='https://localhost/example',
            scraped_timestamp=datetime.now(UTC), follower_count=0,
        )
        self.pool: MagicMock = MagicMock()

        @asynccontextmanager
        async def session(proxy: str) -> AsyncIterator[MagicMock]:
            yield MagicMock()

        self.pool.session_for = session
        self.pool.limiter = MagicMock()
        await self.queue.add_member('example', 'example', 0)
        await self.queue.reschedule('example')
        await self.queue.claim_batch(1, 'test')

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()
        self.directory.cleanup()

    async def process(self) -> str | None:
        return await process_creator(
            'example', 'direct', self.pool, self.queue, self.fm,
            self.settings, 'test', 'test',
        )

    async def test_success_writes_record_and_reschedules(self) -> None:
        with patch('tools.tw_creator_scrape.fetch_profile',
                   AsyncMock(return_value=self.creator)):
            self.assertIsNone(await self.process())
        record: dict = await self.fm.read_file('twitch-creator-example.json.br')
        self.assertEqual(record['user_id'], '123')
        self.assertEqual(record['follower_count'], 0)
        state: dict = await self.queue.show_member('example')
        self.assertEqual(state['state'], 'queued')
        self.assertEqual(state['scrape_status'], 'scraped')

    async def test_missing_count_keeps_refresh_tier(self) -> None:
        previous_tier: int = await self.queue.get_tier('example')
        self.creator.follower_count = None
        with patch('tools.tw_creator_scrape.fetch_profile',
                   AsyncMock(return_value=self.creator)):
            self.assertIsNone(await self.process())
        self.assertEqual(await self.queue.get_tier('example'), previous_tier)
        state: dict = await self.queue.get_scrape_state('example')
        self.assertFalse(state['follower_count_known'])
        self.assertNotIn('last_follower_count', state)

    async def test_maintenance_recovers_expired_claim(self) -> None:
        await self.redis.delete(f'{self.queue._claim_prefix}example')
        with patch('tools.tw_creator_scrape.asyncio.sleep',
                   AsyncMock(side_effect=asyncio.CancelledError)), (
            self.assertRaises(asyncio.CancelledError)
        ):
            await _maintenance(self.queue, self.settings, 'test')
        self.assertEqual(
            (await self.queue.show_member('example'))['state'], 'queued',
        )
        self.assertLessEqual(
            await self.redis.ttl(self.queue._orphan_recovery_lock_key()),
            self.settings.creator_orphan_recovery_interval_seconds,
        )

    async def test_identity_conflict_preserves_file_and_parks_creator(
        self,
    ) -> None:
        await self.fm.write_file(
            'twitch-creator-example.json.br', self.creator.to_dict(),
        )
        self.creator.user_id = '456'
        with patch('tools.tw_creator_scrape.fetch_profile',
                   AsyncMock(return_value=self.creator)):
            self.assertEqual(await self.process(), 'identity_conflict')
        record: dict = await self.fm.read_file('twitch-creator-example.json.br')
        self.assertEqual(record['user_id'], '123')
        self.assertEqual(
            (await self.queue.show_member('example'))['state'], 'removed',
        )

    async def test_write_failure_does_not_mark_success(self) -> None:
        with patch('tools.tw_creator_scrape.fetch_profile',
                   AsyncMock(return_value=self.creator)), patch.object(
            self.fm, 'write_file', AsyncMock(side_effect=OSError('disk full')),
        ):
            self.assertEqual(await self.process(), 'storage')
        state: dict = await self.queue.get_scrape_state('example')
        self.assertNotIn('last_success_at', state)
        self.assertEqual(state['scrape_status'], 'storage')

    async def test_removed_claim_is_not_written_or_rescheduled(self) -> None:
        await self.queue.exclude('example')
        with patch('tools.tw_creator_scrape.fetch_profile',
                   AsyncMock(return_value=self.creator)):
            self.assertEqual(await self.process(), 'claim_lost')
        self.assertFalse(
            (Path(self.directory.name) / 'twitch-creator-example.json.br')
            .exists(),
        )
        self.assertEqual(
            (await self.queue.show_member('example'))['state'], 'removed',
        )

    async def test_only_definitive_unavailable_is_terminal(self) -> None:
        for error, reason, expected_state in (
            (ProfileRateLimitError(), 'rate_limit', 'queued'),
            (ProfileUnavailableError(), 'unavailable', 'removed'),
        ):
            with self.subTest(reason=reason):
                await self.queue.reschedule('example')
                await self.queue.claim_batch(1, 'test')
                with patch('tools.tw_creator_scrape.fetch_profile',
                           AsyncMock(side_effect=error)):
                    self.assertEqual(await self.process(), reason)
                self.assertEqual(
                    (await self.queue.show_member('example'))['state'],
                    expected_state,
                )
