'''Periodic channel enumeration and durable scrape progress.'''

import asyncio
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import httpx2 as httpx
from prometheus_client import Counter

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeProgress,
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)
from scrape_exchange.youtube import channel_video_refresh as refresh
from scrape_exchange.youtube.channel_video_refresh import VIDEO_IDS
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from tools import yt_channel_scrape as scraper


class TestChannelVideoRefresh(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        self.settings: scraper.ChannelSettings = scraper.ChannelSettings(
            _env_file=None, _cli_parse_args=False,
            channel_data_directory=self.directory.name,
            video_data_directory=self.directory.name,
        )
        self.fm: AssetFileManagement = AssetFileManagement(
            self.directory.name,
        )
        self.channel: YouTubeChannel = YouTubeChannel(
            'example', channel_id='UCexample',
            with_download_client=False,
        )
        self.channel.subscriber_count = 1000
        self.channel.video_count = 1
        self.channel.video_ids = {'new-video'}
        self.existing: bool = False
        self.modes: list[bool] = []
        self.requests: list[str] = []
        self.status: int = 200
        # Video IDs the mock exchange reports for the filter query.
        self.exchange_video_ids: set[str] = set()
        self.http: httpx.AsyncClient = httpx.AsyncClient(
            transport=httpx.MockTransport(self.respond),
        )
        await self.queue.enqueue_scheduled('UCexample', source='test')

    async def asyncTearDown(self) -> None:
        await self.http.aclose()
        await self.redis.aclose()

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request.url.path)
        if request.method == 'POST' and (
            request.url.path == '/api/v1/filter'
        ):
            if self.status != 200:
                return httpx.Response(self.status)
            edges: list[dict[str, dict[str, str]]] = [
                {'node': {'platform_content_id': video_id}}
                for video_id in sorted(self.exchange_video_ids)
            ]
            return httpx.Response(200, json={
                'total_count': len(edges),
                'edges': edges,
                'page_info': {
                    'has_next_page': False, 'end_cursor': None,
                },
            })
        return httpx.Response(self.status)

    async def scrape(self, **kwargs: object) -> None:
        self.modes.append(bool(kwargs['metadata_only']))

    async def run_scrape(self) -> None:
        creator: AsyncMock = AsyncMock()
        creator.get.return_value = 'example'
        creator.redis_client = self.redis

        async def scrape_to_disk(
            *args: object, **kwargs: object,
        ) -> YouTubeChannel:
            await self.scrape(**kwargs)
            return self.channel

        with (
            patch.object(
                scraper, '_channel_exists_on_exchange',
                AsyncMock(return_value=self.existing),
            ),
            patch.object(
                scraper, '_do_scrape_channel_to_disk_typed',
                side_effect=scrape_to_disk,
            ),
        ):
            await scraper._scrape_one_queued(
                'UCexample', queue=self.queue, settings=self.settings,
                fm=self.fm, creator_map_backend=creator,
                http_client=self.http,
            )

    async def test_missing_count_delivers_and_preserves_tier(self) -> None:
        self.channel.subscriber_count = None
        await self.redis.hset('youtube:channel:tiers', 'UCexample', '2')
        await self.queue.set_meta(
            'i:UCexample', force_rescrape_mode='full',
            subscriber_count='12000',
        )
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['new-video'],
        )
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '1')
        self.assertEqual(meta['next_full_scrape'], '11')
        self.assertEqual(meta['subscriber_count'], '12000')
        self.assertEqual(meta['force_rescrape_mode'], 'metadata')
        self.assertEqual(meta['force_source'], 'missing_subscriber_count')
        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', 'UCexample'),
            '2',
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:2', 'i:UCexample',
        )
        self.assertEqual(score, int(meta['last_attempt_at']) + 86400)
        summaries: list[logging.LogRecord] = [
            record for record in logs.records
            if hasattr(record, 'video_ids_added')
        ]
        self.assertEqual(summaries[-1].outcome, 'success')
        self.existing = True
        self.requests.clear()
        await self.run_scrape()
        self.assertEqual(self.modes, [False, True])
        self.assertEqual(self.requests, [])
        self.channel.subscriber_count = 12000
        await self.run_scrape()
        meta = await self.queue.get_meta('i:UCexample')
        self.assertNotIn('force_rescrape_mode', meta)

    async def test_missing_count_delivery_failure_forces_metadata_scrape(
        self,
    ) -> None:
        self.channel.subscriber_count = None
        self.status = 503
        # A filter-query failure is all-or-nothing: the scrape fails
        # and the refresh stays due, no partial accounting.
        with self.assertRaises(RuntimeError):
            await self.run_scrape()
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            [],
        )
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '0')
        self.assertNotIn('force_rescrape_mode', meta)
        # A working filter query on the next scrape succeeds and
        # forces the metadata re-scrape for the missing count.
        self.status = 200
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        summaries: list[logging.LogRecord] = [
            record for record in logs.records
            if hasattr(record, 'video_ids_added')
        ]
        self.assertEqual(summaries[-1].outcome, 'success')
        meta = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '1')
        self.assertEqual(meta['force_rescrape_mode'], 'metadata')
        self.assertEqual(meta['force_source'], 'missing_subscriber_count')

    async def test_missing_count_retry_keeps_periodic_full_cadence(
        self,
    ) -> None:
        self.channel.subscriber_count = None
        self.existing = True
        await self.queue.set_meta(
            'i:UCexample', successful_scrapes='10',
            next_full_scrape='11', force_rescrape_mode='metadata',
            force_source='missing_subscriber_count',
        )
        await self.run_scrape()
        self.assertEqual(self.modes, [False])
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['next_full_scrape'], '21')

    async def test_missing_count_completion_is_atomic(self) -> None:
        progress: ChannelScrapeProgress = await self.queue.get_scrape_progress(
            'UCexample', existing=False,
        )
        results: list[bool | None] = await asyncio.gather(*[
            self.queue.update_tier(
                'UCexample', sub_count=None, now=100,
                progress=progress, full_scrape=True,
            ) for _ in range(2)
        ])
        self.assertCountEqual(results, [True, False])
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '1')
        self.assertEqual(meta['next_full_scrape'], '11')
        self.assertEqual(meta['force_rescrape_mode'], 'metadata')

    async def test_missing_count_cannot_revive_terminal_channel(
        self,
    ) -> None:
        progress: ChannelScrapeProgress = await self.queue.get_scrape_progress(
            'UCexample', existing=False,
        )
        await self.queue.mark('i:UCexample', state=ChannelState.REMOVED)
        result: bool | None = await self.queue.update_tier(
            'UCexample', sub_count=None, now=100,
            progress=progress, full_scrape=True,
        )
        self.assertFalse(result)
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['state'], 'removed')
        self.assertNotIn('force_rescrape_mode', meta)

    async def test_full_scrapes_on_one_eleven_and_twenty_one(self) -> None:
        index: int
        for index in range(21):
            await self.run_scrape()
            self.existing = True
        self.assertEqual(
            [i + 1 for i, metadata in enumerate(self.modes)
             if not metadata],
            [1, 11, 21],
        )
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '21')
        self.assertEqual(meta['next_full_scrape'], '31')
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['new-video'],
        )

    async def test_existing_channel_rollout_waits_ten_successes(self) -> None:
        self.existing = True
        index: int
        for index in range(10):
            await self.run_scrape()
        self.assertEqual(self.modes, [True] * 9 + [False])

    async def test_systemic_lookup_failure_keeps_first_full_scrape_due(
            self,
    ) -> None:
        self.channel.video_ids = {f'v{index}' for index in range(10)}
        self.status = 503
        with self.assertRaises(RuntimeError):
            await self.run_scrape()
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '0')
        self.assertEqual(meta['next_full_scrape'], '1')
        self.existing = True
        self.status = 200
        await self.run_scrape()
        self.assertEqual(self.modes, [False, False])

    async def test_transient_outage_retries_then_succeeds(self) -> None:
        attempts: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            attempts.append(request.url.path)
            status: int = 503 if len(attempts) < 3 else 200
            if status == 200:
                return httpx.Response(200, json={
                    'total_count': 0, 'edges': [],
                    'page_info': {
                        'has_next_page': False, 'end_cursor': None,
                    },
                })
            return httpx.Response(status)

        self.http._transport = httpx.MockTransport(respond)
        await self.run_scrape()
        self.assertEqual(len(attempts), 3)
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '1')
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['new-video'],
        )

    async def test_filters_known_videos_and_reports_actual_additions(
        self,
    ) -> None:
        self.channel.video_ids = {'uploaded', 'local', 'new-video'}
        await self.redis.sadd('youtube:video:uploaded', 'uploaded')
        local: Path = self.fm.base_dir / 'video-min-local.json.br'
        local.touch()
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        self.assertEqual(len(self.requests), 1)
        summary: logging.LogRecord = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_added, 1)
        self.assertEqual(summary.video_ids_existing, 2)
        self.assertEqual(summary.channel_id, 'UCexample')

    async def test_forced_metadata_defers_due_enumeration(self) -> None:
        await self.queue.set_meta(
            'i:UCexample', successful_scrapes='10',
            next_full_scrape='11', force_rescrape_mode='metadata',
        )
        self.existing = True
        await self.run_scrape()
        await self.run_scrape()
        self.assertEqual(self.modes, [True, False])
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['next_full_scrape'], '21')

    async def test_concurrent_completion_counts_once(self) -> None:
        progress: ChannelScrapeProgress = await self.queue.get_scrape_progress(
            'UCexample', existing=False,
        )
        await asyncio.gather(*[
            self.queue.update_tier(
                'UCexample', sub_count=1000, now=100,
                progress=progress, full_scrape=True,
            ) for index in range(2)
        ])
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '1')
        self.assertEqual(meta['next_full_scrape'], '11')

    async def test_terminal_state_wins_over_completion(self) -> None:
        progress: ChannelScrapeProgress = await self.queue.get_scrape_progress(
            'UCexample', existing=False,
        )
        await self.queue.mark('i:UCexample', state=ChannelState.REMOVED)
        await self.queue.update_tier(
            'UCexample', sub_count=1000, now=100,
            progress=progress, full_scrape=True,
        )
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '0')
        self.assertEqual(meta['state'], 'removed')

    async def test_content_failure_propagates_in_strict_mode(self) -> None:
        self.channel.save_dir = self.directory.name
        self.channel.browse_client = object()
        error: Exception
        for error in (
            RuntimeError('incomplete enumeration'),
            ValueError('invalid continuation'),
        ):
            with (
                self.subTest(error=type(error)),
                patch.object(
                    self.channel, 'scrape_channel_content',
                    AsyncMock(side_effect=error),
                ),
                self.assertRaises(RuntimeError),
            ):
                await self.channel.scrape(require_complete_video_ids=True)

    async def test_filter_failure_retries_without_duplicate_additions(
        self,
    ) -> None:
        self.channel.video_ids = {'good', 'retry'}

        def respond(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        await self.http.aclose()
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        # A filter failure raises: no enqueues happen at all.
        with self.assertRaises(RuntimeError):
            await self.run_scrape()
        self.assertEqual(
            await self.redis.zcard('youtube:video:queue'), 0,
        )
        await self.http.aclose()
        self.http = httpx.AsyncClient(
            transport=httpx.MockTransport(self.respond),
        )
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        summary: logging.LogRecord = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_added, 2)
        self.assertEqual(summary.outcome, 'success')
        # Re-scrape: the same IDs are now queue-known, not re-added.
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        summary = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_added, 0)
        self.assertEqual(summary.video_ids_queue_known, 2)
        self.assertEqual(summary.outcome, 'success')
        # The failed run did not count as a successful scrape.
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '2')

    async def test_early_forced_full_preserves_cadence(self) -> None:
        await self.queue.set_meta(
            'i:UCexample', successful_scrapes='3', next_full_scrape='11',
            force_rescrape_mode='full',
        )
        self.existing = True
        await self.run_scrape()
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '4')
        self.assertEqual(meta['next_full_scrape'], '11')
        self.assertEqual(self.modes, [False])
        self.assertEqual(
            await self.redis.zcard('youtube:video:queue'), 1,
        )

    async def test_metrics_count_added_ids_not_duplicates(self) -> None:
        metric: Counter = VIDEO_IDS.labels(outcome='added')
        before: float = metric._value.get()
        await self.run_scrape()
        self.assertEqual(metric._value.get() - before, 1)
        await self.queue.set_meta('i:UCexample', force_rescrape_mode='full')
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        self.assertEqual(metric._value.get() - before, 1)
        summary: logging.LogRecord = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_added, 0)
        self.assertEqual(summary.video_ids_queue_known, 1)

    async def test_redis_tracked_ids_dedupe_without_data_gets(
        self,
    ) -> None:
        # Queue-tracked (queued and terminal) IDs are resolved by the
        # single filter query plus local queue dedupe — no per-video
        # existence GETs against /api/v1/data/content.
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )
        await queue.enqueue('queued-already', source='test')
        await queue.mark(
            'terminal-one', state=VideoState.UNAVAILABLE,
        )
        self.channel.video_ids = {'queued-already', 'terminal-one'}
        with self.assertLogs(level='INFO') as logs:
            await self.run_scrape()
        self.assertEqual(
            [p for p in self.requests if 'data/content' in p],
            [],
        )
        summary: logging.LogRecord = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_queue_known, 2)
        self.assertEqual(summary.video_ids_added, 0)
        self.assertEqual(summary.video_ids_failed, 0)

    async def test_api_existing_and_tombstoned_ids_are_not_added(
        self,
    ) -> None:
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )
        await queue.enqueue('removed', source='test')
        await queue.mark('removed', state=VideoState.REMOVED)
        self.channel.video_ids = {'removed'}
        await self.run_scrape()
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 0)
        self.channel.video_ids = {'api-existing'}
        self.exchange_video_ids = {'api-existing'}
        await self.queue.set_meta('i:UCexample', force_rescrape_mode='full')
        await self.run_scrape()
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 0)

    async def test_scrape_failure_does_not_count_and_logs_zero_added(
        self,
    ) -> None:
        with (
            patch.object(
                self, 'scrape', AsyncMock(side_effect=RuntimeError('fail')),
            ),
            self.assertLogs(level='INFO') as logs,
        ):
            await self.run_scrape()
        meta: dict[str, str] = await self.queue.get_meta('i:UCexample')
        self.assertEqual(meta['successful_scrapes'], '0')
        summary: logging.LogRecord = [r for r in logs.records
                   if hasattr(r, 'video_ids_added')][-1]
        self.assertEqual(summary.video_ids_added, 0)
        self.assertEqual(summary.outcome, 'failure')
