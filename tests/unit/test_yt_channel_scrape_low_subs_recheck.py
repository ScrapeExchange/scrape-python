'''
The ``--recheck-low-subs`` mode of ``tools/yt_channel_scrape.py``: a
one-shot pass that runs instead of the supervisor and its workers.
'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import RedisChannelScrapeQueue
from scrape_exchange.youtube.low_subs_recheck import RecheckSummary
from tools import yt_channel_scrape
from tools.yt_channel_scrape import ChannelSettings


def _settings(**overrides: object) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.redis_dsn = 'redis://localhost:6379/0'
    settings.rate_limiter_state_dir = None
    settings.proxies = []
    settings.channel_concurrency = 2
    settings.channel_priority_queues = '7:1000,30:100,365:0'
    settings.channel_priority_weights = ''
    settings.recheck_low_subs = True
    settings.recheck_apply = False
    settings.recheck_after_days = 30.0
    settings.recheck_limit = None
    for name, value in overrides.items():
        setattr(settings, name, value)
    return settings


class TestQueueBuilder(unittest.TestCase):

    def test_queue_builder_uses_settings(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        queue: RedisChannelScrapeQueue = (
            yt_channel_scrape._build_channel_queue(
                ChannelSettings(
                    _cli_parse_args=[],
                    channel_priority_queues='7:1000,30:100,365:0',
                ),
                redis,
            )
        )
        self.assertEqual(len(queue._tiers), 3)


class TestRunRecheck(unittest.IsolatedAsyncioTestCase):

    async def test_recheck_requires_redis(self) -> None:
        recheck: AsyncMock = AsyncMock()
        with patch.object(
            yt_channel_scrape, 'recheck_low_subs_channels', recheck,
        ):
            code: int = await yt_channel_scrape._run_low_subs_recheck(
                _settings(redis_dsn=None),
            )
        self.assertEqual(code, 2)
        recheck.assert_not_awaited()

    async def test_recheck_passes_settings(self) -> None:
        calls: list[str] = []
        recheck: AsyncMock = AsyncMock(
            return_value=RecheckSummary(),
            side_effect=lambda *a, **k: calls.append('recheck')
            or RecheckSummary(),
        )
        limiter: MagicMock = MagicMock(
            side_effect=lambda s: calls.append('limiter'),
        )
        with patch.object(
            yt_channel_scrape, 'recheck_low_subs_channels', recheck,
        ), patch.object(
            yt_channel_scrape, '_build_channel_rate_limiter', limiter,
        ), patch.object(
            yt_channel_scrape, 'redis_from_url',
            return_value=fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            ),
        ) as from_url:
            code: int = await yt_channel_scrape._run_low_subs_recheck(
                _settings(
                    recheck_apply=True, recheck_after_days=7,
                    recheck_limit=5,
                ),
            )
        self.assertEqual(code, 0)
        self.assertEqual(calls, ['limiter', 'recheck'])
        kwargs: dict = recheck.await_args.kwargs
        self.assertTrue(kwargs['apply'])
        self.assertEqual(kwargs['recheck_after_days'], 7)
        self.assertEqual(kwargs['limit'], 5)
        self.assertEqual(kwargs['concurrency'], 2)
        self.assertIn('component', from_url.call_args.kwargs)


class TestMain(unittest.TestCase):

    def test_main_branches_before_runner(self) -> None:
        run: AsyncMock = AsyncMock(return_value=0)
        with patch.object(
            yt_channel_scrape, 'ChannelSettings',
            return_value=_settings(),
        ), patch.object(
            yt_channel_scrape, '_validate_settings',
        ), patch.object(
            yt_channel_scrape, 'ScraperRunner',
        ) as runner, patch.object(
            yt_channel_scrape, '_run_low_subs_recheck', run,
        ), self.assertRaises(SystemExit) as raised:
            yt_channel_scrape.main()
        self.assertEqual(raised.exception.code, 0)
        runner.assert_not_called()
        run.assert_awaited_once()

    def test_cli_flags(self) -> None:
        settings: ChannelSettings = ChannelSettings(_cli_parse_args=[
            '--recheck-low-subs', 'true',
            '--recheck-apply', 'true',
            '--recheck-after-days', '7',
            '--recheck-limit', '3',
        ])
        self.assertTrue(settings.recheck_low_subs)
        self.assertTrue(settings.recheck_apply)
        self.assertEqual(settings.recheck_after_days, 7.0)
        self.assertEqual(settings.recheck_limit, 3)

    def test_recheck_defaults_off(self) -> None:
        settings: ChannelSettings = ChannelSettings(_cli_parse_args=[])
        self.assertFalse(settings.recheck_low_subs)
        self.assertFalse(settings.recheck_apply)
        self.assertEqual(settings.recheck_after_days, 30.0)
        self.assertIsNone(settings.recheck_limit)


if __name__ == '__main__':
    unittest.main()
