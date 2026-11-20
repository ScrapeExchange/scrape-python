'''
Unit tests for holding new channels out of the RSS queue until the
channel scraper has completed their first full scrape.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType

import fakeredis.aioredis


def _load_yt_rss_scrape() -> ModuleType:
    for key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        if key in sys.modules:
            return sys.modules[key]
    repo_root: Path = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', repo_root / 'tools' / 'yt_rss_scrape.py',
    )
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


class TestHoldBackUnscrapedChannels(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_holds_back_channels_without_first_full_scrape(
        self,
    ) -> None:
        meta: str = 'youtube:channel:meta:i:'
        await self.redis.hset(f'{meta}UCpending', mapping={
            'state': 'scheduled',
        })
        await self.redis.hset(f'{meta}UCzero', mapping={
            'state': 'scheduled', 'successful_scrapes': '0',
        })
        await self.redis.hset(f'{meta}UCdone', mapping={
            'state': 'scheduled', 'successful_scrapes': '1',
        })
        await self.redis.hset(f'{meta}UCknown', mapping={
            'state': 'scheduled', 'successful_scrapes': '0',
        })
        # Scraped before scrape-progress tracking existed: no
        # successful_scrapes, but a recorded attempt.
        await self.redis.hset(f'{meta}UCprogressless', mapping={
            'state': 'scheduled', 'last_attempt_at': '1786000000',
        })
        await self.redis.hset(f'{meta}UCterminal', mapping={
            'state': 'low_subs',
        })
        channel_map: dict[str, str] = {
            'UCpending': 'p', 'UCzero': 'z', 'UCdone': 'd',
            'UClegacy': 'l', 'UCknown': 'k', 'UCprogressless': 'q',
            'UCterminal': 't',
        }
        held: int
        gone: int
        held, gone = await yt_rss_scrape._hold_back_unscraped_channels(
            self.redis, channel_map, known_ids={'UCknown'},
        )
        self.assertEqual(held, 3)
        self.assertEqual(gone, 0)
        self.assertEqual(
            set(channel_map),
            {'UCdone', 'UClegacy', 'UCknown', 'UCprogressless'},
        )

    async def test_drops_channels_the_channel_queue_marks_gone(
        self,
    ) -> None:
        meta: str = 'youtube:channel:meta:i:'
        gone_states: list[str] = sorted(
            yt_rss_scrape.RSS_SKIPPED_CHANNEL_STATES,
        )
        channel_map: dict[str, str] = {}
        state: str
        for state in gone_states:
            cid: str = f'UC{state}'
            # A completed scrape: only the gone state drops it.
            await self.redis.hset(f'{meta}{cid}', mapping={
                'state': state, 'successful_scrapes': '3',
            })
            channel_map[cid] = state
        await self.redis.hset(f'{meta}UCknowngone', mapping={
            'state': 'not_found', 'successful_scrapes': '3',
        })
        await self.redis.hset(f'{meta}UClive', mapping={
            'state': 'scheduled', 'successful_scrapes': '3',
        })
        channel_map['UCknowngone'] = 'k'
        channel_map['UClive'] = 'l'
        held: int
        gone: int
        held, gone = await yt_rss_scrape._hold_back_unscraped_channels(
            self.redis, channel_map, known_ids={'UCknowngone'},
        )
        self.assertEqual(held, 0)
        self.assertEqual(gone, len(gone_states))
        # Channels already in the RSS queue are left alone.
        self.assertEqual(set(channel_map), {'UCknowngone', 'UClive'})

    def test_skipped_states_cover_the_marker_meanings(self) -> None:
        self.assertEqual(
            set(yt_rss_scrape.RSS_SKIPPED_CHANNEL_STATES),
            {
                'not_found', 'invalid_handle', 'unresolved', 'removed',
                'terminated', 'hard_unavailable', 'moved',
            },
        )


if __name__ == '__main__':
    unittest.main()
