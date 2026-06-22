'''
Integration test: scrape a real TikTok creator profile live and
validate the resulting record against the committed creator JSON
Schema.

Exercises the whole foundation path end-to-end against live TikTok:

    bootstrap -> rate-limited session acquire -> User.info ->
    from_api -> to_dict -> JSON-Schema validation.

Skipped unless TIKTOK_LIVE_INTEGRATION=1. Reads PROXIES from env
(comma-separated, like the production scrapers) and scrapes the
first proxy only. The creator defaults to 'charlidamelio' and can
be overridden with TIKTOK_TEST_CREATOR.

No tool binary exists yet (deferred to tt_creator_scrape.py), so the
test drives the foundation directly: it issues the User.info call on
the pool's TikTokApi instance from inside the rate-limited session
context.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import os
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator

from scrape_exchange.tiktok.tiktok_creator import TikTokCreator
from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_session_pool import (
    TikTokSessionPool,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType


_SCHEMA_PATH: Path = (
    Path(__file__).parent.parent
    / 'collateral'
    / 'drand-tiktok-creator-schema.json'
)

_DEFAULT_CREATOR: str = 'charlidamelio'


def _gated() -> bool:
    return os.environ.get(
        'TIKTOK_LIVE_INTEGRATION', '0',
    ) == '1'


@unittest.skipUnless(
    _gated(),
    'set TIKTOK_LIVE_INTEGRATION=1 + PROXIES to run',
)
class TestCreatorLiveScrape(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        proxies_csv: str | None = os.environ.get('PROXIES')
        self.assertIsNotNone(
            proxies_csv,
            'PROXIES env var required for live creator scrape',
        )
        self.proxy: str = proxies_csv.split(',')[0].strip()
        self.creator: str = os.environ.get(
            'TIKTOK_TEST_CREATOR', _DEFAULT_CREATOR,
        )

        TikTokRateLimiter.reset()
        self.rate_limiter: TikTokRateLimiter = TikTokRateLimiter()
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.pool: TikTokSessionPool = TikTokSessionPool(
            proxies=[self.proxy],
            state_dir=self._tmp.name,
            ms_token_ttl_seconds=14400,
            rate_limiter=self.rate_limiter,
            scraper_label='integration',
            api_call_type=TikTokCallType.CREATOR_API,
        )
        await self.pool.bootstrap()
        self.assertIn(
            self.proxy,
            self.pool.ready_proxies(),
            'bootstrap failed; cannot run live creator scrape',
        )

    async def asyncTearDown(self) -> None:
        await self.pool.shutdown()
        self._tmp.cleanup()

    async def test_live_creator_scrape_validates(self) -> None:
        async with self.pool.session_for(self.proxy) as api:
            # session_for yields this proxy's CamoufoxTikTokApi
            # (main-world signing + fetch); scrape through it inside
            # the rate-limited session context.
            resp: dict = await api.user(
                username=self.creator,
            ).info()

        creator: TikTokCreator = TikTokCreator.from_user_info(resp)

        # Sanity-check the live data maps as expected.
        self.assertEqual(creator.username, self.creator)
        self.assertTrue(creator.sec_uid)
        self.assertTrue(creator.user_id)
        self.assertEqual(
            creator.url,
            f'https://www.tiktok.com/@{self.creator}',
        )
        self.assertGreaterEqual(creator.follower_count, 0)

        # The scraped record must validate against the committed
        # creator schema.
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(creator.to_dict())


if __name__ == '__main__':
    unittest.main()
