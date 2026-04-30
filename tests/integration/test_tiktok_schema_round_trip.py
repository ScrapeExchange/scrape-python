'''
Integration test: register tiktok-creator/video/hashtag schemas
against scrape.exchange, fetch them back, validate sample
records.

Skipped unless SCRAPE_EXCHANGE_INTEGRATION=1. Requires
EXCHANGE_URL, API_KEY_ID, API_KEY_SECRET, and SCHEMA_OWNER
in the env (SCHEMA_OWNER must match the username on the
API key — e.g. ``boinko``).
'''

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

from httpx import Response
from jsonschema import Draft202012Validator

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.schema_validator import fetch_schema_dict
from scrape_exchange.tiktok.tiktok_creator import TikTokCreator
from scrape_exchange.tiktok.tiktok_hashtag import TikTokHashtag
from scrape_exchange.tiktok.tiktok_video import TikTokVideo


_COLLATERAL: Path = (
    Path(__file__).parent.parent / 'collateral'
)


def _gated() -> bool:
    return os.environ.get(
        'SCRAPE_EXCHANGE_INTEGRATION', '0',
    ) == '1'


@unittest.skipUnless(
    _gated(),
    'set SCRAPE_EXCHANGE_INTEGRATION=1 to run',
)
class TestTikTokSchemaRoundTrip(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.exchange_url: str = os.environ['EXCHANGE_URL']
        self.schema_owner: str = os.environ['SCHEMA_OWNER']
        self.client: ExchangeClient = (
            await ExchangeClient.setup(
                api_key_id=os.environ['API_KEY_ID'],
                api_key_secret=os.environ['API_KEY_SECRET'],
                exchange_url=self.exchange_url,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()

    async def _register(
        self, file_name: str, entity: str,
    ) -> None:
        schema: dict = json.loads(
            (_COLLATERAL / file_name).read_text(),
        )
        url: str = (
            f'{self.exchange_url}'
            f'{ExchangeClient.POST_SCHEMA_API}'
        )
        resp: Response = await self.client.post(
            url=url,
            json={
                'platform': 'tiktok',
                'version': '0.0.1',
                'entity': entity,
                'json_schema': schema,
            },
        )
        # 201 = created, 409 = already exists. Both are fine
        # for this test.
        self.assertIn(resp.status_code, (201, 409))

    async def test_creator_round_trip(self) -> None:
        await self._register(
            'boinko-tiktok-creator-schema.json', 'creator',
        )
        fetched: dict = await fetch_schema_dict(
            self.client,
            exchange_url=self.exchange_url,
            schema_owner=self.schema_owner,
            platform='tiktok',
            entity='creator',
            version='0.0.1',
        )
        payload: dict = json.loads(
            (_COLLATERAL
             / 'tiktok'
             / 'user_info_charlidamelio.json').read_text(),
        )
        c: TikTokCreator = TikTokCreator.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        Draft202012Validator(fetched).validate(c.to_dict())

    async def test_video_round_trip(self) -> None:
        await self._register(
            'boinko-tiktok-video-schema.json', 'video',
        )
        fetched: dict = await fetch_schema_dict(
            self.client,
            exchange_url=self.exchange_url,
            schema_owner=self.schema_owner,
            platform='tiktok',
            entity='video',
            version='0.0.1',
        )
        payload: dict = json.loads(
            (_COLLATERAL
             / 'tiktok'
             / 'video_info_sample.json').read_text(),
        )
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        Draft202012Validator(fetched).validate(v.to_dict())

    async def test_hashtag_round_trip(self) -> None:
        await self._register(
            'boinko-tiktok-hashtag-schema.json', 'hashtag',
        )
        fetched: dict = await fetch_schema_dict(
            self.client,
            exchange_url=self.exchange_url,
            schema_owner=self.schema_owner,
            platform='tiktok',
            entity='hashtag',
            version='0.0.1',
        )
        payload: dict = json.loads(
            (_COLLATERAL
             / 'tiktok'
             / 'hashtag_info_fyp.json').read_text(),
        )
        h: TikTokHashtag = TikTokHashtag.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        Draft202012Validator(fetched).validate(h.to_dict())


if __name__ == '__main__':
    unittest.main()
