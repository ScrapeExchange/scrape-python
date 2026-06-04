'''End-to-end: channel scrape + upload + RSS all key on channel_id.'''

import unittest
from unittest import mock

import fakeredis.aioredis

from scrape_exchange.bulk_upload import apply_bulk_results
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from tools.yt_channel_scrape import (
    get_channel_filename,
    _channel_id_from_filename,
)


class TestChannelIdNamingE2E(unittest.IsolatedAsyncioTestCase):

    async def test_upload_round_trip_seeds_exchange_set(self) -> None:
        cid: str = 'UCabcdefghijklmnopqrstuv'
        filename: str = get_channel_filename(cid)
        self.assertEqual(filename, f'channel-{cid}.json.br')
        self.assertEqual(_channel_id_from_filename(filename), cid)

        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        ex = RedisExchangeChannelsSet(redis)
        fm = mock.AsyncMock()
        await apply_bulk_results(
            batch_records=[(cid, filename)],
            results=[{
                'status': 'success',
                'platform_content_id': cid,
            }],
            fm=fm, batch_id='b', job_id='j',
            exchange_set=ex,
            id_from_filename=_channel_id_from_filename,
        )
        membership = await ex.contains_many([cid])
        self.assertTrue(membership[cid])
        await redis.flushall()
        await redis.aclose()


if __name__ == '__main__':
    unittest.main()
