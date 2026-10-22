'''
Masking of YouTube channel, video and RSS records, and the lookup of a
channel's counts in the channel scrape queue's Redis meta.
'''

import unittest

import fakeredis

from scrape_exchange.youtube.privacy import (
    HARDCODED_ALLOWED_DOMAINS,
    lookup_channel_counts,
    mask_channel,
    mask_channel_record,
    mask_video,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_external_link import (
    YouTubeExternalLink,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


A: frozenset[str] = HARDCODED_ALLOWED_DOMAINS


class TestMaskChannel(unittest.TestCase):

    def test_description_and_links(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='h')
        channel.description = 'mail jan@mail.example'
        channel.external_urls = {
            YouTubeExternalLink('Site', 'https://myblog.example', 1),
            YouTubeExternalLink('IG', 'https://instagram.com/jan', 2),
            YouTubeExternalLink('Mail', 'mailto:jan@mail.example', 3),
        }
        mask_channel(channel, A)
        self.assertEqual(channel.description, 'mail <email hidden>')
        urls: set[tuple[str, str, int]] = {
            (link.name, link.url, link.priority)
            for link in channel.external_urls
        }
        self.assertEqual(urls, {
            ('Site', '<url withheld>', 1),
            ('IG', 'https://instagram.com/jan', 2),
            ('Mail', '<email hidden>', 3),
        })

    def test_no_description(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='h')
        channel.description = None
        mask_channel(channel, A)
        self.assertIsNone(channel.description)


class TestMaskVideoAndRecord(unittest.TestCase):

    def test_video_description(self) -> None:
        video: YouTubeVideo = YouTubeVideo()
        video.description = 'call +31 6 12345678'
        mask_video(video, A)
        self.assertEqual(video.description, 'call <phone hidden>')

    def test_rss_record(self) -> None:
        record: dict = {
            'channel_id': 'UCa',
            'description': 'site https://myblog.example',
        }
        mask_channel_record(record, A)
        self.assertEqual(record['description'], 'site <url withheld>')

    def test_rss_record_without_description(self) -> None:
        record: dict = {'channel_id': 'UCa'}
        mask_channel_record(record, A)
        self.assertEqual(record, {'channel_id': 'UCa'})


class TestLookup(unittest.IsolatedAsyncioTestCase):

    async def test_absent_and_present(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        key: str = 'youtube:channel:meta:i:UCa'
        self.assertIsNone(await lookup_channel_counts(redis, 'UCa'))
        self.assertIsNone(await lookup_channel_counts(redis, None))
        self.assertIsNone(await lookup_channel_counts(redis, ''))
        await redis.hset(key, mapping={'state': 'scheduled'})
        self.assertEqual(
            await lookup_channel_counts(redis, 'UCa'), (None, None))
        await redis.hset(key, mapping={
            'subscriber_count': '12', 'view_count': '345',
        })
        self.assertEqual(
            await lookup_channel_counts(redis, 'UCa'), (12, 345))

    async def test_unparseable_counts_are_unknown(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        await redis.hset('youtube:channel:meta:i:UCb', mapping={
            'subscriber_count': '', 'view_count': 'x',
        })
        self.assertEqual(
            await lookup_channel_counts(redis, 'UCb'), (None, None))

    async def test_bytes_responses(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis()
        )
        await redis.hset('youtube:channel:meta:i:UCc', mapping={
            'subscriber_count': '7',
        })
        self.assertEqual(
            await lookup_channel_counts(redis, 'UCc'), (7, None))


if __name__ == '__main__':
    unittest.main()
