import unittest

from pathlib import Path
from tempfile import TemporaryDirectory

from scrape_exchange.brotli import brotli_write_async
from scrape_exchange.youtube.derived_metadata import (
    CHANNEL_CATEGORY_COUNTS_PREFIX,
    CHANNEL_COUNTRY_HASH,
    enrich_channel_category,
    enrich_video_channel_country,
    increment_channel_category_count,
    infer_channel_category_from_redis,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}

    async def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: str | None = None,
        mapping: dict[str, str] | None = None,
    ) -> None:
        bucket: dict[str, str] = self.hashes.setdefault(key, {})
        if mapping is not None:
            bucket.update({str(k): str(v) for k, v in mapping.items()})
        elif field is not None and value is not None:
            bucket[str(field)] = str(value)

    async def hincrby(
        self, key: str, field: str, amount: int,
    ) -> int:
        bucket: dict[str, str] = self.hashes.setdefault(key, {})
        value: int = int(bucket.get(field, '0')) + amount
        bucket[field] = str(value)
        return value

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)


class TestDerivedMetadata(unittest.IsolatedAsyncioTestCase):
    async def test_increment_video_category_and_select_channel(
        self,
    ) -> None:
        redis: FakeRedis = FakeRedis()
        for _ in range(21):
            await increment_channel_category_count(
                redis, 'UC123', ' Music ',
            )

        category: str | None = await infer_channel_category_from_redis(
            redis, 'UC123',
        )

        self.assertEqual(category, 'Music')

    async def test_country_enrichment_uses_redis_first(self) -> None:
        redis: FakeRedis = FakeRedis()
        await redis.hset(CHANNEL_COUNTRY_HASH, 'UC123', 'US')
        video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        video.channel_id = 'UC123'

        await enrich_video_channel_country(
            video,
            redis=redis,
            channel_data_directory=None,
            client=None,
            username=None,
        )

        self.assertEqual(video.channel_country, 'US')

    async def test_country_enrichment_uses_local_channel_file(self) -> None:
        redis: FakeRedis = FakeRedis()
        video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        video.channel_id = 'UC123'
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.country = 'GB'
        with TemporaryDirectory() as channel_dir:
            await brotli_write_async(
                Path(channel_dir) / 'channel-UC123.json.br',
                channel.to_dict(with_video_ids=True),
            )

            await enrich_video_channel_country(
                video,
                redis=redis,
                channel_data_directory=channel_dir,
                client=None,
                username=None,
            )

        self.assertEqual(video.channel_country, 'GB')
        self.assertEqual(
            redis.hashes[CHANNEL_COUNTRY_HASH]['UC123'], 'GB',
        )

    async def test_channel_category_uses_local_video_files(self) -> None:
        redis: FakeRedis = FakeRedis()
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.video_ids = {f'{index:011d}' for index in range(21)}
        with TemporaryDirectory() as video_dir:
            for video_id in channel.video_ids:
                video: YouTubeVideo = YouTubeVideo(video_id=video_id)
                video.channel_id = 'UC123'
                video.category = 'Education'
                await brotli_write_async(
                    Path(video_dir) / f'video-dlp-{video_id}.json.br',
                    video.to_dict(),
                )

            await enrich_channel_category(
                channel,
                redis=redis,
                video_data_directory=video_dir,
            )

        self.assertEqual(channel.category, 'Education')
        key: str = f'{CHANNEL_CATEGORY_COUNTS_PREFIX}UC123'
        self.assertEqual(redis.hashes[key]['Education'], '21')

    async def test_channel_category_does_not_double_count_local_counts(
        self,
    ) -> None:
        redis: FakeRedis = FakeRedis()
        key: str = f'{CHANNEL_CATEGORY_COUNTS_PREFIX}UC123'
        await redis.hset(key, mapping={'Music': '20'})
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.video_ids = {'aaaaaaaaaaa'}
        with TemporaryDirectory() as video_dir:
            video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
            video.channel_id = 'UC123'
            video.category = 'Music'
            await brotli_write_async(
                Path(video_dir) / 'video-dlp-aaaaaaaaaaa.json.br',
                video.to_dict(),
            )

            await enrich_channel_category(
                channel,
                redis=redis,
                video_data_directory=video_dir,
            )

        self.assertIsNone(channel.category)
        self.assertEqual(redis.hashes[key]['Music'], '20')

    async def test_channel_category_local_counts_are_idempotent(
        self,
    ) -> None:
        redis: FakeRedis = FakeRedis()
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.video_ids = {f'{index:011d}' for index in range(8)}
        with TemporaryDirectory() as video_dir:
            for video_id in channel.video_ids:
                video: YouTubeVideo = YouTubeVideo(video_id=video_id)
                video.channel_id = 'UC123'
                video.category = 'Education'
                await brotli_write_async(
                    Path(video_dir) / f'video-dlp-{video_id}.json.br',
                    video.to_dict(),
                )

            for _ in range(4):
                channel.category = None
                await enrich_channel_category(
                    channel,
                    redis=redis,
                    video_data_directory=video_dir,
                )

        self.assertIsNone(channel.category)
        key: str = f'{CHANNEL_CATEGORY_COUNTS_PREFIX}UC123'
        self.assertEqual(redis.hashes[key]['Education'], '8')


if __name__ == '__main__':
    unittest.main()
