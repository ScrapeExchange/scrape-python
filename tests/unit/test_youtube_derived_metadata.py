import time
import unittest

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import httpx2

from scrape_exchange.brotli import brotli_write_async
from scrape_exchange.youtube.derived_metadata import (
    CATEGORY_COUNTS_TTL_SECONDS,
    CHANNEL_CATEGORY_COUNTS_PREFIX,
    CHANNEL_COUNTRY_HASH,
    CHANNEL_NO_COUNTRY_HASH,
    NO_COUNTRY_TTL_SECONDS,
    enrich_channel_category,
    enrich_video_channel_country,
    increment_channel_category_count,
    infer_channel_category_from_redis,
    set_channel_country,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.ttls: dict[str, int] = {}
        self.field_ttls: dict[tuple[str, str], int] = {}

    async def expire(self, key: str, seconds: int) -> bool:
        if key not in self.hashes and key not in self.sets:
            return False
        self.ttls[key] = seconds
        return True

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

    async def hdel(self, key: str, *fields: str) -> int:
        bucket: dict[str, str] = self.hashes.get(key, {})
        removed: int = 0
        for field in fields:
            if bucket.pop(field, None) is not None:
                removed += 1
        return removed

    async def hexpire(
        self, key: str, seconds: int, *fields: str,
    ) -> list[int]:
        for field in fields:
            self.field_ttls[(key, field)] = seconds
        return [1 for _ in fields]


DERIVED: str = 'scrape_exchange.youtube.derived_metadata'
API_URL: str = 'https://api.scrape.exchange/api/v1/data/param/x'


def _http_error(status: int) -> httpx2.HTTPStatusError:
    request: httpx2.Request = httpx2.Request('GET', API_URL)
    return httpx2.HTTPStatusError(
        f'HTTP {status}',
        request=request,
        response=httpx2.Response(status, request=request),
    )


def _video(channel_id: str = 'UC123') -> YouTubeVideo:
    video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
    video.channel_id = channel_id
    return video


async def _enrich_via_api(
    redis: FakeRedis, video: YouTubeVideo,
) -> None:
    await enrich_video_channel_country(
        video,
        redis=redis,
        channel_data_directory=None,
        client=object(),
        username='scraper',
    )


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
        self.assertEqual(
            redis.ttls[key], CATEGORY_COUNTS_TTL_SECONDS,
        )

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


class TestChannelNoCountryCache(unittest.IsolatedAsyncioTestCase):
    '''Negative cache: channels the API says have no country skip the
    API for NO_COUNTRY_TTL_SECONDS; errors are never cached.'''

    async def test_api_404_marks_channel_and_skips_next_lookup(
        self,
    ) -> None:
        redis: FakeRedis = FakeRedis()
        lookup: AsyncMock = AsyncMock(side_effect=_http_error(404))
        with patch(f'{DERIVED}.get_data_by_param', lookup):
            await _enrich_via_api(redis, _video())
            video: YouTubeVideo = _video()
            await _enrich_via_api(redis, video)

        self.assertEqual(lookup.await_count, 1)
        self.assertIsNone(video.channel_country)
        expires_at: float = float(
            redis.hashes[CHANNEL_NO_COUNTRY_HASH]['UC123'],
        )
        self.assertAlmostEqual(
            expires_at, time.time() + NO_COUNTRY_TTL_SECONDS, delta=5,
        )
        self.assertEqual(
            redis.field_ttls[(CHANNEL_NO_COUNTRY_HASH, 'UC123')],
            NO_COUNTRY_TTL_SECONDS,
        )

    async def test_channel_without_country_is_marked(self) -> None:
        redis: FakeRedis = FakeRedis()
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        with patch(f'{DERIVED}.get_data_by_param', AsyncMock()), \
                patch(
                    f'{DERIVED}.fetch_youtube_channel',
                    AsyncMock(return_value=channel),
                ):
            await _enrich_via_api(redis, _video())

        self.assertIn('UC123', redis.hashes[CHANNEL_NO_COUNTRY_HASH])

    async def test_transient_errors_are_not_cached(self) -> None:
        errors: list[Exception] = [
            _http_error(500),
            httpx2.ConnectTimeout('timed out'),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                redis: FakeRedis = FakeRedis()
                lookup: AsyncMock = AsyncMock(side_effect=error)
                with patch(f'{DERIVED}.get_data_by_param', lookup):
                    await _enrich_via_api(redis, _video())
                    await _enrich_via_api(redis, _video())

                self.assertEqual(lookup.await_count, 2)
                self.assertNotIn(CHANNEL_NO_COUNTRY_HASH, redis.hashes)

    async def test_missing_data_file_is_not_cached(self) -> None:
        '''A 404 downloading the record's file is not "no country".'''
        redis: FakeRedis = FakeRedis()
        with patch(f'{DERIVED}.get_data_by_param', AsyncMock()), \
                patch(
                    f'{DERIVED}.fetch_youtube_channel',
                    AsyncMock(side_effect=_http_error(404)),
                ):
            await _enrich_via_api(redis, _video())

        self.assertNotIn(CHANNEL_NO_COUNTRY_HASH, redis.hashes)

    async def test_expired_entry_is_rechecked_and_replaced(self) -> None:
        redis: FakeRedis = FakeRedis()
        await redis.hset(
            CHANNEL_NO_COUNTRY_HASH, 'UC123', f'{time.time() - 1:.0f}',
        )
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.country = 'NL'
        lookup: AsyncMock = AsyncMock()
        with patch(f'{DERIVED}.get_data_by_param', lookup), \
                patch(
                    f'{DERIVED}.fetch_youtube_channel',
                    AsyncMock(return_value=channel),
                ):
            video: YouTubeVideo = _video()
            await _enrich_via_api(redis, video)

        self.assertEqual(lookup.await_count, 1)
        self.assertEqual(video.channel_country, 'NL')
        self.assertEqual(redis.hashes[CHANNEL_COUNTRY_HASH]['UC123'], 'NL')
        self.assertNotIn('UC123', redis.hashes[CHANNEL_NO_COUNTRY_HASH])

    async def test_set_channel_country_clears_no_country_entry(
        self,
    ) -> None:
        redis: FakeRedis = FakeRedis()
        await redis.hset(
            CHANNEL_NO_COUNTRY_HASH, 'UC123', f'{time.time() + 60:.0f}',
        )

        await set_channel_country(redis, 'UC123', 'DE')

        self.assertEqual(redis.hashes[CHANNEL_COUNTRY_HASH]['UC123'], 'DE')
        self.assertNotIn('UC123', redis.hashes[CHANNEL_NO_COUNTRY_HASH])


if __name__ == '__main__':
    unittest.main()
