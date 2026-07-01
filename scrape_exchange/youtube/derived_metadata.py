'''
Helpers for locally-derived YouTube channel/video metadata.

Scrapers use these helpers to enrich freshly-scraped records before
they are written locally and uploaded to scrape.exchange.
'''

from __future__ import annotations

import logging

from pathlib import Path
from typing import Any

from httpx import HTTPStatusError

from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
    COMPRESSED_JSON_SUFFIX,
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
)
from scrape_exchange.scrape_api import (
    fetch_youtube_channel,
    get_data_by_param,
)
from scrape_exchange.datatypes import Platform
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo


LOGGER: logging.Logger = logging.getLogger(__name__)

CATEGORY_THRESHOLD: int = 20
CHANNEL_CATEGORY_COUNTS_PREFIX: str = 'youtube:channel_category_counts:'
CHANNELS_WITH_CATEGORY_COUNTS: str = 'youtube:channels_with_category_counts'
CHANNEL_COUNTRY_HASH: str = 'youtube:channel_country'
SCHEMA_VERSION: str = '0.0.2'


def normalize_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized: str = ' '.join(value.split())
    return normalized or None


def channel_category_counts_key(channel_id: str) -> str:
    return f'{CHANNEL_CATEGORY_COUNTS_PREFIX}{channel_id}'


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return str(value)


async def get_channel_country(
    redis: Any | None,
    channel_id: str,
) -> str | None:
    if redis is None:
        return None
    value: Any = await redis.hget(CHANNEL_COUNTRY_HASH, channel_id)
    if value is None:
        return None
    country: str = _decode(value).strip()
    return country or None


async def set_channel_country(
    redis: Any | None,
    channel_id: str,
    country: str | None,
) -> None:
    if redis is None or not country:
        return
    await redis.hset(CHANNEL_COUNTRY_HASH, channel_id, country)


async def increment_channel_category_count(
    redis: Any | None,
    channel_id: str | None,
    category: str | None,
) -> None:
    normalized: str | None = normalize_category(category)
    if redis is None or not channel_id or not normalized:
        return
    key: str = channel_category_counts_key(channel_id)
    await redis.hincrby(key, normalized, 1)
    await redis.sadd(CHANNELS_WITH_CATEGORY_COUNTS, channel_id)


async def get_channel_category_counts(
    redis: Any | None,
    channel_id: str,
) -> dict[str, int]:
    if redis is None:
        return {}
    raw: dict = await redis.hgetall(channel_category_counts_key(channel_id))
    counts: dict[str, int] = {}
    for key, value in raw.items():
        try:
            counts[_decode(key)] = int(_decode(value))
        except ValueError:
            continue
    return counts


def select_channel_category(
    counts: dict[str, int],
    *,
    threshold: int = CATEGORY_THRESHOLD,
) -> str | None:
    if not counts:
        return None
    ordered: list[tuple[str, int]] = sorted(
        counts.items(), key=lambda item: (-item[1], item[0])
    )
    winner, winner_count = ordered[0]
    if winner_count <= threshold:
        return None
    if len(ordered) > 1 and ordered[1][1] == winner_count:
        return None
    return winner


async def infer_channel_category_from_redis(
    redis: Any | None,
    channel_id: str,
) -> str | None:
    return select_channel_category(
        await get_channel_category_counts(redis, channel_id)
    )


def _channel_filename(channel_id: str) -> str:
    return f'{CHANNEL_FILE_PREFIX}{channel_id}{COMPRESSED_JSON_SUFFIX}'


def _video_filenames(video_id: str) -> tuple[str, str]:
    return (
        f'{VIDEO_YTDLP_FILE_PREFIX}{video_id}{COMPRESSED_JSON_SUFFIX}',
        f'{VIDEO_MIN_FILE_PREFIX}{video_id}{COMPRESSED_JSON_SUFFIX}',
    )


async def local_channel_country(
    channel_data_directory: str | None,
    channel_id: str,
) -> str | None:
    if not channel_data_directory:
        return None
    fm: AssetFileManagement = AssetFileManagement(channel_data_directory)
    filename: str = _channel_filename(channel_id)
    for uploaded in (False, True):
        path: Path = (
            fm.uploaded_dir / filename if uploaded
            else fm.base_dir / filename
        )
        if not path.is_file():
            continue
        try:
            data: dict = (
                await fm.read_uploaded(filename)
                if uploaded else await fm.read_file(filename)
            )
            channel: YouTubeChannel = YouTubeChannel.from_dict(data)
        except Exception:
            LOGGER.warning(
                'Failed to read local channel for derived country',
                exc_info=True,
                extra={'channel_id': channel_id, 'filename': filename},
            )
            continue
        if channel.country:
            return channel.country
    return None


async def api_channel_country(
    client: ExchangeClient | None,
    *,
    username: str | None,
    channel_id: str,
) -> str | None:
    if client is None or not username:
        return None
    try:
        entry = await get_data_by_param(
            client,
            username=username,
            platform=Platform.YOUTUBE,
            entity='channel',
            version=SCHEMA_VERSION,
            platform_content_id=channel_id,
        )
        channel: YouTubeChannel = await fetch_youtube_channel(
            client, entry,
        )
    except HTTPStatusError as exc:
        if exc.response.status_code != 404:
            LOGGER.warning(
                'Failed API channel country lookup',
                exc_info=True,
                extra={'channel_id': channel_id},
            )
        return None
    except Exception:
        LOGGER.warning(
            'Failed API channel country lookup',
            exc_info=True,
            extra={'channel_id': channel_id},
        )
        return None
    return channel.country


async def enrich_video_channel_country(
    video: YouTubeVideo,
    *,
    redis: Any | None,
    channel_data_directory: str | None,
    client: ExchangeClient | None,
    username: str | None,
) -> None:
    if video.channel_country or not video.channel_id:
        return
    country: str | None = await get_channel_country(
        redis, video.channel_id,
    )
    if country is None:
        country = await local_channel_country(
            channel_data_directory, video.channel_id,
        )
    if country is None:
        country = await api_channel_country(
            client,
            username=username,
            channel_id=video.channel_id,
        )
    if country:
        video.channel_country = country
        await set_channel_country(redis, video.channel_id, country)


async def local_video_category_counts(
    video_data_directory: str | None,
    video_ids: set[str],
) -> dict[str, int]:
    if not video_data_directory or not video_ids:
        return {}
    fm: AssetFileManagement = AssetFileManagement(video_data_directory)
    counts: dict[str, int] = {}
    for video_id in sorted(video_ids):
        found: bool = False
        for filename in _video_filenames(video_id):
            for uploaded in (False, True):
                path: Path = (
                    fm.uploaded_dir / filename if uploaded
                    else fm.base_dir / filename
                )
                if not path.is_file():
                    continue
                try:
                    data: dict = (
                        await fm.read_uploaded(filename)
                        if uploaded else await fm.read_file(filename)
                    )
                    video: YouTubeVideo = YouTubeVideo.from_dict(data)
                except Exception:
                    LOGGER.warning(
                        'Failed to read local video for category counts',
                        exc_info=True,
                        extra={'video_id': video_id, 'filename': filename},
                    )
                    continue
                category: str | None = normalize_category(video.category)
                if category:
                    counts[category] = counts.get(category, 0) + 1
                found = True
                break
            if found:
                break
    return counts


async def merge_category_counts_into_redis(
    redis: Any | None,
    channel_id: str,
    counts: dict[str, int],
) -> None:
    if redis is None or not counts:
        return
    key: str = channel_category_counts_key(channel_id)
    for category, count in counts.items():
        if count > 0:
            current_value: Any = await redis.hget(key, category)
            current_count: int = 0
            if current_value is not None:
                try:
                    current_count = int(_decode(current_value))
                except ValueError:
                    current_count = 0
            if count > current_count:
                await redis.hset(key, category, str(count))
    await redis.sadd(CHANNELS_WITH_CATEGORY_COUNTS, channel_id)


def _merge_category_count_max(
    current: dict[str, int],
    observed: dict[str, int],
) -> dict[str, int]:
    merged: dict[str, int] = dict(current)
    for category, count in observed.items():
        merged[category] = max(merged.get(category, 0), count)
    return merged


async def enrich_channel_category(
    channel: YouTubeChannel,
    *,
    redis: Any | None,
    video_data_directory: str | None,
) -> None:
    if channel.category or not channel.channel_id:
        return
    redis_counts: dict[str, int] = await get_channel_category_counts(
        redis, channel.channel_id,
    )
    category: str | None = select_channel_category(redis_counts)
    if category is None:
        local_counts: dict[str, int] = await local_video_category_counts(
            video_data_directory, channel.video_ids,
        )
        await merge_category_counts_into_redis(
            redis, channel.channel_id, local_counts,
        )
        combined_counts: dict[str, int] = _merge_category_count_max(
            redis_counts, local_counts,
        )
        category = select_channel_category(combined_counts)
    if category:
        channel.category = category
