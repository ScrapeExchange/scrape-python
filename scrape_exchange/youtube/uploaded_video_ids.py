'''Redis Bloom filter of YouTube video IDs already uploaded.'''

from collections.abc import Iterable
from typing import ClassVar

import redis.asyncio as aioredis

from scrape_exchange.redis_client import redis_from_url

# Sizing of the Bloom filter. At a 1e-5 false-positive rate a
# Bloom filter needs ~24 bits per ID: ~600 MB at full capacity,
# versus ~40 bytes per ID for the SET it replaces. A false
# positive makes a never-uploaded video look uploaded, so it is
# skipped (about 1 in 100,000). Past the capacity Redis adds
# sub-filters (EXPANSION) instead of degrading the error rate.
UPLOADED_BF_CAPACITY: int = 200_000_000
UPLOADED_BF_ERROR_RATE: float = 0.00001
UPLOADED_BF_EXPANSION: int = 2


class UploadedVideoIds:
    '''Fleet-wide set of YouTube IDs uploaded to scrape.exchange,
    stored as a Bloom filter: membership may report a false
    positive at UPLOADED_BF_ERROR_RATE, never a false negative.'''

    _KEY: ClassVar[str] = 'youtube:video:uploaded_bf'

    def __init__(
        self, redis_dsn: str, *, redis_client: aioredis.Redis | None = None,
    ) -> None:
        self._client: aioredis.Redis = redis_client if (
            redis_client is not None
        ) else redis_from_url(
            redis_dsn,
            component='youtube-uploaded-video-ids',
            decode_responses=True,
        )

    async def contains(self, video_id: str) -> bool:
        '''Return whether *video_id* is (probably) uploaded.'''
        return bool(
            await self._client.execute_command(
                'BF.EXISTS', self._KEY, video_id,
            ),
        )

    async def contains_many(
        self,
        video_ids: Iterable[str],
    ) -> dict[str, bool]:
        '''Return uploaded membership for each input ID.'''
        ids: list[str] = list(video_ids)
        if not ids:
            return {}
        flags: list[int] = await self._client.execute_command(
            'BF.MEXISTS', self._KEY, *ids,
        )
        return {
            video_id: bool(flag)
            for video_id, flag in zip(ids, flags)
        }

    async def add(self, video_id: str) -> None:
        '''Record *video_id* as uploaded.'''
        await self.add_many([video_id])

    async def add_many(self, video_ids: Iterable[str]) -> None:
        '''Record *video_ids* as uploaded. The first write creates
        the filter with the configured sizing.'''
        ids: list[str] = [v for v in video_ids if v]
        if not ids:
            return
        await self._client.execute_command(
            'BF.INSERT', self._KEY,
            'CAPACITY', UPLOADED_BF_CAPACITY,
            'ERROR', f'{UPLOADED_BF_ERROR_RATE:f}',
            'EXPANSION', UPLOADED_BF_EXPANSION,
            'ITEMS', *ids,
        )
