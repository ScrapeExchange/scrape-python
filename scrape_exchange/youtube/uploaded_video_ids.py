'''Redis-backed set of YouTube video IDs already uploaded.'''

from typing import ClassVar, Iterable

from scrape_exchange.redis_client import redis_from_url


class UploadedVideoIds:
    '''Fleet-wide set of YouTube IDs uploaded to scrape.exchange.'''

    _KEY: ClassVar[str] = 'youtube:video:uploaded'

    def __init__(self, redis_dsn: str) -> None:
        self._client = redis_from_url(
            redis_dsn,
            component='youtube-uploaded-video-ids',
            decode_responses=True,
        )

    async def contains(self, video_id: str) -> bool:
        '''Return whether *video_id* is already uploaded.'''
        return bool(
            await self._client.sismember(self._KEY, video_id),
        )

    async def contains_many(
        self,
        video_ids: Iterable[str],
    ) -> dict[str, bool]:
        '''Return uploaded membership for each input ID.'''
        ids: list[str] = list(video_ids)
        if not ids:
            return {}
        flags: list[int] = await self._client.smismember(
            self._KEY, *ids,
        )
        return {
            video_id: bool(flag)
            for video_id, flag in zip(ids, flags)
        }

    async def add(self, video_id: str) -> None:
        '''Record *video_id* as uploaded.'''
        await self._client.sadd(self._KEY, video_id)
