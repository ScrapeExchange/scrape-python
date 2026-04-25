'''Integration: handle written by channel/RSS reaches video uploads.'''

import os
import tempfile
import unittest

from scrape_exchange.creator_map import FileCreatorMap
from scrape_exchange.youtube.youtube_video import YouTubeVideo

from tools.yt_video_scrape import resolve_video_upload_handle


class TestHandlePropagation(unittest.IsolatedAsyncioTestCase):
    async def test_video_reads_handle_written_by_channel_scraper(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            map_path: str = os.path.join(tmp, 'creator_map.csv')

            # Channel scraper writes the canonical handle.
            writer: FileCreatorMap = FileCreatorMap(map_path)
            await writer.put(
                'UC1234567890abcdefghij', 'CanonicalHandle',
            )

            # Simulate a video from that channel with a different
            # casing in its RSS-populated channel_name.
            video: YouTubeVideo = YouTubeVideo(video_id='vid1234')
            video.channel_id = 'UC1234567890abcdefghij'
            video.channel_name = 'canonicalhandle'

            # Fresh FileCreatorMap instance (another process) reading
            # the same file.
            reader: FileCreatorMap = FileCreatorMap(map_path)
            handle: str | None = await resolve_video_upload_handle(
                video, reader, proxy=None,
            )

            self.assertEqual(handle, 'CanonicalHandle')
