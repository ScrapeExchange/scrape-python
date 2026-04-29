'''
Integration tests for YouTubeVideo.from_rss_entry.

Fetches a real YouTube RSS feed over HTTPS and exercises the
full XML → untangle → YouTubeVideo path. A stable, high-profile
channel (Rick Astley) is used so the field shape is
deterministic; the specific values (titles, view counts) are
allowed to vary.

Synthetic-XML edge-case tests live alongside as unit tests at
tests/unit/test_youtube_video_from_rss.py.

Run with::

    uv run python -m unittest \\
        tests.integration.test_youtube_video_from_rss_entry
'''

import unittest

from datetime import datetime

import httpx
import untangle

from scrape_exchange.youtube.youtube_thumbnail import (
    YouTubeThumbnail,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


# Rick Astley — long-lived channel with a stable feed format.
RICK_ASTLEY_CHANNEL_ID: str = 'UCuAXFkgsw1L7xaCfnd5JJOw'
RICK_ASTLEY_HANDLE: str = 'RickAstleyYT'

RSS_URL_TEMPLATE: str = (
    'https://www.youtube.com/feeds/videos.xml?channel_id={cid}'
)


def _parse_entries(xml: str) -> list[untangle.Element]:
    '''Parse an RSS feed payload and return its entries as a
    list, matching what ``fetch_rss`` in the RSS scraper does.'''
    feed: untangle.Element = untangle.parse(xml)
    raw: list | object = getattr(feed.feed, 'entry', [])
    if not isinstance(raw, list):
        raw = [raw]
    return list(raw)


class TestFromRssEntryLive(unittest.TestCase):
    '''Hits live YouTube RSS. Slow and may be flaky under
    network pressure, but catches drift in the upstream feed
    schema that a frozen fixture would not.'''

    entries: list[untangle.Element]

    @classmethod
    def setUpClass(cls) -> None:
        url: str = RSS_URL_TEMPLATE.format(
            cid=RICK_ASTLEY_CHANNEL_ID,
        )
        response: httpx.Response = httpx.get(
            url, timeout=15.0,
        )
        response.raise_for_status()
        cls.entries = _parse_entries(response.text)

    def test_feed_has_entries(self) -> None:
        self.assertGreater(
            len(self.entries), 0,
            msg='YouTube RSS returned an empty feed',
        )

    def test_every_entry_has_video_id_and_title(self) -> None:
        for entry in self.entries:
            video: YouTubeVideo = YouTubeVideo.from_rss_entry(
                entry, channel_handle=RICK_ASTLEY_HANDLE,
            )
            self.assertTrue(
                video.video_id,
                msg='video_id missing',
            )
            self.assertEqual(len(video.video_id), 11)
            self.assertTrue(
                video.title, msg='title missing',
            )

    def test_urls_are_formatted_with_video_id(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entries[0], channel_handle=RICK_ASTLEY_HANDLE,
        )
        self.assertEqual(
            video.url,
            f'https://www.youtube.com/watch?v={video.video_id}',
        )
        self.assertEqual(
            video.embed_url,
            f'https://www.youtube.com/embed/{video.video_id}',
        )

    def test_channel_info_is_populated(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entries[0], channel_handle=RICK_ASTLEY_HANDLE,
        )
        self.assertEqual(
            video.channel_id, RICK_ASTLEY_CHANNEL_ID,
        )
        # channel_handle is always the value the caller passed
        # in; it is never derived from the RSS entry.
        self.assertEqual(
            video.channel_handle, RICK_ASTLEY_HANDLE,
        )
        self.assertTrue(video.channel_url)
        self.assertIn(
            RICK_ASTLEY_CHANNEL_ID, video.channel_url,
        )

    def test_published_and_uploaded_timestamps_are_aware(
        self,
    ) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entries[0], channel_handle=RICK_ASTLEY_HANDLE,
        )
        self.assertIsInstance(
            video.published_timestamp, datetime,
        )
        self.assertIsInstance(
            video.uploaded_timestamp, datetime,
        )
        # YouTube RSS timestamps are always timezone-aware.
        self.assertIsNotNone(
            video.published_timestamp.tzinfo,
        )
        self.assertIsNotNone(
            video.uploaded_timestamp.tzinfo,
        )

    def test_at_least_one_entry_has_view_count(self) -> None:
        '''Some very recent / unlisted entries may lack a view
        count element; over a full feed at least one should
        have it.'''
        counts: list[int | None] = [
            YouTubeVideo.from_rss_entry(
                e, channel_handle=RICK_ASTLEY_HANDLE,
            ).view_count
            for e in self.entries
        ]
        self.assertTrue(
            any(c is not None and c >= 0 for c in counts),
            msg=(
                'No entry in the live feed produced a '
                'numeric view_count — element may have '
                'moved or been removed upstream'
            ),
        )

    def test_first_entry_has_thumbnail(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entries[0], channel_handle=RICK_ASTLEY_HANDLE,
        )
        self.assertTrue(video.thumbnails)
        for thumb in video.thumbnails.values():
            self.assertIsInstance(thumb, YouTubeThumbnail)
            self.assertTrue(thumb.url)


if __name__ == '__main__':
    unittest.main()
