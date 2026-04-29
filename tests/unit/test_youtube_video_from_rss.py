'''
Unit tests for YouTubeVideo.from_rss_entry.

These tests parse inline RSS XML through the real untangle stack
(no mocks) but do no network I/O, so they run under the unit
suite. The companion live-network test lives at
tests/integration/test_youtube_video_from_rss_entry.py.
'''

import unittest

import untangle

from scrape_exchange.youtube.youtube_thumbnail import (
    YouTubeThumbnail,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


_TEST_HANDLE: str = 'CanonicalHandle'


def _parse_entries(xml: str) -> list[untangle.Element]:
    '''Parse an RSS feed payload and return its entries as a
    list, matching what ``fetch_rss`` in the RSS scraper does.'''
    feed: untangle.Element = untangle.parse(xml)
    raw: list | object = getattr(feed.feed, 'entry', [])
    if not isinstance(raw, list):
        raw = [raw]
    return list(raw)


_FULL_ENTRY_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>yt:video:abcdefghijk</id>
    <yt:videoId>abcdefghijk</yt:videoId>
    <yt:channelId>UCtestchannel0123456789</yt:channelId>
    <title>Sample video title</title>
    <link rel="alternate"
          href="https://www.youtube.com/watch?v=abcdefghijk"/>
    <author>
      <name>Sample Channel</name>
      <uri>https://www.youtube.com/channel/UCtestchannel0123456789</uri>
    </author>
    <published>2024-06-15T10:30:00+00:00</published>
    <updated>2024-06-16T08:00:00+00:00</updated>
    <media:group>
      <media:title>Sample video title</media:title>
      <media:content
          url="https://www.youtube.com/v/abcdefghijk?version=3"
          type="application/x-shockwave-flash"
          width="640" height="390"/>
      <media:thumbnail
          url="https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
          width="480" height="360"/>
      <media:description>Line one.
Line two.</media:description>
      <media:community>
        <media:starRating count="42" average="4.8"
                          min="1" max="5"/>
        <media:statistics views="12345"/>
      </media:community>
    </media:group>
  </entry>
</feed>'''


class TestFromRssEntryFullEntry(unittest.TestCase):
    '''A fully populated real-shape entry must map every
    supported RSS field onto the YouTubeVideo.'''

    entry: untangle.Element

    @classmethod
    def setUpClass(cls) -> None:
        cls.entry = _parse_entries(_FULL_ENTRY_XML)[0]

    def test_core_fields(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(video.video_id, 'abcdefghijk')
        self.assertEqual(video.title, 'Sample video title')
        self.assertEqual(
            video.url,
            'https://www.youtube.com/watch?v=abcdefghijk',
        )
        self.assertEqual(
            video.embed_url,
            'https://www.youtube.com/embed/abcdefghijk',
        )

    def test_channel_fields(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(
            video.channel_id,
            'UCtestchannel0123456789',
        )
        # channel_handle is the value passed in, never derived
        # from the RSS <author><name>.
        self.assertEqual(video.channel_handle, _TEST_HANDLE)
        self.assertEqual(
            video.channel_url,
            'https://www.youtube.com/channel/'
            'UCtestchannel0123456789',
        )

    def test_timestamps_parse_as_aware(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(
            video.published_timestamp.year, 2024,
        )
        self.assertEqual(
            video.published_timestamp.month, 6,
        )
        self.assertEqual(
            video.published_timestamp.day, 15,
        )
        self.assertIsNotNone(
            video.published_timestamp.tzinfo,
        )
        self.assertGreater(
            video.uploaded_timestamp,
            video.published_timestamp,
        )

    def test_description_preserves_newlines(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(
            video.description, 'Line one.\nLine two.',
        )

    def test_view_count_parses_as_int(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(video.view_count, 12345)

    def test_thumbnail_captured_with_dimensions(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            self.entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(len(video.thumbnails), 1)
        thumb: YouTubeThumbnail = next(
            iter(video.thumbnails.values()),
        )
        self.assertEqual(
            thumb.url,
            'https://i.ytimg.com/vi/abcdefghijk/'
            'hqdefault.jpg',
        )
        self.assertEqual(thumb.width, 480)
        self.assertEqual(thumb.height, 360)


_MINIMAL_ENTRY_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>minimalidxyz</yt:videoId>
    <title>Just the required bits</title>
  </entry>
</feed>'''


class TestFromRssEntryMinimal(unittest.TestCase):
    '''Every optional RSS field is skipped silently; defaults
    from YouTubeVideo.__init__ remain in place. ``channel_handle``
    is required and so is always set, even on minimal entries.'''

    def test_required_fields_populate(self) -> None:
        entry: untangle.Element = _parse_entries(
            _MINIMAL_ENTRY_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(video.video_id, 'minimalidxyz')
        self.assertEqual(
            video.title, 'Just the required bits',
        )

    def test_optional_fields_keep_defaults(self) -> None:
        entry: untangle.Element = _parse_entries(
            _MINIMAL_ENTRY_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle=_TEST_HANDLE,
        )
        self.assertIsNone(video.channel_id)
        # channel_handle is required and is always set.
        self.assertEqual(video.channel_handle, _TEST_HANDLE)
        self.assertIsNone(video.channel_url)
        self.assertIsNone(video.published_timestamp)
        self.assertIsNone(video.uploaded_timestamp)
        self.assertIsNone(video.description)
        self.assertIsNone(video.view_count)
        self.assertEqual(video.thumbnails, {})


_MISSING_VIDEO_ID_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>No videoId here</title>
  </entry>
</feed>'''


_MISSING_TITLE_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>notitleok12</yt:videoId>
  </entry>
</feed>'''


class TestFromRssEntryRaisesOnMissingRequired(
    unittest.TestCase,
):
    '''Missing video_id or title must raise AttributeError —
    the RSS scraper relies on this to log + skip the entry at
    tools/yt_rss_scrape.py fetch_rss.'''

    def test_missing_video_id_raises(self) -> None:
        entry: untangle.Element = _parse_entries(
            _MISSING_VIDEO_ID_XML,
        )[0]
        with self.assertRaises(AttributeError):
            YouTubeVideo.from_rss_entry(
                entry, channel_handle=_TEST_HANDLE,
            )

    def test_missing_title_raises(self) -> None:
        entry: untangle.Element = _parse_entries(
            _MISSING_TITLE_XML,
        )[0]
        with self.assertRaises(AttributeError):
            YouTubeVideo.from_rss_entry(
                entry, channel_handle=_TEST_HANDLE,
            )


_MALFORMED_TIMESTAMP_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>badtimes123</yt:videoId>
    <title>Bad timestamp</title>
    <published>this is not a date</published>
    <updated>2024-06-15T10:30:00+00:00</updated>
  </entry>
</feed>'''


class TestFromRssEntryMalformedTimestamp(
    unittest.TestCase,
):
    '''A malformed published timestamp must not break the
    whole entry — the other fields should still be set.'''

    def test_malformed_published_is_skipped_not_raised(
        self,
    ) -> None:
        entry: untangle.Element = _parse_entries(
            _MALFORMED_TIMESTAMP_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(video.video_id, 'badtimes123')
        self.assertIsNone(video.published_timestamp)
        # The well-formed updated timestamp must still parse.
        self.assertEqual(
            video.uploaded_timestamp.year, 2024,
        )


_THUMBNAIL_NO_DIMENSIONS_XML: str = (
    '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>nodimthumbx</yt:videoId>
    <title>No dims on thumb</title>
    <media:group>
      <media:thumbnail
          url="https://i.ytimg.com/vi/nodimthumbx/hqdefault.jpg"/>
    </media:group>
  </entry>
</feed>'''
)


class TestFromRssEntryThumbnailWithoutDimensions(
    unittest.TestCase,
):
    '''Real YouTube feeds sometimes emit a thumbnail element
    without ``width``/``height`` attributes. The URL must
    still be captured; missing dimensions are skipped
    silently.'''

    def test_thumbnail_url_captured_without_dimensions(
        self,
    ) -> None:
        entry: untangle.Element = _parse_entries(
            _THUMBNAIL_NO_DIMENSIONS_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle=_TEST_HANDLE,
        )
        self.assertEqual(len(video.thumbnails), 1)
        thumb: YouTubeThumbnail = next(
            iter(video.thumbnails.values()),
        )
        self.assertEqual(
            thumb.url,
            'https://i.ytimg.com/vi/nodimthumbx/'
            'hqdefault.jpg',
        )
        self.assertIsNone(thumb.width)
        self.assertIsNone(thumb.height)


_ENTRY_WITH_AUTHOR_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>overrideid01</yt:videoId>
    <yt:channelId>UCoverridechannel0000000</yt:channelId>
    <title>Override target</title>
    <author>
      <name>RSS Author Name</name>
      <uri>https://www.youtube.com/channel/UCoverridechannel0000000</uri>
    </author>
  </entry>
</feed>'''


_ENTRY_WITHOUT_AUTHOR_XML: str = '''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <yt:videoId>noauthorid01</yt:videoId>
    <yt:channelId>UCnoauthorchannel000000</yt:channelId>
    <title>No author element</title>
  </entry>
</feed>'''


class TestFromRssEntryChannelHandle(unittest.TestCase):
    '''``channel_handle`` is a required parameter; the RSS
    entry's ``<author><name>`` is never consulted. The
    ``channel_url`` continues to come from the RSS entry's
    ``<author><uri>`` when present.'''

    def test_handle_ignores_rss_author_name(self) -> None:
        entry: untangle.Element = _parse_entries(
            _ENTRY_WITH_AUTHOR_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle='CanonicalHandle',
        )
        self.assertEqual(
            video.channel_handle, 'CanonicalHandle',
        )
        # channel_url still comes from the RSS entry.
        self.assertEqual(
            video.channel_url,
            'https://www.youtube.com/channel/'
            'UCoverridechannel0000000',
        )

    def test_handle_set_when_author_missing(self) -> None:
        '''When the entry has no <author> block, channel_handle
        is still set from the parameter and channel_url stays
        unset.'''
        entry: untangle.Element = _parse_entries(
            _ENTRY_WITHOUT_AUTHOR_XML,
        )[0]
        video: YouTubeVideo = YouTubeVideo.from_rss_entry(
            entry, channel_handle='CanonicalHandle',
        )
        self.assertEqual(
            video.channel_handle, 'CanonicalHandle',
        )
        self.assertIsNone(video.channel_url)


if __name__ == '__main__':
    unittest.main()
