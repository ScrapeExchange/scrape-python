'''
Unit tests for ``InnerTubeVideoParser._apply_player_data``.

The mapping was previously inlined in ``scrape()``; extracting it as a
pure function lets us cover one specific class of bug without
mocking the InnerTube transport.

Bug class: identity fields (``channel_id``, ``channel_handle``,
``channel_url``, ``title``, ``description``, ``url``, ``embed_url``)
must NOT be clobbered when YouTube's ``player`` response includes the
key with an empty/None value — common for unavailable, deleted, or
region-blocked videos.  The previous code path used
``video_details.get('key', existing)``, which only honours the
default when the key is absent; this test set asserts the new
``response or existing`` semantics work for the full set of keys.
'''

import unittest

from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video_innertube import (
    InnerTubeVideoParser,
)


def _video_with_rss_data() -> YouTubeVideo:
    '''
    Build a YouTubeVideo populated as if ``from_rss_entry`` had just
    run on a feed whose ``<entry>`` carried the full set of identity
    fields.  Each test then runs ``_apply_player_data`` against a
    crafted ``player_data`` dict to verify the merge behaviour.
    '''
    video: YouTubeVideo = YouTubeVideo(
        video_id='vid12345', channel_handle='OriginalHandle',
    )
    video.channel_id = 'UCabcdefghijABCDEFGHIJ'
    video.channel_url = 'https://youtube.com/@OriginalHandle'
    video.title = 'Original RSS title'
    video.description = 'Original RSS description'
    video.url = 'https://youtu.be/vid12345'
    video.embed_url = 'https://www.youtube.com/embed/vid12345'
    return video


class TestApplyPlayerDataIdentityFieldsNotClobbered(
    unittest.TestCase,
):
    '''
    These cases reproduce the runtime crash that motivated the fix:
    the bulk uploader received video-min files where InnerTube had
    overwritten RSS-set channel_id/channel_handle with empty
    strings, causing ``resolve_video_upload_handle`` to call
    ``fallback_handle('')`` and abort the whole bulk batch.
    '''

    def test_empty_string_channel_id_does_not_clobber(self) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'channelId': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')

    def test_none_channel_id_does_not_clobber(self) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'channelId': None},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')

    def test_empty_string_author_does_not_clobber_handle(
        self,
    ) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'author': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_handle, 'OriginalHandle')

    def test_missing_keys_preserve_existing_values(self) -> None:
        '''Sanity: when InnerTube returns no identity keys at all
        (the original "default" semantics), existing values are
        kept too — so the fix is strictly more permissive, not
        regressing the absent-key case.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')
        self.assertEqual(video.channel_handle, 'OriginalHandle')
        self.assertEqual(video.title, 'Original RSS title')
        self.assertEqual(video.description, 'Original RSS description')

    def test_present_values_overwrite_existing(self) -> None:
        '''Happy path: when InnerTube returns concrete identity
        values they replace the RSS-sourced placeholders.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {
                'channelId': 'UCnewchannelidNEW123456',
                'author': 'CanonicalHandle',
                'title': 'Canonical title',
                'shortDescription': 'Canonical description',
            },
            'microformat': {
                'playerMicroformatRenderer': {
                    'canonicalUrl':
                        'https://www.youtube.com/watch?v=vid12345',
                    'ownerProfileUrl':
                        'https://youtube.com/@CanonicalHandle',
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCnewchannelidNEW123456')
        self.assertEqual(video.channel_handle, 'CanonicalHandle')
        self.assertEqual(video.title, 'Canonical title')
        self.assertEqual(video.description, 'Canonical description')
        self.assertEqual(
            video.url,
            'https://www.youtube.com/watch?v=vid12345',
        )
        self.assertEqual(
            video.channel_url,
            'https://youtube.com/@CanonicalHandle',
        )

    def test_empty_microformat_url_fields_do_not_clobber(
        self,
    ) -> None:
        '''Same fix applied to the microformat-sourced URL fields:
        an empty ``canonicalUrl``, ``ownerProfileUrl``, or embed
        ``iframeUrl`` must not blank an RSS-sourced value.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {},
            'microformat': {
                'playerMicroformatRenderer': {
                    'canonicalUrl': '',
                    'ownerProfileUrl': '',
                    'embed': {'iframeUrl': ''},
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(
            video.url, 'https://youtu.be/vid12345',
        )
        self.assertEqual(
            video.channel_url,
            'https://youtube.com/@OriginalHandle',
        )
        self.assertEqual(
            video.embed_url,
            'https://www.youtube.com/embed/vid12345',
        )

    def test_empty_title_and_description_do_not_clobber(
        self,
    ) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'title': '', 'shortDescription': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.title, 'Original RSS title')
        self.assertEqual(
            video.description, 'Original RSS description',
        )


if __name__ == '__main__':
    unittest.main()
