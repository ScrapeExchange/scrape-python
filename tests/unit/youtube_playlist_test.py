#!/usr/bin/env python3
'''
Unit tests for YouTubePlaylist
'''

import unittest

from scrape_exchange.youtube.youtube_playlist import YouTubePlaylist


# Sample lockupViewModel as returned by the InnerTube browse API
SAMPLE_LOCKUP: dict = {
    'lockupViewModel': {
        'contentId': 'PLm8I5TkIJrVlmyIJIgeFaBFUnCb0Gg09c',
        'contentType': 'LOCKUP_CONTENT_TYPE_PLAYLIST',
        'metadata': {
            'lockupMetadataViewModel': {
                'title': {
                    'content': 'The Cold War (1945-1991)'
                },
                'metadata': {
                    'contentMetadataViewModel': {
                        'metadataRows': [
                            {
                                'metadataParts': [
                                    {
                                        'text': {
                                            'content': 'View full playlist',
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        },
        'contentImage': {
            'collectionThumbnailViewModel': {
                'primaryThumbnail': {
                    'thumbnailViewModel': {
                        'image': {
                            'sources': [
                                {
                                    'url': 'https://i.ytimg.com/vi/qBryX5K-XHY/hqdefault.jpg',
                                    'width': 480,
                                    'height': 270
                                }
                            ]
                        },
                        'overlays': [
                            {
                                'thumbnailOverlayBadgeViewModel': {
                                    'thumbnailBadges': [
                                        {
                                            'thumbnailBadgeViewModel': {
                                                'text': '22 videos',
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
}


class TestYouTubePlaylist(unittest.TestCase):

    def test_from_innertube(self) -> None:
        playlist: YouTubePlaylist | None = YouTubePlaylist.from_innertube(
            SAMPLE_LOCKUP, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(playlist)
        self.assertEqual(
            playlist.playlist_id, 'PLm8I5TkIJrVlmyIJIgeFaBFUnCb0Gg09c'
        )
        self.assertEqual(playlist.title, 'The Cold War (1945-1991)')
        self.assertEqual(playlist.video_count, 22)
        self.assertEqual(
            playlist.thumbnail_url,
            'https://i.ytimg.com/vi/qBryX5K-XHY/hqdefault.jpg'
        )
        self.assertEqual(
            playlist.channel_id, 'UC22BdTgxefuvUivrjesETjg'
        )
        self.assertEqual(
            playlist.url,
            'https://www.youtube.com/playlist?list=PLm8I5TkIJrVlmyIJIgeFaBFUnCb0Gg09c'
        )

    def test_from_innertube_non_playlist(self) -> None:
        item: dict = {
            'lockupViewModel': {
                'contentId': 'abc',
                'contentType': 'LOCKUP_CONTENT_TYPE_PODCAST',
            }
        }
        self.assertIsNone(
            YouTubePlaylist.from_innertube(item)
        )

    def test_from_innertube_empty(self) -> None:
        self.assertIsNone(YouTubePlaylist.from_innertube({}))

    def test_to_dict_from_dict_round_trip(self) -> None:
        playlist = YouTubePlaylist(
            playlist_id='PLtest123',
            title='Test Playlist',
            video_count=10,
            thumbnail_url='https://example.com/thumb.jpg',
            channel_id='UCtest',
        )
        data: dict = playlist.to_dict()
        restored: YouTubePlaylist = YouTubePlaylist.from_dict(data)
        self.assertEqual(playlist, restored)

    def test_to_dict_fields(self) -> None:
        playlist = YouTubePlaylist(
            playlist_id='PLtest',
            title='My Playlist',
            video_count=5,
            thumbnail_url='https://example.com/t.jpg',
            channel_id='UCabc',
        )
        d: dict[str, any] = playlist.to_dict()
        self.assertEqual(d['playlist_id'], 'PLtest')
        self.assertEqual(d['title'], 'My Playlist')
        self.assertEqual(d['video_count'], 5)
        self.assertEqual(d['thumbnail_url'], 'https://example.com/t.jpg')
        self.assertEqual(d['channel_id'], 'UCabc')
        self.assertEqual(
            d['url'],
            'https://www.youtube.com/playlist?list=PLtest'
        )
        self.assertIn('created_timestamp', d)

    def test_hash_and_equality(self) -> None:
        a = YouTubePlaylist(playlist_id='PL1', title='A', video_count=1)
        b = YouTubePlaylist(playlist_id='PL1', title='A', video_count=1)
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

        s: set = {a, b}
        self.assertEqual(len(s), 1)

    def test_repr(self) -> None:
        p = YouTubePlaylist(
            playlist_id='PL1', title='Test', video_count=3
        )
        self.assertIn('PL1', repr(p))
        self.assertIn('Test', repr(p))
        self.assertIn('3', repr(p))


if __name__ == '__main__':
    unittest.main()
