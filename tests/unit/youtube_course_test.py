#!/usr/bin/env python3
'''
Unit tests for YouTubeCourse and YouTubeCourseVideo
'''

import unittest

from scrape_exchange.youtube.youtube_course import (
    YouTubeCourse, YouTubeCourseVideo
)


# Sample richItemRenderer as returned by the InnerTube browse API
# on the courses tab
SAMPLE_COURSE_ITEM: dict = {
    'richItemRenderer': {
        'content': {
            'playlistRenderer': {
                'playlistId': 'PLi01XoE8jYogSx77uul015dNBA5fn8sMT',
                'title': {
                    'simpleText': 'Quantitative Finance'
                },
                'videoCount': '14',
                'thumbnails': [
                    {
                        'thumbnails': [
                            {
                                'url': 'https://i.ytimg.com/vi/VeCRV8MP7xA/hqdefault.jpg',
                                'width': 480,
                                'height': 270
                            }
                        ]
                    }
                ],
                'videos': [
                    {
                        'childVideoRenderer': {
                            'title': {
                                'simpleText': 'Stock Options Survival Guide'
                            },
                            'navigationEndpoint': {
                                'watchEndpoint': {
                                    'videoId': 'VeCRV8MP7xA',
                                    'playlistId': 'PLi01XoE8jYogSx77uul015dNBA5fn8sMT'
                                }
                            },
                            'lengthText': {
                                'accessibility': {
                                    'accessibilityData': {
                                        'label': '10 minutes, 43 seconds'
                                    }
                                }
                            }
                        }
                    },
                    {
                        'childVideoRenderer': {
                            'title': {
                                'simpleText': 'Stock Options Course v1.0'
                            },
                            'navigationEndpoint': {
                                'watchEndpoint': {
                                    'videoId': 'MB0n6CpNw9o',
                                    'playlistId': 'PLi01XoE8jYogSx77uul015dNBA5fn8sMT'
                                }
                            },
                            'lengthText': {
                                'accessibility': {
                                    'accessibilityData': {
                                        'label': '1 minute, 11 seconds'
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
}


class TestYouTubeCourseVideo(unittest.TestCase):

    def test_from_innertube(self) -> None:
        child: dict = SAMPLE_COURSE_ITEM[
            'richItemRenderer'
        ]['content']['playlistRenderer']['videos'][0]
        video: YouTubeCourseVideo = YouTubeCourseVideo.from_innertube(child)
        self.assertIsNotNone(video)
        self.assertEqual(video.video_id, 'VeCRV8MP7xA')
        self.assertEqual(video.title, 'Stock Options Survival Guide')
        self.assertEqual(
            video.duration_label, '10 minutes, 43 seconds'
        )

    def test_from_innertube_empty(self) -> None:
        self.assertIsNone(YouTubeCourseVideo.from_innertube({}))

    def test_to_dict_from_dict_round_trip(self) -> None:
        video = YouTubeCourseVideo(
            video_id='abc',
            title='Test',
            duration_label='5 minutes',
        )
        restored = YouTubeCourseVideo.from_dict(video.to_dict())
        self.assertEqual(video, restored)

    def test_hash(self) -> None:
        a = YouTubeCourseVideo(video_id='x', title='A')
        b = YouTubeCourseVideo(video_id='x', title='A')
        self.assertEqual(hash(a), hash(b))
        self.assertEqual(len({a, b}), 1)


class TestYouTubeCourse(unittest.TestCase):

    def test_from_innertube(self) -> None:
        course: YouTubeCourse = YouTubeCourse.from_innertube(
            SAMPLE_COURSE_ITEM,
            channel_id='UCW6TXMZ5Pq6yL6_k5NZ2e0Q'
        )
        self.assertIsNotNone(course)
        self.assertEqual(
            course.playlist_id,
            'PLi01XoE8jYogSx77uul015dNBA5fn8sMT'
        )
        self.assertEqual(course.title, 'Quantitative Finance')
        self.assertEqual(course.video_count, 14)
        self.assertEqual(
            course.thumbnail_url,
            'https://i.ytimg.com/vi/VeCRV8MP7xA/hqdefault.jpg'
        )
        self.assertEqual(
            course.channel_id, 'UCW6TXMZ5Pq6yL6_k5NZ2e0Q'
        )
        self.assertEqual(len(course.videos), 2)
        self.assertEqual(course.videos[0].video_id, 'VeCRV8MP7xA')
        self.assertEqual(course.videos[1].video_id, 'MB0n6CpNw9o')
        self.assertEqual(
            course.url,
            'https://www.youtube.com/playlist?list='
            'PLi01XoE8jYogSx77uul015dNBA5fn8sMT'
        )

    def test_from_innertube_empty(self) -> None:
        self.assertIsNone(YouTubeCourse.from_innertube({}))

    def test_from_innertube_no_playlist_renderer(self) -> None:
        self.assertIsNone(
            YouTubeCourse.from_innertube({'richItemRenderer': {}})
        )

    def test_to_dict_from_dict_round_trip(self) -> None:
        course = YouTubeCourse(
            playlist_id='PLtest',
            title='Test Course',
            video_count=3,
            thumbnail_url='https://example.com/thumb.jpg',
            channel_id='UCtest',
            videos=[
                YouTubeCourseVideo(
                    video_id='v1', title='Lesson 1',
                    duration_label='5 minutes'
                ),
                YouTubeCourseVideo(
                    video_id='v2', title='Lesson 2',
                    duration_label='10 minutes'
                ),
            ],
        )
        data: dict = course.to_dict()
        restored = YouTubeCourse.from_dict(data)
        self.assertEqual(course, restored)

    def test_to_dict_fields(self) -> None:
        course = YouTubeCourse(
            playlist_id='PLabc',
            title='My Course',
            video_count=5,
            thumbnail_url='https://example.com/t.jpg',
            channel_id='UCxyz',
        )
        d: dict[str, any] = course.to_dict()
        self.assertEqual(d['playlist_id'], 'PLabc')
        self.assertEqual(d['title'], 'My Course')
        self.assertEqual(d['video_count'], 5)
        self.assertEqual(d['thumbnail_url'], 'https://example.com/t.jpg')
        self.assertEqual(d['channel_id'], 'UCxyz')
        self.assertEqual(
            d['url'], 'https://www.youtube.com/playlist?list=PLabc'
        )
        self.assertEqual(d['videos'], [])
        self.assertIn('created_timestamp', d)

    def test_hash_and_equality(self) -> None:
        a = YouTubeCourse(
            playlist_id='PL1', title='A', video_count=1
        )
        b = YouTubeCourse(
            playlist_id='PL1', title='A', video_count=1
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertEqual(len({a, b}), 1)

    def test_repr(self) -> None:
        c = YouTubeCourse(
            playlist_id='PL1', title='Test', video_count=3
        )
        self.assertIn('PL1', repr(c))
        self.assertIn('Test', repr(c))
        self.assertIn('3', repr(c))


if __name__ == '__main__':
    unittest.main()
