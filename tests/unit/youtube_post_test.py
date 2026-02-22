#!/usr/bin/env python3
'''
Unit tests for YouTubePost
'''

import unittest

from scrape_exchange.youtube.youtube_post import YouTubePost


# Sample backstagePostThreadRenderer items as returned by the InnerTube
# browse API on the posts/community tab

SAMPLE_IMAGE_POST: dict = {
    'backstagePostThreadRenderer': {
        'post': {
            'backstagePostRenderer': {
                'postId': 'UgwaY28p7Vn8bzlQ_5p4AaABCQ',
                'authorText': {
                    'runs': [{'text': 'History Matters'}]
                },
                'contentText': {
                    'runs': [
                        {
                            'text': "Hi all, just thought I'd let you know "
                                    "what videos to expect over the next "
                                    "two weeks. Just to keep everyone "
                                    "informed."
                        }
                    ]
                },
                'publishedTimeText': {
                    'runs': [{'text': '5 years ago'}]
                },
                'voteCount': {
                    'simpleText': '10K',
                    'accessibility': {
                        'accessibilityData': {'label': '10K likes'}
                    }
                },
                'backstageAttachment': {
                    'backstageImageRenderer': {
                        'image': {
                            'thumbnails': [
                                {
                                    'url': 'https://lh3.googleusercontent.com/small.jpg',
                                    'width': 288,
                                    'height': 288
                                },
                                {
                                    'url': 'https://lh3.googleusercontent.com/large.jpg',
                                    'width': 640,
                                    'height': 640
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
}

SAMPLE_POLL_POST: dict = {
    'backstagePostThreadRenderer': {
        'post': {
            'backstagePostRenderer': {
                'postId': 'UgzgLdJRC7hfK07YOLx4AaABCQ',
                'authorText': {
                    'runs': [{'text': 'History Matters'}]
                },
                'contentText': {
                    'runs': [
                        {
                            'text': 'Do you guys think my own political '
                                    'opinions come through in videos? '
                                    'What do you think I am?'
                        }
                    ]
                },
                'publishedTimeText': {
                    'runs': [{'text': '6 years ago'}]
                },
                'voteCount': {
                    'simpleText': '6K',
                    'accessibility': {
                        'accessibilityData': {'label': '6K likes'}
                    }
                },
                'backstageAttachment': {
                    'pollRenderer': {
                        'choices': [
                            {
                                'text': {
                                    'runs': [
                                        {
                                            'text': 'I think History '
                                                    'Matters leans left wing'
                                        }
                                    ]
                                }
                            },
                            {
                                'text': {
                                    'runs': [
                                        {
                                            'text': 'I think History '
                                                    'Matters leans right wing'
                                        }
                                    ]
                                }
                            },
                            {
                                'text': {
                                    'runs': [
                                        {'text': "I can't tell"}
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

SAMPLE_VIDEO_POST: dict = {
    'backstagePostThreadRenderer': {
        'post': {
            'backstagePostRenderer': {
                'postId': 'Ugw32X53cT_OWzKCwQp4AaABCQ',
                'authorText': {
                    'runs': [{'text': 'History Matters'}]
                },
                'contentText': {
                    'runs': [
                        {'text': 'New video is up!'}
                    ]
                },
                'publishedTimeText': {
                    'runs': [{'text': '7 years ago'}]
                },
                'voteCount': {
                    'simpleText': '2.3K'
                },
                'backstageAttachment': {
                    'videoRenderer': {
                        'videoId': 'LpAJTURalIM',
                        'title': {
                            'runs': [
                                {
                                    'text': 'When a Bf-109 spared '
                                            'a stricken B-17'
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
}

SAMPLE_TEXT_ONLY_POST: dict = {
    'backstagePostThreadRenderer': {
        'post': {
            'backstagePostRenderer': {
                'postId': 'Ugy8hynnv2-IU8dhppd4AaABCQ',
                'authorText': {
                    'runs': [{'text': 'History Matters'}]
                },
                'contentText': {
                    'runs': [
                        {'text': 'Just some text with no attachment.'}
                    ]
                },
                'publishedTimeText': {
                    'runs': [{'text': '4 years ago'}]
                },
                'voteCount': {
                    'simpleText': '1.5K'
                },
                'backstageAttachment': {}
            }
        }
    }
}


class TestYouTubePost(unittest.TestCase):

    def test_from_innertube_image_post(self) -> None:
        post: YouTubePost | None = YouTubePost.from_innertube(
            SAMPLE_IMAGE_POST, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(post)
        self.assertEqual(post.post_id, 'UgwaY28p7Vn8bzlQ_5p4AaABCQ')
        self.assertIn("just thought I'd let you know", post.content_text)
        self.assertEqual(post.published_time_text, '5 years ago')
        self.assertEqual(post.vote_count, '10K')
        self.assertEqual(post.attachment_type, 'image')
        self.assertEqual(len(post.image_urls), 1)
        self.assertIn('large.jpg', post.image_urls[0])
        self.assertIsNone(post.video_id)
        self.assertEqual(post.poll_choices, [])
        self.assertEqual(
            post.channel_id, 'UC22BdTgxefuvUivrjesETjg'
        )
        self.assertEqual(
            post.url,
            'https://www.youtube.com/post/UgwaY28p7Vn8bzlQ_5p4AaABCQ'
        )

    def test_from_innertube_poll_post(self) -> None:
        post: YouTubePost | None = YouTubePost.from_innertube(
            SAMPLE_POLL_POST, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(post)
        self.assertEqual(post.post_id, 'UgzgLdJRC7hfK07YOLx4AaABCQ')
        self.assertEqual(post.attachment_type, 'poll')
        self.assertEqual(len(post.poll_choices), 3)
        self.assertIn('left wing', post.poll_choices[0])
        self.assertIn('right wing', post.poll_choices[1])
        self.assertEqual(post.vote_count, '6K')
        self.assertEqual(post.image_urls, [])
        self.assertIsNone(post.video_id)

    def test_from_innertube_video_post(self) -> None:
        post: YouTubePost | None = YouTubePost.from_innertube(
            SAMPLE_VIDEO_POST, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(post)
        self.assertEqual(post.post_id, 'Ugw32X53cT_OWzKCwQp4AaABCQ')
        self.assertEqual(post.attachment_type, 'video')
        self.assertEqual(post.video_id, 'LpAJTURalIM')
        self.assertEqual(post.content_text, 'New video is up!')
        self.assertEqual(post.vote_count, '2.3K')
        self.assertEqual(post.image_urls, [])
        self.assertEqual(post.poll_choices, [])

    def test_from_innertube_text_only_post(self) -> None:
        post: YouTubePost | None = YouTubePost.from_innertube(
            SAMPLE_TEXT_ONLY_POST, channel_id='UC22BdTgxefuvUivrjesETjg'
        )
        self.assertIsNotNone(post)
        self.assertEqual(post.post_id, 'Ugy8hynnv2-IU8dhppd4AaABCQ')
        self.assertIsNone(post.attachment_type)
        self.assertIsNone(post.video_id)
        self.assertEqual(post.image_urls, [])
        self.assertEqual(post.poll_choices, [])
        self.assertEqual(
            post.content_text,
            'Just some text with no attachment.'
        )

    def test_from_innertube_empty(self) -> None:
        self.assertIsNone(YouTubePost.from_innertube({}))

    def test_to_dict_from_dict_round_trip(self) -> None:
        post = YouTubePost(
            post_id='Ugtest123',
            content_text='Hello world',
            published_time_text='1 year ago',
            vote_count='500',
            attachment_type='image',
            image_urls=['https://example.com/img.jpg'],
            channel_id='UCtest',
        )
        data: dict = post.to_dict()
        restored: YouTubePost = YouTubePost.from_dict(data)
        self.assertEqual(post, restored)

    def test_to_dict_fields(self) -> None:
        post = YouTubePost(
            post_id='Ugtest',
            content_text='Test post',
            published_time_text='2 days ago',
            vote_count='100',
            attachment_type='video',
            video_id='abc123',
            channel_id='UCxyz',
        )
        d: dict[str, any] = post.to_dict()
        self.assertEqual(d['post_id'], 'Ugtest')
        self.assertEqual(d['content_text'], 'Test post')
        self.assertEqual(d['published_time_text'], '2 days ago')
        self.assertEqual(d['vote_count'], '100')
        self.assertEqual(d['attachment_type'], 'video')
        self.assertEqual(d['video_id'], 'abc123')
        self.assertEqual(d['channel_id'], 'UCxyz')
        self.assertEqual(
            d['url'],
            'https://www.youtube.com/post/Ugtest'
        )
        self.assertIn('created_timestamp', d)

    def test_hash_and_equality(self) -> None:
        a = YouTubePost(post_id='Ug1', content_text='A', vote_count='1')
        b = YouTubePost(post_id='Ug1', content_text='A', vote_count='1')
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

        s: set = {a, b}
        self.assertEqual(len(s), 1)

    def test_repr(self) -> None:
        p = YouTubePost(
            post_id='Ugtest', attachment_type='image', vote_count='5K'
        )
        self.assertIn('Ugtest', repr(p))
        self.assertIn('image', repr(p))
        self.assertIn('5K', repr(p))

    def test_url_none_when_no_id(self) -> None:
        p = YouTubePost()
        self.assertIsNone(p.url)

    def test_from_innertube_multi_run_content(self) -> None:
        '''Content text with multiple runs should be concatenated.'''
        item: dict = {
            'backstagePostThreadRenderer': {
                'post': {
                    'backstagePostRenderer': {
                        'postId': 'UgMulti',
                        'contentText': {
                            'runs': [
                                {'text': 'Check out '},
                                {'text': 'this link'},
                                {'text': ' for more!'}
                            ]
                        },
                        'publishedTimeText': {'runs': [{'text': '1 day ago'}]},
                        'voteCount': {'simpleText': '42'},
                    }
                }
            }
        }
        post = YouTubePost.from_innertube(item)
        self.assertEqual(
            post.content_text, 'Check out this link for more!'
        )


if __name__ == '__main__':
    unittest.main()
