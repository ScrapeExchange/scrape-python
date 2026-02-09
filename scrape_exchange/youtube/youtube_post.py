'''
Model a YouTube community post as scraped from a channel's posts tab

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Self
from logging import Logger
from logging import getLogger
from datetime import UTC, datetime

_LOGGER: Logger = getLogger(__name__)


class YouTubePost:
    '''
    Represents a YouTube community post as returned by the InnerTube
    browse API on a channel's "Posts" (community) tab.

    The data comes from a ``backstagePostRenderer`` inside a
    ``backstagePostThreadRenderer``.
    '''

    POST_URL: str = 'https://www.youtube.com/post/{post_id}'

    def __init__(self,
                 post_id: str | None = None,
                 content_text: str | None = None,
                 published_time_text: str | None = None,
                 vote_count: str | None = None,
                 attachment_type: str | None = None,
                 video_id: str | None = None,
                 image_urls: list[str] | None = None,
                 poll_choices: list[str] | None = None,
                 channel_id: str | None = None) -> None:

        self.post_id: str | None = post_id
        self.content_text: str | None = content_text
        self.published_time_text: str | None = published_time_text
        self.vote_count: str | None = vote_count
        self.attachment_type: str | None = attachment_type
        self.video_id: str | None = video_id
        self.image_urls: list[str] = image_urls or []
        self.poll_choices: list[str] = poll_choices or []
        self.channel_id: str | None = channel_id
        self.created_timestamp: datetime = datetime.now(tz=UTC)

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubePost):
            return False

        return (
            self.post_id == other.post_id
            and self.content_text == other.content_text
            and self.published_time_text == other.published_time_text
            and self.vote_count == other.vote_count
            and self.attachment_type == other.attachment_type
            and self.video_id == other.video_id
            and self.image_urls == other.image_urls
            and self.poll_choices == other.poll_choices
            and self.channel_id == other.channel_id
        )

    def __hash__(self) -> int:
        return hash(self.post_id)

    def __repr__(self) -> str:
        return (
            f'YouTubePost(id={self.post_id}, '
            f'attachment={self.attachment_type}, '
            f'votes={self.vote_count})'
        )

    @property
    def url(self) -> str | None:
        if self.post_id:
            return self.POST_URL.format(post_id=self.post_id)
        return None

    def to_dict(self) -> dict[str, any]:
        '''
        Returns a dict representation of the post.
        '''

        return {
            'post_id': self.post_id,
            'content_text': self.content_text,
            'published_time_text': self.published_time_text,
            'vote_count': self.vote_count,
            'attachment_type': self.attachment_type,
            'video_id': self.video_id,
            'image_urls': self.image_urls,
            'poll_choices': self.poll_choices,
            'channel_id': self.channel_id,
            'url': self.url,
            'created_timestamp': self.created_timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory method that creates a YouTubePost from a dict
        previously produced by ``to_dict``.
        '''

        post = YouTubePost(
            post_id=data.get('post_id'),
            content_text=data.get('content_text'),
            published_time_text=data.get('published_time_text'),
            vote_count=data.get('vote_count'),
            attachment_type=data.get('attachment_type'),
            video_id=data.get('video_id'),
            image_urls=data.get('image_urls', []),
            poll_choices=data.get('poll_choices', []),
            channel_id=data.get('channel_id'),
        )

        ts: str | None = data.get('created_timestamp')
        if ts:
            post.created_timestamp = datetime.fromisoformat(ts)

        return post

    @staticmethod
    def from_innertube(item: dict[str, any],
                       channel_id: str | None = None) -> Self | None:
        '''
        Factory method that creates a YouTubePost from the raw
        ``backstagePostThreadRenderer`` dict returned by the InnerTube
        browse API.

        :param item: a single item from the itemSectionRenderer contents
        :param channel_id: optional channel ID to associate
        :returns: a YouTubePost or None if the item cannot be parsed
        '''

        bpr: dict = item.get(
            'backstagePostThreadRenderer', {}
        ).get(
            'post', {}
        ).get('backstagePostRenderer', {})

        if not bpr:
            return None

        post_id: str | None = bpr.get('postId')
        if not post_id:
            return None

        # Content text: join all runs
        content_text: str | None = None
        runs: list = bpr.get('contentText', {}).get('runs', [])
        if runs:
            content_text = ''.join(r.get('text', '') for r in runs)

        # Published time text
        published_time_text: str | None = None
        time_runs: list = bpr.get(
            'publishedTimeText', {}
        ).get('runs', [])
        if time_runs:
            published_time_text = time_runs[0].get('text')

        # Vote count
        vote_count: str | None = bpr.get(
            'voteCount', {}
        ).get('simpleText')

        # Attachment
        attachment: dict = bpr.get('backstageAttachment', {})
        attachment_type: str | None = None
        video_id: str | None = None
        image_urls: list[str] = []
        poll_choices: list[str] = []

        if 'backstageImageRenderer' in attachment:
            attachment_type = 'image'
            thumbs: list = attachment.get(
                'backstageImageRenderer', {}
            ).get(
                'image', {}
            ).get('thumbnails', [])
            if thumbs:
                # Take the highest resolution thumbnail
                image_urls = [thumbs[-1].get('url', '')]

        elif 'postMultiImageRenderer' in attachment:
            attachment_type = 'image'
            images: list = attachment.get(
                'postMultiImageRenderer', {}
            ).get('images', [])
            for img in images:
                thumbs = img.get(
                    'backstageImageRenderer', {}
                ).get(
                    'image', {}
                ).get('thumbnails', [])
                if thumbs:
                    image_urls.append(thumbs[-1].get('url', ''))

        elif 'videoRenderer' in attachment:
            attachment_type = 'video'
            video_id = attachment.get(
                'videoRenderer', {}
            ).get('videoId')

        elif 'pollRenderer' in attachment:
            attachment_type = 'poll'
            choices: list = attachment.get(
                'pollRenderer', {}
            ).get('choices', [])
            for choice in choices:
                choice_runs: list = choice.get(
                    'text', {}
                ).get('runs', [])
                if choice_runs:
                    poll_choices.append(
                        ''.join(r.get('text', '') for r in choice_runs)
                    )

        return YouTubePost(
            post_id=post_id,
            content_text=content_text,
            published_time_text=published_time_text,
            vote_count=vote_count,
            attachment_type=attachment_type,
            video_id=video_id,
            image_urls=image_urls,
            poll_choices=poll_choices,
            channel_id=channel_id,
        )
