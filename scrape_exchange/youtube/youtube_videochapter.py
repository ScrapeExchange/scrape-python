'''
Module for representing YouTube video chapters, which are segments of a video

:author: Boinko <boinko@scrape.exchange
:copyright: 2026 Boinko
:license: GPL-3.0
'''

from typing import Self


class YouTubeVideoChapter:
    def __init__(self, chapter_info: dict[str, float | str]) -> None:
        self.start_time: float = chapter_info.get('start_time')
        self.end_time: float = chapter_info.get('end_time')
        self.title: str = chapter_info.get('title')
        self.thumb_url: str = chapter_info.get('thumbnail_url')

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeVideoChapter):
            return False

        return (
            self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.title == other.title
        )

    def to_dict(self) -> dict[str, str | float | None]:
        '''
        Returns a dict representation of the chapter that
        round-trips through :meth:`from_dict`. ``thumb_url`` is
        only emitted when set so files without chapter
        thumbnails stay compact.
        '''

        data: dict[str, str | float | None] = {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'title': self.title,
        }
        if self.thumb_url:
            data['thumb_url'] = self.thumb_url
        return data

    @staticmethod
    def from_dict(data: dict[str, str | int | float]) -> Self:
        '''
        Factory for YouTubeVideoChapter. Accepts both the
        on-disk shape produced by :meth:`to_dict` (``thumb_url``)
        and the raw yt-dlp shape (``thumbnail_url``).
        '''

        thumbnail_url: str | None = (
            data.get('thumb_url') or data.get('thumbnail_url')
        )
        return YouTubeVideoChapter(
            {
                'start_time': data.get('start_time'),
                'end_time': data.get('end_time'),
                'title': data.get('title'),
                'thumbnail_url': thumbnail_url,
            }
        )
