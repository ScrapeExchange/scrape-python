'''
Module for representing YouTube captions, which are subtitle tracks for a video

:author: Boinko <boinko@scrape.exchange
:copyright: 2026 Boinko
:license: GPL-3.0
'''

from typing import Self


class YouTubeCaption:
    def __init__(self, language_code: str, caption_info: dict[str, str]
                 ) -> None:
        '''
        Describes a caption / subtitle track for a YouTube video

        :param language_code: BCP-47 language code for the caption
        :param is_auto_generated: whether the caption track is auto-generated
        :param caption_info: information about the caption track
        :returns: (none)
        :raises: (none)
        '''

        self.language_code: str = language_code
        self.url: str = caption_info.get('url')
        self.extension: str = caption_info.get('ext')
        self.protocol: str = caption_info.get('protocol')

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeCaption):
            return False

        return (
            self.language_code == other.language_code
            and self.url == other.url
            and self.extension == other.extension
            and self.protocol == other.protocol
        )

    def to_dict(self) -> dict[str, str]:
        '''
        Returns a dict representation of the caption
        '''

        return {
            'language_code': self.language_code,
            'url': self.url,
            'extension': self.extension,
            'protocol': self.protocol
        }

    @staticmethod
    def from_dict(data: dict[str, str | int | bool]) -> Self:
        '''
        Factory for YouTubeCaption, parses data provided by yt-dlp
        '''

        return YouTubeCaption(
            language_code=data.get('language_code'),
            caption_info={
                'url': data.get('url'),
                'ext': data.get('extension'),
                'protocol': data.get('protocol')
            }
        )
