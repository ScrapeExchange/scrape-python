'''
Model a Youtube video/audio/storyboard etc. format

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Self


class YouTubeFragment:
    '''
    Models a fragment of a YouTube video or audio track
    '''

    def __init__(self) -> None:
        self.url: str | None = None
        self.duration: float | None = None
        self.path: str | None = None

    def to_dict(self) -> dict[str, str | float]:
        '''
        Returns a dict representation of the fragment
        '''

        return {
            'url': self.url,
            'duration': self.duration,
            'path': self.path
        }

    @staticmethod
    def from_dict(data: dict[str, str | float]) -> Self:
        '''
        Factory for YouTubeFragment, parses data are provided
        by yt-dlp
        '''

        fragment = YouTubeFragment()
        fragment.url = data.get('url')
        fragment.path = data.get('path')
        fragment.duration = data.get('duration')
        return fragment


class YouTubeFormat:
    '''
    Models a track (audio, video, or storyboard of YouTube video
    '''

    def __init__(self) -> None:
        self.format_id: str | None = None
        self.format_note: str | None = None
        self.ext: str | None = None
        self.audio_ext: str | None = None
        self.video_ext: str | None = None
        self.protocol: str | None = None
        self.audio_codec: str | None = None
        self.video_codec: str | None = None
        self.container: str | None = None
        self.url: str | None = None
        self.width: int | None = None
        self.height: int | None = None
        self.fps: float | None = None
        self.quality: float | None = None
        self.dynamic_range: str | None = None
        self.has_drm: bool | None = None
        self.tbr: float | None = None
        self.abr: float | None = None
        self.asr: int | None = None
        self.audio_channels: int | None = None
        self.rows: int | None = None
        self.cols: int | None = None
        self.resolution: str | None = None
        self.aspect_ratio: str | None = None
        self.format: str | None = None
        self.fragments: list[YouTubeFragment] = []

    def __str__(self) -> str:
        return (
            f'YouTubeFormat('
            f'{self.format_id}, {self.format_note}, {self.ext}, '
            f'{self.protocol}, {self.audio_codec}, {self.video_codec}, '
            f'{self.container}, {self.width}, {self.height}, {self.fps}, '
            f'{self.resolution}, '
            f'{self.audio_ext}, {self.video_ext}'
            ')'
        )

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeFormat):
            return False

        return self.to_dict() == other.to_dict()

    def to_dict(self) -> dict[str, any]:
        '''
        Returns a dict representation of the video
        '''

        data: dict[str, any] = {
            'format_id': self.format_id,
            'format_note': self.format_note,
            'ext': self.ext,
            'audio_ext': self.audio_ext,
            'video_ext': self.video_ext,
            'protocol': self.protocol,
            'audio_codec': self.audio_codec,
            'video_codec': self.video_codec,
            'container': self.container,
            'url': self.url,
            'width': self.width,
            'height': self.height,
            'fps': self.fps,
            'quality': self.quality,
            'dynamic_range': self.dynamic_range,
            'has_drm': self.has_drm,
            'tbr': self.tbr,
            'abr': self.abr,
            'asr': self.asr,
            'audio_channels': self.audio_channels,
            'rows': self.rows,
            'cols': self.cols,
            'resolution': self.resolution,
            'aspect_ratio': self.aspect_ratio,
            'format': self.format,
            'fragments': [fragment.to_dict() for fragment in self.fragments],
        }

        return data

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory using data retrieved using the 'yt-dlp' tool
        '''

        yt_format = YouTubeFormat()
        yt_format.format_id = data['format_id']
        yt_format.format_note = data.get('format_note')
        yt_format.ext = data.get('ext')
        yt_format.audio_ext = data.get('audio_ext')
        yt_format.video_ext = data.get('video_ext')
        yt_format.protocol = data.get('protocol')
        yt_format.audio_codec = data.get('audio_codec', data.get('audiocodec'))
        yt_format.video_codec = data.get('video_codec', data.get('videocodec'))
        yt_format.container = data.get('container')
        yt_format.url = data.get('url')
        yt_format.width = data.get('width')
        yt_format.height = data.get('height')
        yt_format.fps = data.get('fps')
        yt_format.quality = data.get('quality')
        yt_format.dynamic_range = data.get('dynamic_range')
        yt_format.has_drm = data.get('has_drm')
        yt_format.tbr = data.get('tbr')
        yt_format.asr = data.get('asr')
        yt_format.abr = data.get('abr')
        yt_format.audio_channels = data.get('audio_channels')
        yt_format.rows = data.get('rows')
        yt_format.cols = data.get('cols')
        yt_format.resolution = data.get('resolution')
        yt_format.aspect_ratio = data.get('aspect_ratio')
        yt_format.format = data.get('format')

        for fragment_data in data.get('fragments', []):
            fragment: YouTubeFragment = YouTubeFragment.from_dict(
                fragment_data
            )
            yt_format.fragments.append(fragment)

        return yt_format
