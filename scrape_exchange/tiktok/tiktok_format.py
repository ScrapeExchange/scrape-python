'''
TikTokFormat model — one pre-encoded rendition (gear) of a
TikTok video, parsed from a ``video.bitrateInfo[]`` entry of
the item payload. TikTok serves VOD as progressive MP4 over
HTTPS; the player adapts by switching between these
whole-file renditions, so this is the TikTok analogue of the
YouTube ``$defs/format`` / ``YouTubeFormat``.

``MVMAF`` (per-rendition VMAF score blob) and ``FileCs``
(chunk checksums) are intentionally not promoted; they remain
available in ``TikTokVideo.raw_item``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class TikTokFormat(BaseModel):
    model_config = ConfigDict(extra='forbid')

    gear_name: str
    bitrate: int | None = None
    quality_type: int | None = None
    codec: str | None = None
    width: int | None = None
    height: int | None = None
    data_size: int | None = None
    uri: str | None = None
    url_key: str | None = None
    urls: list[str] = Field(default_factory=list)
    file_hash: str | None = None

    @classmethod
    def from_api(cls, entry: object) -> Self | None:
        '''
        Parse one ``video.bitrateInfo[]`` entry. Returns None
        for entries that are not dicts or lack a ``GearName``
        (the rendition key).
        '''

        if not isinstance(entry, dict):
            return None
        gear_name: object = entry.get('GearName')
        if not isinstance(gear_name, str) or not gear_name:
            return None

        play_addr: object = entry.get('PlayAddr')
        if not isinstance(play_addr, dict):
            play_addr = {}

        url_list: object = play_addr.get('UrlList')
        if not isinstance(url_list, list):
            url_list = []
        urls: list[str] = [
            url for url in url_list
            if isinstance(url, str) and url
        ]

        return cls(
            gear_name=gear_name,
            bitrate=_to_optional_int(entry.get('Bitrate')),
            quality_type=_to_optional_int(
                entry.get('QualityType'),
            ),
            codec=_to_optional_str(entry.get('CodecType')),
            width=_to_optional_int(play_addr.get('Width')),
            height=_to_optional_int(play_addr.get('Height')),
            data_size=_to_optional_int(
                play_addr.get('DataSize'),
            ),
            uri=_to_optional_str(play_addr.get('Uri')),
            url_key=_to_optional_str(play_addr.get('UrlKey')),
            urls=urls,
            file_hash=_to_optional_str(
                play_addr.get('FileHash'),
            ),
        )


def formats_from_payload(
    video_block: dict,
) -> dict[str, TikTokFormat]:
    '''
    Build the ``formats`` dict (keyed by gear name) from the
    ``video`` block of an item payload. Tolerates a missing or
    malformed ``bitrateInfo``.
    '''

    bitrate_info: object = video_block.get('bitrateInfo')
    if not isinstance(bitrate_info, list):
        return {}

    formats: dict[str, TikTokFormat] = {}
    for entry in bitrate_info:
        fmt: TikTokFormat | None = TikTokFormat.from_api(entry)
        if fmt is not None:
            formats[fmt.gear_name] = fmt
    return formats


def _to_optional_str(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value


def _to_optional_int(value: object) -> int | None:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
