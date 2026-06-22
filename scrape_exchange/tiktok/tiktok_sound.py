'''
Sound (music) descriptor, denormalised onto each video record.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import BaseModel, ConfigDict


class Sound(BaseModel):
    model_config = ConfigDict(extra='forbid')

    id: str
    title: str | None = None
    author_name: str | None = None
    original: bool | None = None
    duration: int | None = None
    play_url: str | None = None
    cover_large_url: str | None = None
    cover_medium_url: str | None = None
    cover_thumb_url: str | None = None
    copyrighted: bool | None = None
    commerce_music: bool | None = None
    unlimited_music: bool | None = None
    private: bool | None = None
    shoot_duration: int | None = None
