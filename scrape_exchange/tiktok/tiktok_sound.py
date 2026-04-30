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
