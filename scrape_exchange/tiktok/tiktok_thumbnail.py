'''
Thumbnail descriptor — same shape as the YouTube
``$defs/thumbnail`` in the YouTube schema.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import BaseModel, ConfigDict


class Thumbnail(BaseModel):
    model_config = ConfigDict(extra='forbid')

    url: str
    id: str | None = None
    width: int | None = None
    height: int | None = None
    preference: str | None = None
    display_hint: str | None = None
