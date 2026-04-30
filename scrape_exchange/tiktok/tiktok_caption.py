'''
Caption descriptor — same shape as the YouTube
``$defs/caption`` in the YouTube schema.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import BaseModel, ConfigDict


class Caption(BaseModel):
    model_config = ConfigDict(extra='forbid')

    language_code: str | None = None
    url: str | None = None
    extension: str | None = None
    protocol: str | None = None
