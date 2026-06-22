'''
TikTok avatar image reference.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import BaseModel, ConfigDict


class AvatarRef(BaseModel):
    '''A named TikTok avatar URL variant, e.g. thumb/medium/large.'''

    model_config = ConfigDict(extra='forbid')

    name: str
    url: str


def avatar_refs_from_payload(payload: dict) -> list[AvatarRef]:
    refs: list[AvatarRef] = []
    for source, name in (
        ('avatarThumb', 'thumb'),
        ('avatarMedium', 'medium'),
        ('avatarLarger', 'large'),
    ):
        value: object = payload.get(source)
        if isinstance(value, str) and value:
            refs.append(AvatarRef(name=name, url=value))
    return refs
