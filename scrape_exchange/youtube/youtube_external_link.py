'''
Model an external link of a Youtube video

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''


from typing import Self


class YouTubeExternalLink:
    def __init__(self, name: str, url: str, priority: int) -> None:
        self.name: str = name
        self.url: str = url
        self.priority: int = priority

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'url': self.url,
            'priority': self.priority,
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> Self:
        return YouTubeExternalLink(
            name=data['name'],
            url=data['url'],
            priority=data['priority']
        )

    def __hash__(self) -> int:
        return hash(self.url)

    def __eq__(self, other) -> bool:
        return self.url == other.url
