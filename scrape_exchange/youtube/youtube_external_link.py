'''
Model an external link of a Youtube video

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''


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

    def __hash__(self) -> int:
        return hash(self.url)

    def __eq__(self, other) -> bool:
        return self.url == other.url
