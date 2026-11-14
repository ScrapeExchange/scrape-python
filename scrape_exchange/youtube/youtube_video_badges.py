'''Preserve video disclosures from YouTube watch-page metadata.'''

from typing import Any

from yt_dlp.extractor.youtube import YoutubeIE


def extract_video_badges(data: Any) -> list[dict[str, str]]:
    '''Read only the main video's badges, excluding owners and suggestions.'''
    contents: Any = data
    key: str
    for key in (
        'contents', 'twoColumnWatchNextResults', 'results', 'results',
        'contents',
    ):
        contents = contents.get(key) if isinstance(contents, dict) else None
    badges: list[dict[str, str]] = []
    item: Any
    for item in contents if isinstance(contents, list) else []:
        primary: Any = (
            item.get('videoPrimaryInfoRenderer')
            if isinstance(item, dict) else None
        )
        raw_badges: Any = (
            primary.get('badges') if isinstance(primary, dict) else None
        )
        raw: Any
        for raw in raw_badges if isinstance(raw_badges, list) else []:
            renderer: Any = (
                raw.get('metadataBadgeRenderer')
                if isinstance(raw, dict) else None
            )
            if not isinstance(renderer, dict):
                continue
            badge: dict[str, str] = {}
            field: str
            path: tuple[str, ...]
            for field, path in (
                ('label', ('label',)),
                ('style', ('style',)),
                ('icon', ('icon', 'iconType')),
                ('tooltip', ('tooltip',)),
                ('accessibility_label', ('accessibilityData', 'label')),
            ):
                value: Any = renderer
                for key in path:
                    value = value.get(key) if isinstance(value, dict) else None
                if isinstance(value, str) and value.strip():
                    badge[field] = value.strip()
            if badge and badge not in badges:
                badges.append(badge)
    return badges


def extract_content_disclosure(data: Any) -> dict[str, str] | None:
    '''Read the "How this was made" disclosure of the main video.

    YouTube lists creator disclosures (altered or synthetic content,
    auto-dubbing, ...) in the description panel. Returns the first
    disclosure as {'title': <kind>, 'body': <explanation>}, or None.
    '''
    panels: Any = data.get('engagementPanels') if isinstance(
        data, dict
    ) else None
    panel: Any
    for panel in panels if isinstance(panels, list) else []:
        content: Any = panel
        key: str
        for key in (
            'engagementPanelSectionListRenderer', 'content',
            'structuredDescriptionContentRenderer', 'items',
        ):
            content = content.get(key) if isinstance(content, dict) else None
        item: Any
        for item in content if isinstance(content, list) else []:
            section: Any = (
                item.get('howThisWasMadeSectionViewModel')
                if isinstance(item, dict) else None
            )
            if not isinstance(section, dict):
                continue
            disclosure: dict[str, str] = {}
            field: str
            source: str
            for field, source in (
                ('title', 'bodyHeader'), ('body', 'bodyText'),
            ):
                text: Any = section.get(source)
                value: Any = (
                    text.get('content') if isinstance(text, dict) else None
                )
                if isinstance(value, str) and value.strip():
                    disclosure[field] = value.strip()
            if disclosure:
                return disclosure
    return None


class BadgeYoutubeIE(YoutubeIE):
    '''Extend yt-dlp's existing request flow without another page fetch.

    yt-dlp discards unfamiliar badges such as AI. Capture its initial
    watch data before that filtering and attach the badges to its result.
    These two upstream hooks are covered by the adapter tests; check them
    when upgrading yt-dlp.
    '''

    @classmethod
    def ie_key(cls) -> str:
        # Replace the standard extractor under the same dispatch key.
        return YoutubeIE.ie_key()

    def _download_initial_data(
        self, video_id: str, webpage: str | None, webpage_client: str,
        webpage_ytcfg: dict[str, Any],
    ) -> dict[str, Any] | None:
        data: dict[str, Any] | None = super()._download_initial_data(
            video_id, webpage, webpage_client, webpage_ytcfg,
        )
        self._video_badges = extract_video_badges(data)
        self._content_disclosure = extract_content_disclosure(data)
        return data

    def _real_extract(self, url: str) -> dict[str, Any]:
        # Extractors are reused: never carry a previous video's disclosure.
        self._video_badges: list[dict[str, str]] = []
        self._content_disclosure: dict[str, str] | None = None
        result: dict[str, Any] = super()._real_extract(url)
        if result.get('_type', 'video') == 'video':
            result['badges'] = self._video_badges
            result['content_disclosure'] = self._content_disclosure
        return result
