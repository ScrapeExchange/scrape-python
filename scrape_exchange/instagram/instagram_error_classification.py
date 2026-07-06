'''
Classify Instagram scraper exceptions into metric/queue reasons.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from scrape_exchange.instagram.instagram_creator import (
    InstagramProfileUnavailableError,
    InstagramRateLimitError,
    UnknownFollowersError,
)


def _evidence_strings(exc: BaseException) -> list[str]:
    evidence: object = getattr(exc, 'evidence', None)
    if not isinstance(evidence, dict):
        return []
    values: list[str] = []
    for key in ('last_url', 'page_title', 'marker_summary'):
        value: object = evidence.get(key)
        if value is not None:
            values.append(str(value).lower())
    http_status: object = evidence.get('http_status')
    if http_status is not None:
        values.append(str(http_status).lower())
    detected_markers: object = evidence.get('detected_markers')
    if isinstance(detected_markers, list | tuple | set):
        values.extend(str(marker).lower() for marker in detected_markers)
    return values


def _has_rate_limit_marker(values: list[str]) -> bool:
    return any(
        marker in value
        for value in values
        for marker in (
            '/accounts/login/',
            'login',
            'challenge',
            'checkpoint',
            'http_block',
            '429',
            '403',
            'please wait a few minutes',
            'rate limit',
            'blocked',
        )
    )


def classify_instagram_error(exc: BaseException) -> str:
    '''Return one of the fixed scraper failure reasons.'''
    if isinstance(exc, InstagramProfileUnavailableError):
        return 'unavailable'
    if isinstance(exc, InstagramRateLimitError):
        return 'rate_limit'
    if isinstance(exc, UnknownFollowersError):
        return 'unknown_followers'
    if _has_rate_limit_marker(_evidence_strings(exc)):
        return 'rate_limit'
    text: str = str(exc).lower()
    if _has_rate_limit_marker([text]):
        return 'rate_limit'
    if any(
        marker in text
        for marker in (
            'timeout',
            'navigation',
            'browser has been closed',
            'target closed',
            'connection',
        )
    ):
        return 'transient'
    if 'not found' in text or 'page is not available' in text:
        return 'unavailable'
    return 'other'
