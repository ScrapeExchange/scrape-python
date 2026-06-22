'''
TikTok short-URL recognition and resolution.

``vm.tiktok.com`` / ``vt.tiktok.com`` links are HTTP redirects to a
canonical ``/@handle[/video/<id>]`` URL. This module recognises and
canonicalises them (pure helpers, used at queue admission) and resolves
them to a creator handle over HTTP through a proxy (used by the creator
scraper at scrape time).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
from dataclasses import dataclass
from enum import Enum

import httpx

_SHORT_URL_RE: re.Pattern[str] = re.compile(
    r'^https?://(?:www\.)?(vm|vt)\.tiktok\.com/'
    r'([A-Za-z0-9]+)/?(?:\?.*)?$',
)

# Permissive handle extractor: matches @handle whether the path ends
# there, continues to /video/<id>, or carries a query string. The
# existing _TIKTOK_CREATOR_URL_RE in queue_admin only matches
# profile-terminal URLs, so this is deliberately broader.
_RESOLVED_HANDLE_RE: re.Pattern[str] = re.compile(
    r'^https?://(?:www\.)?tiktok\.com/@([a-zA-Z0-9_.-]+)'
    r'(?:[/?].*)?$',
)


def is_tiktok_short_url(value: str) -> bool:
    '''True when *value* is a vm/vt TikTok short link.'''
    return _SHORT_URL_RE.fullmatch(value.strip()) is not None


def normalize_tiktok_short_url(value: str) -> str | None:
    '''Canonicalise a short link to ``https://<vm|vt>.tiktok.com/<code>``
    (query string and trailing slash stripped), or ``None``.'''
    match: re.Match[str] | None = _SHORT_URL_RE.fullmatch(value.strip())
    if match is None:
        return None
    subdomain: str = match.group(1)
    code: str = match.group(2)
    return f'https://{subdomain}.tiktok.com/{code}'


def extract_handle_from_resolved_url(url: str) -> str | None:
    '''Extract the bare creator handle from a resolved TikTok URL
    (profile or video), or ``None`` for a non-TikTok URL.'''
    match: re.Match[str] | None = _RESOLVED_HANDLE_RE.fullmatch(
        url.strip(),
    )
    return match.group(1) if match is not None else None


_OG_URL_RE: re.Pattern[str] = re.compile(
    r'<(?:meta[^>]+(?:property|name)="og:url"[^>]+content|'
    r'link[^>]+rel="canonical"[^>]+href)="([^"]+)"',
    re.IGNORECASE,
)

# Statuses that mean "try again later" rather than "gone for good". A
# bare 403/429 from an anti-bot edge must never permanently delete a
# recoverable link.
_TRANSIENT_STATUSES: frozenset[int] = frozenset({403, 408, 425, 429})


class ShortUrlOutcome(str, Enum):
    RESOLVED = 'resolved'        # handle extracted
    TRANSIENT = 'transient'      # recoverable; retry later
    UNAVAILABLE = 'unavailable'  # terminal; discard the alias


@dataclass
class ShortUrlResolution:
    outcome: 'ShortUrlOutcome'
    handle: str | None = None  # set iff outcome is RESOLVED


def _canonical_url_from_html(body: str) -> str | None:
    '''Pull an og:url / canonical href out of an HTML interstitial.'''
    match: re.Match[str] | None = _OG_URL_RE.search(body)
    return match.group(1) if match is not None else None


def _classify_resolution(
    status_code: int,
    final_url: str | None,
    body: str = '',
) -> ShortUrlResolution:
    '''Map an HTTP status + final URL (+ optional body) to an outcome.'''
    if status_code in _TRANSIENT_STATUSES or status_code >= 500:
        return ShortUrlResolution(ShortUrlOutcome.TRANSIENT)
    if status_code >= 400:
        return ShortUrlResolution(ShortUrlOutcome.UNAVAILABLE)
    handle: str | None = (
        extract_handle_from_resolved_url(final_url)
        if final_url else None
    )
    if handle is None and body:
        canonical: str | None = _canonical_url_from_html(body)
        if canonical is not None:
            handle = extract_handle_from_resolved_url(canonical)
    if handle:
        return ShortUrlResolution(ShortUrlOutcome.RESOLVED, handle)
    return ShortUrlResolution(ShortUrlOutcome.UNAVAILABLE)


async def resolve_creator_short_url(
    short_url: str,
    *,
    proxy: str,
    timeout: float,
    user_agent: str,
) -> ShortUrlResolution:
    '''Resolve *short_url* to a creator handle through *proxy*.

    Follows redirects and classifies the result (see
    ``_classify_resolution``). Transport/timeout errors map to
    ``TRANSIENT``; this function never raises.
    '''
    headers: dict[str, str] = {'User-Agent': user_agent}
    try:
        async with httpx.AsyncClient(
            proxies=proxy,
            follow_redirects=True,
            timeout=timeout,
            headers=headers,
        ) as client:
            resp: httpx.Response = await client.get(short_url)
    except httpx.HTTPError:
        return ShortUrlResolution(ShortUrlOutcome.TRANSIENT)
    body: str = resp.text if resp.status_code < 400 else ''
    return _classify_resolution(resp.status_code, str(resp.url), body)
