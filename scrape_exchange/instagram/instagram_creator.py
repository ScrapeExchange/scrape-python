'''
Instagram creator/account profile model and extraction helpers.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import re

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Self

from bs4 import BeautifulSoup
from pydantic import BaseModel, ConfigDict, Field


EXTRACTOR_VERSION: str = 'ig-profile-v2'

_COUNT_RE: re.Pattern[str] = re.compile(
    r'(?P<num>\d+(?:[,.]\d+)?)(?P<suffix>[kKmMbB])?'
)
_META_FOLLOWERS_RE: re.Pattern[str] = re.compile(
    r'(?P<count>[\d,.]+(?:\s*[kKmMbB])?)\s+'
    r'(?:Followers|abonn[eé]s)',
    re.IGNORECASE,
)
_META_FOLLOWING_RE: re.Pattern[str] = re.compile(
    r'(?P<count>[\d,.]+(?:\s*[kKmMbB])?)\s+'
    r'(?:Following|abonnements?|abonnement\(s\))',
    re.IGNORECASE,
)
_META_POSTS_RE: re.Pattern[str] = re.compile(
    r'(?P<count>[\d,.]+(?:\s*[kKmMbB])?)\s+'
    r'(?:Posts|publications)',
    re.IGNORECASE,
)


class InstagramBioLink(BaseModel):
    '''A public link listed on an Instagram profile.'''

    model_config = ConfigDict(extra='forbid')

    title: str | None = None
    url: str | None = None
    lynx_url: str | None = None
    link_type: str | None = None
    image_url: str | None = None
    media_type: str | None = None
    media_accent_color_hex: str | None = None
    creation_source: str | None = None
    is_pinned: bool | None = None


class InstagramCreator(BaseModel):
    '''A scraped Instagram creator/account profile.'''

    model_config = ConfigDict(extra='forbid')

    username: str
    follower_count: int = Field(ge=0)
    verified: bool = False
    private_account: bool = False
    url: str
    scraped_timestamp: datetime
    pk: str | None = None
    user_id: str | None = None
    full_name: str | None = None
    biography: str | None = None
    external_url: str | None = None
    bio_links: list[InstagramBioLink] = Field(default_factory=list)
    avatar_url: str | None = None
    following_count: int | None = Field(default=None, ge=0)
    post_count: int | None = Field(default=None, ge=0)
    category: str | None = None
    business_account: bool | None = None
    professional_account: bool | None = None
    unpublished: bool | None = None
    memorialized: bool | None = None
    coppa_enforced: bool | None = None
    has_any_clips: bool | None = None
    pronouns: list[str] = Field(default_factory=list)
    account_badges: list[dict[str, Any]] = Field(default_factory=list)
    linked_fb_info: dict[str, Any] | None = None
    text_post_app_badge_label: str | None = None
    show_text_post_app_badge: bool | None = None

    @classmethod
    def from_profile_data(
        cls,
        data: 'InstagramProfileData',
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        if data.follower_count is None:
            raise UnknownFollowersError(data.username)
        return cls(
            username=data.username,
            pk=data.pk,
            user_id=data.user_id,
            full_name=data.full_name,
            biography=data.biography,
            external_url=data.external_url,
            bio_links=data.bio_links,
            avatar_url=data.avatar_url,
            follower_count=data.follower_count,
            following_count=data.following_count,
            post_count=data.post_count,
            verified=bool(data.verified),
            private_account=bool(data.private_account),
            category=data.category,
            business_account=data.business_account,
            professional_account=data.professional_account,
            unpublished=data.unpublished,
            memorialized=data.memorialized,
            coppa_enforced=data.coppa_enforced,
            has_any_clips=data.has_any_clips,
            pronouns=data.pronouns,
            account_badges=data.account_badges,
            linked_fb_info=data.linked_fb_info,
            text_post_app_badge_label=data.text_post_app_badge_label,
            show_text_post_app_badge=data.show_text_post_app_badge,
            url=f'https://www.instagram.com/{data.username}/',
            scraped_timestamp=(
                scraped_timestamp or datetime.now(timezone.utc)
            ),
        )

    def to_dict(self) -> dict:
        return self.model_dump(mode='json', exclude_none=True)


class InstagramProfileUnavailableError(RuntimeError):
    '''Instagram clearly reported that the profile does not exist.'''


class InstagramRateLimitError(RuntimeError):
    '''Instagram showed a login wall, challenge, or rate-limit page.'''


class InstagramIdentityError(RuntimeError):
    '''Instagram returned a different profile than requested.'''


class UnknownFollowersError(RuntimeError):
    '''A profile was found, but follower_count was unavailable.'''

    def __init__(self, username: str | None) -> None:
        super().__init__(
            f'instagram profile has unknown followers: {username!r}',
        )


@dataclass
class InstagramProfileData:
    '''Loose profile data extracted from one browser page.'''

    username: str
    pk: str | None = None
    user_id: str | None = None
    full_name: str | None = None
    biography: str | None = None
    external_url: str | None = None
    bio_links: list[InstagramBioLink] = field(default_factory=list)
    avatar_url: str | None = None
    follower_count: int | None = None
    following_count: int | None = None
    post_count: int | None = None
    verified: bool = False
    private_account: bool = False
    category: str | None = None
    business_account: bool | None = None
    professional_account: bool | None = None
    unpublished: bool | None = None
    memorialized: bool | None = None
    coppa_enforced: bool | None = None
    has_any_clips: bool | None = None
    pronouns: list[str] = field(default_factory=list)
    account_badges: list[dict[str, Any]] = field(default_factory=list)
    linked_fb_info: dict[str, Any] | None = None
    text_post_app_badge_label: str | None = None
    show_text_post_app_badge: bool | None = None
    detected_markers: list[str] = field(default_factory=list)


def parse_count(value: object) -> int | None:
    '''Parse Instagram-ish compact counters into integers.'''
    if value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text: str = str(value).strip()
    if not text:
        return None
    text = text.replace('\u00a0', ' ').replace(' ', '')
    match: re.Match[str] | None = _COUNT_RE.fullmatch(text)
    if match is None:
        return None
    suffix: str = (match.group('suffix') or '').lower()
    raw_number: str = match.group('num')
    number: str = (
        raw_number.replace(',', '.')
        if suffix else raw_number.replace(',', '')
    )
    multiplier: int = 1
    if suffix == 'k':
        multiplier = 1_000
    elif suffix == 'm':
        multiplier = 1_000_000
    elif suffix == 'b':
        multiplier = 1_000_000_000
    return int(float(number) * multiplier)


def _first_str(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _first_optional_str(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            return value
    return None


def _first_bool(*values: object) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None


def _str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _dict_or_none(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    return None


def _bio_links_from_value(value: object) -> list[InstagramBioLink]:
    links: list[InstagramBioLink] = []
    for item in _dict_list(value):
        link = InstagramBioLink(
            title=_optional_str(item.get('title')),
            url=_optional_str(item.get('url')),
            lynx_url=_optional_str(item.get('lynx_url')),
            link_type=_optional_str(item.get('link_type')),
            image_url=_optional_str(item.get('image_url')),
            media_type=_optional_str(item.get('media_type')),
            media_accent_color_hex=_optional_str(
                item.get('media_accent_color_hex'),
            ),
            creation_source=_optional_str(item.get('creation_source')),
            is_pinned=_first_bool(item.get('is_pinned')),
        )
        if link.url or link.lynx_url or link.title:
            links.append(link)
    return links


def _walk_dicts(value: object) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        found.append(value)
        for child in value.values():
            found.extend(_walk_dicts(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_dicts(child))
    return found


def _candidate_profile_dicts(data: object) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in _walk_dicts(data):
        username: object = (
            item.get('username')
            or item.get('user_name')
            or item.get('alternateName')
        )
        follower: object = (
            item.get('follower_count')
            or item.get('followers_count')
            or item.get('edge_followed_by')
        )
        if username is not None or follower is not None:
            candidates.append(item)
    return candidates


def _count_from_edge(value: object) -> int | None:
    if isinstance(value, dict):
        return parse_count(value.get('count'))
    return parse_count(value)


def _profile_from_dict(item: dict[str, Any]) -> InstagramProfileData | None:
    username: str | None = _first_str(
        item.get('username'),
        item.get('user_name'),
        item.get('alternateName'),
    )
    if username and username.startswith('@'):
        username = username[1:]
    if not username:
        return None
    follower_count: int | None = _count_from_edge(
        item.get('edge_followed_by')
        or item.get('follower_count')
        or item.get('followers_count')
    )
    following_count: int | None = _count_from_edge(
        item.get('edge_follow')
        or item.get('following_count')
        or item.get('follows_count')
    )
    post_count: int | None = _count_from_edge(
        item.get('edge_owner_to_timeline_media')
        or item.get('post_count')
        or item.get('media_count')
        or item.get('all_media_count')
    )
    return InstagramProfileData(
        username=username.lower(),
        pk=_first_str(item.get('pk')),
        user_id=_first_str(item.get('id'), item.get('pk')),
        full_name=_first_str(item.get('full_name'), item.get('name')),
        biography=_first_optional_str(
            item.get('biography'), item.get('bio'),
        ),
        external_url=_first_str(item.get('external_url')),
        bio_links=_bio_links_from_value(item.get('bio_links')),
        avatar_url=_first_str(
            item.get('profile_pic_url_hd'),
            item.get('profile_pic_url'),
            item.get('image'),
        ),
        follower_count=follower_count,
        following_count=following_count,
        post_count=post_count,
        verified=bool(item.get('is_verified', False)),
        private_account=bool(item.get('is_private', False)),
        category=_first_str(item.get('category_name')),
        business_account=_first_bool(item.get('is_business_account')),
        professional_account=_first_bool(
            item.get('is_professional_account'),
        ),
        unpublished=_first_bool(item.get('is_unpublished')),
        memorialized=_first_bool(item.get('is_memorialized')),
        coppa_enforced=_first_bool(item.get('is_coppa_enforced')),
        has_any_clips=_first_bool(item.get('has_any_clips')),
        pronouns=_str_list(item.get('pronouns')),
        account_badges=_dict_list(item.get('account_badges')),
        linked_fb_info=_dict_or_none(item.get('linked_fb_info')),
        text_post_app_badge_label=_first_str(
            item.get('text_post_app_badge_label'),
        ),
        show_text_post_app_badge=_first_bool(
            item.get('show_text_post_app_badge'),
        ),
        detected_markers=['structured_profile'],
    )


def _extract_json_objects(soup: BeautifulSoup) -> list[object]:
    objects: list[object] = []
    for script in soup.find_all('script'):
        text: str = script.string or script.get_text() or ''
        stripped: str = text.strip()
        if not stripped:
            continue
        if script.get('type') == 'application/ld+json':
            try:
                objects.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
        for marker in (
            '"username"', '"edge_followed_by"', '"follower_count"',
        ):
            if marker in stripped:
                break
        else:
            continue
        start: int = stripped.find('{')
        end: int = stripped.rfind('}')
        if start < 0 or end <= start:
            continue
        try:
            objects.append(json.loads(stripped[start:end + 1]))
        except json.JSONDecodeError:
            continue
    return objects


def _meta_content(soup: BeautifulSoup, *names: str) -> str | None:
    for name in names:
        tag = soup.find('meta', attrs={'property': name})
        if tag is None:
            tag = soup.find('meta', attrs={'name': name})
        if tag is not None:
            content: object = tag.get('content')
            if isinstance(content, str) and content:
                return content
    return None


def _profile_from_meta(
    soup: BeautifulSoup,
    requested_username: str,
) -> InstagramProfileData | None:
    description: str | None = _meta_content(
        soup, 'og:description', 'description',
    )
    if not description:
        return None
    follower_count: int | None = None
    following_count: int | None = None
    post_count: int | None = None
    match: re.Match[str] | None = _META_FOLLOWERS_RE.search(
        description,
    )
    if match is not None:
        follower_count = parse_count(match.group('count'))
    match = _META_FOLLOWING_RE.search(description)
    if match is not None:
        following_count = parse_count(match.group('count'))
    match = _META_POSTS_RE.search(description)
    if match is not None:
        post_count = parse_count(match.group('count'))
    return InstagramProfileData(
        username=requested_username.lower(),
        full_name=_meta_content(soup, 'og:title'),
        biography=description,
        avatar_url=_meta_content(soup, 'og:image'),
        follower_count=follower_count,
        following_count=following_count,
        post_count=post_count,
        detected_markers=['meta_profile'],
    )


def _apply_meta_fallback(
    profile: InstagramProfileData,
    meta_profile: InstagramProfileData | None,
) -> InstagramProfileData:
    if meta_profile is None:
        return profile
    markers: list[str] = list(profile.detected_markers)
    used_meta: bool = False
    if profile.follower_count is None:
        profile.follower_count = meta_profile.follower_count
        used_meta = meta_profile.follower_count is not None
    if profile.following_count is None:
        profile.following_count = meta_profile.following_count
        used_meta = used_meta or meta_profile.following_count is not None
    if profile.post_count is None:
        profile.post_count = meta_profile.post_count
        used_meta = used_meta or meta_profile.post_count is not None
    if profile.full_name is None:
        profile.full_name = meta_profile.full_name
        used_meta = used_meta or meta_profile.full_name is not None
    if profile.avatar_url is None:
        profile.avatar_url = meta_profile.avatar_url
        used_meta = used_meta or meta_profile.avatar_url is not None
    if used_meta and 'meta_profile' not in markers:
        markers.append('meta_profile')
    profile.detected_markers = markers
    return profile


def extract_profile_data(
    html: str,
    requested_username: str,
) -> InstagramProfileData:
    '''Extract public profile metadata from an Instagram HTML page.'''
    lowered: str = html.lower()
    if (
        'sorry, this page isn' in lowered
        or 'link you followed may be broken' in lowered
    ):
        raise InstagramProfileUnavailableError(requested_username)

    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    meta_profile: InstagramProfileData | None = _profile_from_meta(
        soup, requested_username,
    )
    fallback_profile: InstagramProfileData | None = None
    for obj in _extract_json_objects(soup):
        for candidate in _candidate_profile_dicts(obj):
            profile: InstagramProfileData | None = (
                _profile_from_dict(candidate)
            )
            if profile is None:
                continue
            if profile.username.casefold() != requested_username.casefold():
                raise InstagramIdentityError(
                    f'requested={requested_username!r} '
                    f'returned={profile.username!r}',
                )
            if profile.follower_count is None:
                if fallback_profile is None:
                    fallback_profile = profile
                continue
            return _apply_meta_fallback(profile, meta_profile)

    if fallback_profile is not None:
        return _apply_meta_fallback(fallback_profile, meta_profile)
    if meta_profile is not None:
        return meta_profile
    if (
        '/accounts/login/' in lowered
        or '/challenge/' in lowered
        or 'please wait a few minutes' in lowered
        or 'checkpoint' in lowered
    ):
        raise InstagramRateLimitError(requested_username)
    raise RuntimeError('instagram profile metadata not found')
