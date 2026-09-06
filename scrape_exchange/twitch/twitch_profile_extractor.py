'''Pure extraction of identity-matched anonymous website data.'''

import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from bs4 import BeautifulSoup, PageElement, Tag

from scrape_exchange.twitch.normalization import normalize_creator
from scrape_exchange.twitch.twitch_creator import (
    TwitchCreator,
    TwitchLink,
    TwitchPanel,
)
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileExtractionError,
    ProfileIdentityError,
)


def _objects(value: object, depth: int = 0) -> Iterator[dict[str, Any]]:
    if depth > 30:
        return
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _objects(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            yield from _objects(child, depth + 1)


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _count(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    return None


def _apply_user(creator: TwitchCreator, user: dict[str, Any]) -> None:
    fields: dict[str, str] = {
        'displayName': 'display_name', 'description': 'biography',
        'profileImageURL': 'avatar_url', 'bannerImageURL': 'banner_url',
        'profileBannerURL': 'banner_url',
    }
    for source, target in fields.items():
        value: str | None = _string(user.get(source))
        if value is not None:
            setattr(creator, target, value)
    followers: object = user.get('followers')
    if isinstance(followers, dict):
        count: int | None = _count(followers.get('totalCount'))
        if count is not None:
            creator.follower_count = count
            creator.follower_count_is_approximate = False
    roles: object = user.get('roles')
    if isinstance(roles, dict):
        for source, target in (
            ('isPartner', 'partner'), ('isAffiliate', 'affiliate'),
        ):
            if isinstance(roles.get(source), bool):
                setattr(creator, target, roles[source])
    links: object = user.get('socialMedia')
    channel: object = user.get('channel')
    if isinstance(channel, dict):
        links = channel.get('socialMedias', links)
    if isinstance(links, list):
        for item in links:
            if isinstance(item, dict) and _string(item.get('url')):
                link: TwitchLink = TwitchLink(
                    url=item['url'], title=_string(item.get('title')),
                )
                if link not in creator.social_links:
                    creator.social_links.append(link)
    panels: object = user.get('panels')
    if isinstance(panels, list):
        for item in panels:
            if not isinstance(item, dict):
                continue
            panel: TwitchPanel = TwitchPanel(
                title=_string(item.get('title')),
                description=_string(item.get('description')),
                image_url=_string(item.get('imageURL')),
                link_url=_string(item.get('linkURL')),
            )
            if panel.model_dump(exclude_none=True) and (
                panel not in creator.panels
            ):
                creator.panels.append(panel)


def _meta(soup: BeautifulSoup, name: str) -> str | None:
    tag: Tag | None = soup.select_one(f'meta[property="{name}"]')
    return _string(tag.get('content')) if tag else None


def extract_profile(
    payloads: list[object], html: str, username: str, base_url: str,
) -> TwitchCreator:
    '''Use exact handle/id matches; never treat a generic page as a user.'''
    if normalize_creator(username) != username:
        raise ProfileIdentityError('Invalid normalized username')
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    objects: list[dict[str, Any]] = []
    for payload in payloads:
        objects.extend(_objects(payload))
    for script in soup.find_all('script', type=(
        'application/json', 'application/ld+json',
    )):
        try:
            objects.extend(_objects(json.loads(script.get_text())))
        except (ValueError, RecursionError):
            continue
    matching: list[dict[str, Any]] = [
        item for item in objects
        if isinstance(item.get('login'), str)
        and item['login'].casefold() == username
    ]
    ids: set[str] = {
        str(item['id']) for item in matching
        if isinstance(item.get('id'), (str, int))
        and not isinstance(item['id'], bool)
    }
    if len(ids) > 1:
        raise ProfileIdentityError('Conflicting account IDs in responses')
    creator: TwitchCreator = TwitchCreator(
        username=username, url=f'{base_url.rstrip("/")}/{username}',
        scraped_timestamp=datetime.now(UTC),
        user_id=next(iter(ids), None),
    )
    if creator.user_id:
        matching.extend(
            item for item in objects
            if str(item.get('id')) == creator.user_id
            and 'login' not in item
            and item.get('__typename', 'User') == 'User'
        )
    for item in matching:
        _apply_user(creator, item)
    if matching:
        creator.sources.append('structured')

    canonical: str | None = _meta(soup, 'og:url')
    tag: Tag | None = soup.select_one('link[rel="canonical"]')
    if canonical is None and tag:
        canonical = _string(tag.get('href'))
    page_matches: bool = bool(
        canonical and normalize_creator(canonical, base_url) == username
    )
    title: str | None = _meta(soup, 'og:title')
    heading: Tag | None = soup.select_one(
        '[data-a-target="user-display-name"]',
    )
    about: Tag | None = soup.select_one('[data-a-target="about-panel"]')
    if heading is None and about is not None:
        heading = about.select_one('h3')
    if page_matches and (title or heading):
        if creator.display_name is None:
            if heading is not None:
                creator.display_name = heading.get_text(
                    strip=True,
                ).removeprefix('About ')
            elif title:
                creator.display_name = title.removesuffix(' - Twitch')
        if creator.biography is None:
            creator.biography = _meta(soup, 'og:description')
            if creator.biography is None and about is not None:
                biography: Tag | None = about.select_one('p[dir="auto"]')
                if biography:
                    creator.biography = biography.get_text(' ', strip=True)
        if creator.avatar_url is None:
            creator.avatar_url = _meta(soup, 'og:image')
        creator.sources.append('html')
        count_tag: Tag | None = soup.select_one(
            '[data-a-target="followers-count"]',
        )
        if count_tag is None and about is not None:
            followers: PageElement | None = about.find(string=re.compile(
                r'^\s*followers?\s*$', re.IGNORECASE,
            ))
            if followers is not None:
                count_tag = followers.parent
        if count_tag and creator.follower_count is None:
            match: re.Match[str] | None = re.fullmatch(
                r'([\d,.]+)\s*([KMB]?)\s*followers?',
                count_tag.get_text(' ', strip=True), re.IGNORECASE,
            )
            if match:
                suffix: str = match[2].upper()
                creator.follower_count = int(
                    float(match[1].replace(',', ''))
                    * {'': 1, 'K': 1000, 'M': 1000000, 'B': 1000000000}[
                        suffix
                    ]
                )
                creator.follower_count_is_approximate = bool(suffix)
    if not creator.sources:
        raise ProfileExtractionError('No matching public profile found')
    if all(value is not None for value in (
        creator.user_id, creator.display_name, creator.biography,
        creator.avatar_url, creator.follower_count,
    )):
        creator.completeness = 'complete'
    return creator
