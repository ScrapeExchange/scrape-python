'''Normalize creator input against an public website origin.'''

import re
from urllib.parse import SplitResult, urlsplit

from scrape_exchange.twitch.endpoints import PROFILE_BASE_URL

_HANDLE: re.Pattern[str] = re.compile(r'[a-zA-Z0-9_]{1,25}')
_RESERVED: set[str] = {
    'about', 'bits', 'broadcast', 'collections', 'creatorcamp',
    'dashboard', 'directory', 'downloads', 'drops', 'friends',
    'inventory', 'jobs', 'login', 'logout', 'messages', 'p',
    'payments', 'search', 'settings', 'signup', 'store',
    'subscriptions', 'turbo', 'videos', 'wallet',
}


def normalize_creator(
    value: str, base_url: str | None = PROFILE_BASE_URL,
) -> str | None:
    '''Accept a bare handle, @handle, profile URL or profile About URL.'''
    candidate: str = value.strip()
    if '://' in candidate:
        if not base_url:
            return None
        try:
            parsed: SplitResult = urlsplit(candidate)
            base: SplitResult = urlsplit(base_url)
            if (
                parsed.scheme not in ('http', 'https')
                or parsed.hostname != base.hostname
                or parsed.port != base.port
                or parsed.username is not None
                or parsed.password is not None
            ):
                return None
        except ValueError:
            return None
        parts: list[str] = parsed.path.strip('/').split('/')
        if len(parts) > 2 or (len(parts) == 2 and parts[1] != 'about'):
            return None
        candidate = parts[0]
    candidate = candidate.removeprefix('@').lower()
    if candidate in _RESERVED or not _HANDLE.fullmatch(candidate):
        return None
    return candidate
