'''Explicit failure categories; generic errors never delete creators.'''

import asyncio

from playwright.async_api import Error as BrowserError


class ProfileExtractionError(RuntimeError):
    '''No trustworthy profile identity was observed.'''


class ProfileIdentityError(RuntimeError):
    '''The observed identity conflicts with another observation.'''


class ProfileUnavailableError(RuntimeError):
    '''The website definitively reported a nonexistent profile.'''


class ProfileRateLimitError(RuntimeError):
    '''The website blocked an anonymous request.'''


def classify_twitch_error(exc: BaseException) -> str:
    if isinstance(exc, ProfileIdentityError):
        return 'identity_conflict'
    if isinstance(exc, ProfileUnavailableError):
        return 'unavailable'
    if isinstance(exc, ProfileRateLimitError):
        return 'rate_limit'
    if isinstance(exc, ProfileExtractionError):
        return 'extraction'
    if isinstance(exc, (asyncio.TimeoutError, BrowserError)):
        return 'transient'
    if isinstance(exc, OSError):
        return 'storage'
    return 'other'
