'''
Shared type aliases and enums for the TikTok scraper package.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from enum import Enum


class TikTokCallType(str, Enum):
    '''Discriminator for the TikTok rate-limiter buckets.'''
    API = 'api'
    BOOTSTRAP = 'bootstrap'
