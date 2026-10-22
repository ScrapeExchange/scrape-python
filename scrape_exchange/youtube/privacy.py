'''
Privacy limits for YouTube channels and videos.

Small YouTube channels are most likely individuals. Before a record is
written to disk for upload, the scrapers classify its channel by size:

- ``TINY`` (below ``CHANNEL_MIN_SUBSCRIBERS``, or of unknown size):
  the record is kept on disk with a marker suffix and never uploaded.
- ``SMALL`` (below ``YOUTUBE_CHANNEL_PRIVACY_SUBSCRIBERS``): email
  addresses, phone numbers, street addresses and links to personal
  websites in descriptions are masked.
- ``LARGE``: the record is uploaded unchanged.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re

from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

from prometheus_client import Counter

from ..channel_scrape_queue import KEY_PREFIX as CHANNEL_KEY_PREFIX
from .settings import YouTubeScraperSettings
from .youtube_external_link import YouTubeExternalLink

if TYPE_CHECKING:
    from .youtube_channel import YouTubeChannel
    from .youtube_video import YouTubeVideo


EMAIL_HIDDEN: str = '<email hidden>'
PHONE_HIDDEN: str = '<phone hidden>'
ADDRESS_HIDDEN: str = '<address hidden>'
URL_WITHHELD: str = '<url withheld>'

PRIVACY_MASKED_ITEMS: Counter = Counter(
    'youtube_privacy_masked_items_total',
    'Personal data items masked in YouTube descriptions',
    ['kind'],
)


class ChannelSize(str, Enum):
    TINY = 'tiny'
    SMALL = 'small'
    LARGE = 'large'


def classify_channel_size(
    subscriber_count: int | None,
    view_count: int | None,
    *,
    min_subscribers: int,
    privacy_subscribers: int,
    views_per_subscriber: int,
) -> ChannelSize:
    '''Classify a channel from its subscriber count, falling back to
    its total view count compared to each threshold times
    *views_per_subscriber*. A channel with neither count is ``TINY``.
    A threshold of 0 disables its rule: ``min_subscribers=0`` never
    returns ``TINY`` and ``privacy_subscribers=0`` never returns
    ``SMALL``.'''
    count: int | None = subscriber_count
    factor: int = 1
    if count is None and view_count is not None:
        count = view_count
        factor = views_per_subscriber
    if count is None:
        if min_subscribers > 0:
            return ChannelSize.TINY
        if privacy_subscribers > 0:
            return ChannelSize.SMALL
        return ChannelSize.LARGE
    if min_subscribers > 0 and count < min_subscribers * factor:
        return ChannelSize.TINY
    if privacy_subscribers > 0 and count < privacy_subscribers * factor:
        return ChannelSize.SMALL
    return ChannelSize.LARGE


def classify_for_settings(
    subscriber_count: int | None,
    view_count: int | None,
    settings: YouTubeScraperSettings,
) -> ChannelSize:
    ''':func:`classify_channel_size` with the thresholds of
    *settings*.'''
    return classify_channel_size(
        subscriber_count, view_count,
        min_subscribers=settings.channel_min_subscribers,
        privacy_subscribers=settings.channel_privacy_subscribers,
        views_per_subscriber=settings.channel_views_per_subscriber,
    )


# Domains behind each key of scrape_exchange.datatypes.SocialNetworks.
# Keep the keys in sync with that dict; a unit test checks it.
SOCIAL_NETWORK_DOMAINS: dict[str, tuple[str, ...]] = {
    'facebook': ('facebook.com', 'fb.com', 'fb.me'),
    'twitter': ('twitter.com', 't.co'),
    'x': ('x.com',),
    'instagram': ('instagram.com', 'instagr.am'),
    'youtube': ('youtube.com', 'youtu.be'),
    'linkedin': ('linkedin.com', 'lnkd.in'),
    'pinterest': ('pinterest.com', 'pin.it'),
    'tumblr': ('tumblr.com',),
    'reddit': ('reddit.com', 'redd.it'),
    'snapchat': ('snapchat.com',),
    'flickr': ('flickr.com',),
    'tiktok': ('tiktok.com',),
    'whatsapp': ('whatsapp.com', 'wa.me'),
    'telegram': ('telegram.org', 'telegram.me', 't.me'),
    'signal': ('signal.org', 'signal.me'),
    'messenger': ('messenger.com', 'm.me'),
    'parler': ('parler.com',),
    'gab': ('gab.com',),
    'rumble': ('rumble.com',),
    'patreon': ('patreon.com',),
    'twitch': ('twitch.tv',),
    'spotify': ('spotify.com', 'spotify.link'),
    'discord': ('discord.com', 'discord.gg'),
    'slack': ('slack.com',),
    'nebula': ('nebula.tv',),
    'wechat': ('wechat.com',),
    'douyin': ('douyin.com',),
    'kuaishou': ('kuaishou.com',),
    'weibo': ('weibo.com',),
    'qq': ('qq.com',),
    'qzone': ('qzone.qq.com',),
    'myjosh': ('myjosh.in',),
    'microsoft': ('microsoft.com', 'teams.live.com'),
    'quora': ('quora.com',),
    'skype': ('skype.com',),
    'tieba': ('tieba.baidu.com',),
    'baidu': ('baidu.com',),
    'viber': ('viber.com',),
    'line': ('line.me',),
    'imo': ('imo.im',),
    'xiaohongshu': ('xiaohongshu.com', 'xhslink.com'),
    'likee': ('likee.video',),
    'picsart': ('picsart.com',),
    'soundcloud': ('soundcloud.com',),
    'onlyfans': ('onlyfans.com',),
    'vevo': ('vevo.com',),
    'bbc': ('bbc.com', 'bbc.co.uk'),
    'vk': ('vk.com',),
    'threads': ('threads.net', 'threads.com'),
    'zoom': ('zoom.us',),
    'meet': ('meet.google.com',),
    'clubhouse': ('clubhouse.com',),
    'imessage': ('apple.com',),
    'facetime': ('apple.com',),
    'byo': ('byo.tube',),
    'steampowered': ('steampowered.com', 'steamcommunity.com'),
    'linktr.ee': ('linktr.ee',),
    'amzn': ('amzn.to', 'amzn.eu'),
    'amazon': ('amazon.com',),
}

# Link-in-bio services, creator platforms and well-known sites. Links
# to these are not personal data.
_WELL_KNOWN_DOMAINS: tuple[str, ...] = (
    'linktr.ee', 'beacons.ai', 'bio.link', 'linkin.bio', 'campsite.bio',
    'lnk.bio', 'carrd.co', 'solo.to', 'linkfire.com', 'lnk.to',
    'ko-fi.com', 'buymeacoffee.com', 'substack.com', 'medium.com',
    'bandcamp.com', 'github.com', 'gitlab.com', 'etsy.com',
    'gofundme.com', 'kickstarter.com', 'indiegogo.com', 'paypal.com',
    'paypal.me', 'wikipedia.org', 'wikimedia.org', 'imdb.com',
    'apple.com', 'music.apple.com', 'podcasts.apple.com', 'google.com',
    'goo.gl', 'bit.ly', 'amazon.co.uk', 'amazon.de', 'amazon.fr',
    'amazon.it', 'amazon.es', 'amazon.nl', 'amazon.ca', 'amazon.com.au',
    'amazon.co.jp', 'amazon.in', 'amazon.com.br', 'amazon.com.mx',
    'deezer.com', 'tidal.com', 'shazam.com', 'vimeo.com',
    'dailymotion.com', 'kick.com', 'scrape.exchange',
)

HARDCODED_ALLOWED_DOMAINS: frozenset[str] = frozenset(
    domain
    for domains in SOCIAL_NETWORK_DOMAINS.values()
    for domain in domains
) | frozenset(_WELL_KNOWN_DOMAINS)


def allowed_domains(settings: YouTubeScraperSettings) -> frozenset[str]:
    '''The hardcoded allowlist plus ``YOUTUBE_PRIVACY_ALLOWED_DOMAINS``.
    '''
    return HARDCODED_ALLOWED_DOMAINS | settings.extra_allowed_domains


def _host_of(url: str) -> str:
    candidate: str = url.strip()
    if '://' not in candidate:
        candidate = f'https://{candidate}'
    try:
        host: str = urlsplit(candidate).hostname or ''
    except ValueError:
        return ''
    return host.lower().rstrip('.')


def unwrap_youtube_redirect(url: str) -> str:
    '''Return the target of a ``youtube.com/redirect?q=<target>`` link,
    or *url* unchanged when it is not such a wrapper.'''
    host: str = _host_of(url)
    if host != 'youtube.com' and not host.endswith('.youtube.com'):
        return url
    candidate: str = url if '://' in url else f'https://{url}'
    parts = urlsplit(candidate)
    if parts.path.rstrip('/') != '/redirect':
        return url
    targets: list[str] = parse_qs(parts.query).get('q', [])
    return targets[0] if targets else url


def is_allowed_url(url: str, allowed: frozenset[str]) -> bool:
    '''True when the host of *url* (after unwrapping a YouTube redirect)
    is one of *allowed* or a subdomain of one.'''
    host: str = _host_of(unwrap_youtube_redirect(url))
    if not host:
        return False
    labels: list[str] = host.split('.')
    index: int
    for index in range(len(labels)):
        if '.'.join(labels[index:]) in allowed:
            return True
    return False


# --- Emails -------------------------------------------------------------

_LOCAL: str = r'[\w.%+-]+'
_LABEL: str = r'[A-Za-z0-9-]+'
_TLD: str = r'[A-Za-z]{2,}'
_BRACKET_AT: str = r'\s*[\[({<]\s*at\s*[\])}>]\s*'
_BRACKET_DOT: str = r'\s*[\[({<]\s*dot\s*[\])}>]\s*'
_WORD_DOT: str = r'\s+dot\s+'


def _domain(dot: str) -> str:
    return rf'{_LABEL}(?:(?:{dot}){_LABEL})*(?:{dot}){_TLD}\b'


# Plain addresses, then written-out forms. A word-form " at " needs a
# written-out dot ("jan at gmail dot com"), so prose such as
# "available at example.com" is not taken for an address.
_EMAIL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(rf'(?<![\w.%+-]){_LOCAL}@{_domain(r"\.")}'),
    re.compile(
        rf'(?<![\w.%+-]){_LOCAL}{_BRACKET_AT}'
        rf'{_domain(rf"{_BRACKET_DOT}|{_WORD_DOT}|\.")}',
        re.IGNORECASE,
    ),
    re.compile(
        rf'(?<![\w.%+-]){_LOCAL}\s+at\s+'
        rf'{_domain(rf"{_BRACKET_DOT}|{_WORD_DOT}")}',
        re.IGNORECASE,
    ),
    re.compile(
        rf'(?<![\w.%+-]){_LOCAL}\s+@\s+'
        rf'{_domain(rf"{_BRACKET_DOT}|{_WORD_DOT}|\s*\.\s*")}',
        re.IGNORECASE,
    ),
)


# --- URLs ---------------------------------------------------------------

# Top-level domains accepted for links without a scheme or "www.". A
# bare host with a path ("shop.de/jan") may end in any two-letter
# country code; without a path it must end in one of these, so file
# names such as "Node.js" or "README.md" are not taken for links.
_BARE_TLDS: str = (
    r'com|net|org|info|biz|edu|gov|io|co|me|tv|ai|app|dev|xyz|online|'
    r'site|website|shop|store|blog|page|live|link|art|club|music|'
    r'studio|design|tech|life|world|space|fun|top|eu|asia|example|'
    r'nl|be|de|at|ch|fr|it|es|pt|uk|ie|se|no|dk|fi|pl|cz|sk|hu|ro|'
    r'gr|tr|ru|ua|us|ca|mx|br|ar|cl|au|nz|jp|kr|cn|in|id|ph|vn|th|'
    r'my|sg|za|ng|ke'
)
_URL_CHARS: str = r'[^\s<>"\'`]+'
_BARE_HOST: str = r'(?<![\w@/.-])(?:[a-z0-9-]+\.)+'
_URL_PATTERN: re.Pattern[str] = re.compile(
    rf'(?:\bhttps?://{_URL_CHARS}'
    rf'|\bwww\.{_URL_CHARS}'
    rf'|{_BARE_HOST}[a-z]{{2,}}/{_URL_CHARS}'
    rf'|{_BARE_HOST}(?:{_BARE_TLDS})\b(?![.-]?\w))',
    re.IGNORECASE,
)
_TRAILING_PUNCTUATION: str = '.,;:!?)]}\'"'


# --- Phone numbers ------------------------------------------------------

_PHONE_PATTERN: re.Pattern[str] = re.compile(
    r'(?<![\w+])(?:\+\d{1,3}[ .-]?)?(?:\(\d{1,4}\)[ .-]?)?'
    r'\d(?:[ .-]?\d)+(?![\w])'
)
_MIN_PHONE_DIGITS: int = 8
_MAX_PHONE_DIGITS: int = 15
_DATE_LIKE: re.Pattern[str] = re.compile(
    r'(?:19|20)\d{2}[-/.](?:19|20)\d{2}'
    r'|(?:19|20)\d{2}[-/.]\d{1,2}[-/.]\d{1,2}'
    r'|\d{1,2}[-/.]\d{1,2}[-/.](?:19|20)\d{2}'
)
_THOUSANDS: re.Pattern[str] = re.compile(r'[1-9]\d{0,2}(?:[ .,]\d{3})+')
# "1.000.000" or "1,234,567" inside a longer run of numbers.
_GROUPED_NUMBER: re.Pattern[str] = re.compile(
    r'(?<!\d)\d{1,3}(?:([.,])\d{3})(?:\1\d{3})+(?!\d)'
)
_YEARS_ONLY: re.Pattern[str] = re.compile(
    r'(?:19|20)\d{2}(?:[ .-](?:19|20)\d{2})+'
)


def _is_phone(candidate: str) -> bool:
    digits: int = sum(char.isdigit() for char in candidate)
    if not _MIN_PHONE_DIGITS <= digits <= _MAX_PHONE_DIGITS:
        return False
    if _DATE_LIKE.search(candidate) or _YEARS_ONLY.fullmatch(candidate):
        return False
    if _GROUPED_NUMBER.search(candidate):
        return False
    return not _THOUSANDS.fullmatch(candidate)


# --- Street addresses ---------------------------------------------------

_NAME: str = r"[A-Z][\w'’.-]*"
_HOUSE_NUMBER: str = r'\d{1,5}[A-Za-z]?\b'
_ADDRESS_PATTERNS: tuple[re.Pattern[str], ...] = (
    # "221B Baker Street", "1600 Pennsylvania Avenue"
    re.compile(
        rf'\b{_HOUSE_NUMBER}\s+(?:{_NAME}\s+){{1,4}}'
        # Only unambiguous street types: "Way", "Place", "Court",
        # "Square" and "Drive" are common in title-case video phrases.
        r'(?:Street|St\.|Road|Rd\.|Avenue|Ave\.|Lane|Ln\.|Boulevard|'
        r'Blvd\.)(?!\w)'
    ),
    # "12 rue de la Paix", "7 avenida del Mar"
    re.compile(
        rf'\b{_HOUSE_NUMBER},?\s+'
        r'(?:rue|avenue|boulevard|chemin|place|calle|avenida|via|viale|'
        r'rua|praça)\s+'
        r"(?:(?:de|du|des|la|le|les|del|de la|di|da|dos|das|l'|d')\s+)*"
        rf'{_NAME}',
        re.IGNORECASE,
    ),
    # "Kerkstraat 12", "Hauptstrasse 5", "Dorpsweg 3a"
    re.compile(
        r'\b[A-ZÄÖÜ][a-zäöüß]+'
        r'(?:straat|weg|laan|plein|gracht|kade|singel|dijk|steeg|'
        r'strasse|straße|str\.|gasse|allee|platz|ring|damm)'
        rf'\s+{_HOUSE_NUMBER}'
    ),
    # "Calle Mayor 7", "Via Roma 10", "Rue Lepic 5"
    re.compile(
        r'\b(?:Calle|Avenida|Via|Viale|Rua|Rue|Avenue|Piazza|Plaza)\s+'
        rf'(?:{_NAME}\s+){{1,3}}{_HOUSE_NUMBER}'
    ),
)


# --- Masking ------------------------------------------------------------

_KEEP_OPEN: str = ''
_KEEP_CLOSE: str = ''
_KEEP_TOKEN: re.Pattern[str] = re.compile(
    f'{_KEEP_OPEN}(.){_KEEP_CLOSE}'
)


def _mask_emails(text: str) -> str:
    pattern: re.Pattern[str]
    for pattern in _EMAIL_PATTERNS:
        text, count = pattern.subn(EMAIL_HIDDEN, text)
        if count:
            PRIVACY_MASKED_ITEMS.labels(kind='email').inc(count)
    return text


def _mask_urls(
    text: str, allowed: frozenset[str], kept: list[str],
) -> str:
    '''Replace non-allowed URLs and swap allowed ones for tokens so the
    phone and address passes do not touch them.'''

    def replace(match: re.Match[str]) -> str:
        url: str = match.group(0)
        tail: str = ''
        while url and url[-1] in _TRAILING_PUNCTUATION:
            tail = url[-1] + tail
            url = url[:-1]
        if not url:
            return match.group(0)
        if is_allowed_url(url, allowed):
            kept.append(url)
            token: str = chr(0xE100 + len(kept) - 1)
            return f'{_KEEP_OPEN}{token}{_KEEP_CLOSE}{tail}'
        PRIVACY_MASKED_ITEMS.labels(kind='url').inc()
        return f'{URL_WITHHELD}{tail}'

    return _URL_PATTERN.sub(replace, text)


def _mask_phones(text: str) -> str:

    def replace(match: re.Match[str]) -> str:
        if not _is_phone(match.group(0)):
            return match.group(0)
        PRIVACY_MASKED_ITEMS.labels(kind='phone').inc()
        return PHONE_HIDDEN

    return _PHONE_PATTERN.sub(replace, text)


def _mask_addresses(text: str) -> str:
    pattern: re.Pattern[str]
    for pattern in _ADDRESS_PATTERNS:
        text, count = pattern.subn(ADDRESS_HIDDEN, text)
        if count:
            PRIVACY_MASKED_ITEMS.labels(kind='address').inc(count)
    return text


def mask_text(text: str | None, allowed: frozenset[str]) -> str | None:
    '''Mask email addresses, links to non-allowed sites, phone numbers
    and street addresses in *text*. Emails go first so their domains
    are not taken for links; links next so the digits in a kept link
    are not taken for a phone number.'''
    if not text:
        return text
    kept: list[str] = []
    masked: str = _mask_emails(text)
    masked = _mask_urls(masked, allowed, kept)
    masked = _mask_phones(masked)
    masked = _mask_addresses(masked)
    return _KEEP_TOKEN.sub(
        lambda match: kept[ord(match.group(1)) - 0xE100], masked,
    )


# --- Records ------------------------------------------------------------

def _mask_link_url(url: str, allowed: frozenset[str]) -> str:
    if url.lower().startswith('mailto:'):
        PRIVACY_MASKED_ITEMS.labels(kind='email').inc()
        return EMAIL_HIDDEN
    if is_allowed_url(url, allowed):
        return url
    PRIVACY_MASKED_ITEMS.labels(kind='url').inc()
    return URL_WITHHELD


def mask_channel(
    channel: 'YouTubeChannel', allowed: frozenset[str],
) -> None:
    '''Mask the description and the external links of *channel* in
    place. A link keeps its name and priority.'''
    channel.description = mask_text(channel.description, allowed)
    channel.external_urls = {
        YouTubeExternalLink(
            name=link.name,
            url=_mask_link_url(link.url, allowed),
            priority=link.priority,
        )
        for link in channel.external_urls or set()
    }


def mask_video(video: 'YouTubeVideo', allowed: frozenset[str]) -> None:
    '''Mask the description of *video* in place.'''
    video.description = mask_text(video.description, allowed)


def mask_channel_record(record: dict, allowed: frozenset[str]) -> None:
    '''Mask the description of an RSS lite channel record in place.'''
    if record.get('description'):
        record['description'] = mask_text(record['description'], allowed)


def parse_count(value: Any) -> int | None:
    '''Parse a count stored in Redis; None when absent or invalid.'''
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='replace')
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def lookup_channel_counts(
    redis: Any, channel_id: str | None,
) -> tuple[int | None, int | None] | None:
    '''Return ``(subscriber_count, view_count)`` from the channel scrape
    queue's meta hash of *channel_id*, or ``None`` when the channel is
    not in the queue. A count that is missing or unparseable is
    ``None``.'''
    if not channel_id:
        return None
    key: str = f'{CHANNEL_KEY_PREFIX}:meta:i:{channel_id}'
    values: list[Any] = await redis.hmget(
        key, ['subscriber_count', 'view_count'],
    )
    if all(value is None for value in values) and not await redis.exists(
        key,
    ):
        return None
    return parse_count(values[0]), parse_count(values[1])
