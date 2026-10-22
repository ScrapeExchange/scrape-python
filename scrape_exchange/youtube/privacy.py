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

from enum import Enum
from urllib.parse import parse_qs, urlsplit

from .settings import YouTubeScraperSettings


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
