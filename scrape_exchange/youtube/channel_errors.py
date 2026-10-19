'''
Errors for YouTube channels that YouTube no longer shows, and the
classification of the error alert InnerTube returns for them.

Kept free of other scrape_exchange imports so both the InnerTube tab
scraper and the channel identity store can use them without an import
cycle; :mod:`scrape_exchange.youtube.channel_identity` re-exports the
classes.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Any


class ChannelNotFoundError(LookupError):
    '''Raised when a YouTube channel cannot be found
    via InnerTube (HTTP 404 / empty response).'''


class ChannelTerminatedError(Exception):
    '''Raised when InnerTube returns the
    "channel terminated by YouTube" sentinel.'''


# YouTube system pages that are not creator channels: a channel ID
# that redirects to one of these was folded into it (for example a
# film studio's channel into the Movies & TV store) rather than moved.
# Verified 2026-10-19: these IDs return no channel metadata.
YOUTUBE_SYSTEM_PAGE_CHANNEL_IDS: frozenset[str] = frozenset({
    'UClgRkhTL3_hImCAmdLfDE4g',  # Movies & TV
    'UCEgdi0XIXXZ-qJOFPf4JSKw',  # Sports
    'UCOpNcN46UbXVtpKMrmU4Abg',  # Gaming
    'UCYfdidRxbB8Qhf0Nx7ioOYw',  # News
    'UC4R8DWoMoI7CAwX8_LjQHig',  # Live
    'UCtFRv9O2AHqOZjjynzrv-xg',  # Learning
    'UCrpQ4p1Ql_hG8rKXIKM1MOQ',  # Fashion & Beauty
    'UCF0pVplsI8R5kcAqgtoRqoA',  # Popular on YouTube
})


class ChannelRedirectedError(Exception):
    '''Raised when YouTube redirects a channel ID to another channel
    ID, :attr:`target_channel_id`. That is the channel's new ID unless
    :attr:`to_system_page` is set.'''

    def __init__(self, channel_id: str | None, target_channel_id: str,
                 ) -> None:
        super().__init__(f'{channel_id} redirects to {target_channel_id}')
        self.target_channel_id: str = target_channel_id

    @property
    def to_system_page(self) -> bool:
        '''Whether the target is a YouTube system page, not a creator
        channel the old ID moved to.'''
        return self.target_channel_id in YOUTUBE_SYSTEM_PAGE_CHANNEL_IDS


# Phrases of the ERROR alert InnerTube returns instead of channel tabs.
# The scraper's InnerTube clients request English.
_TERMINATED_PHRASES: tuple[str, ...] = (
    'removed because it violated',
    'has been terminated',
)
_NOT_FOUND_PHRASES: tuple[str, ...] = (
    'does not exist',
    'is not available',
)


def _alert_text(alert: dict[str, Any]) -> str:
    text: Any = alert.get('text') or {}
    if not isinstance(text, dict):
        return ''
    simple: Any = text.get('simpleText')
    if isinstance(simple, str):
        return simple
    return ''.join(
        str(run.get('text', '')) for run in text.get('runs', []) or []
        if isinstance(run, dict)
    )


def raise_for_channel_alert(
    channel_data: dict[str, Any], channel_id: str | None,
) -> None:
    '''Raise the matching error when *channel_data* (an InnerTube
    channel browse response) carries an ERROR alert saying the channel
    was removed or does not exist; return otherwise.

    :raises ChannelTerminatedError: the channel was removed or its
        account terminated
    :raises ChannelNotFoundError: the channel does not exist or is not
        available
    '''
    alerts: Any = channel_data.get('alerts') or []
    if not isinstance(alerts, list):
        return
    entry: Any
    for entry in alerts:
        if not isinstance(entry, dict):
            continue
        alert: Any = (
            entry.get('alertRenderer')
            or entry.get('alertWithButtonRenderer')
        )
        if not isinstance(alert, dict) or alert.get('type') != 'ERROR':
            continue
        text: str = _alert_text(alert)
        lowered: str = text.lower()
        if any(phrase in lowered for phrase in _TERMINATED_PHRASES):
            raise ChannelTerminatedError(f'{channel_id}: {text}')
        if any(phrase in lowered for phrase in _NOT_FOUND_PHRASES):
            raise ChannelNotFoundError(f'{channel_id}: {text}')


def raise_for_channel_redirect(
    channel_data: dict[str, Any], channel_id: str | None,
) -> None:
    '''Raise :class:`ChannelRedirectedError` when *channel_data* (an
    InnerTube channel browse response without tabs) only tells the
    client to navigate to a different channel ID.'''
    actions: Any = channel_data.get('onResponseReceivedActions') or []
    if not isinstance(actions, list):
        return
    action: Any
    for action in actions:
        if not isinstance(action, dict):
            continue
        endpoint: Any = (
            (action.get('navigateAction') or {}).get('endpoint') or {}
        )
        if not isinstance(endpoint, dict):
            continue
        target: Any = (endpoint.get('browseEndpoint') or {}).get('browseId')
        if (
            isinstance(target, str)
            and target.startswith('UC')
            and len(target) == 24
            and target != channel_id
        ):
            raise ChannelRedirectedError(channel_id, target)
