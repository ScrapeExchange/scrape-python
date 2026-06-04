'''
Channel identity orchestration: paired writes to creator_map and
handle_map so both directions of the lookup triangle stay
consistent.

See ``CONTEXT.md`` for the inverse invariant and source precedence.
'''

import logging
import re

from scrape_exchange.creator_map import CreatorMap
from scrape_exchange.handle_map import HandleMap
from scrape_exchange.youtube.youtube_channel import YouTubeChannel


_LOGGER: logging.Logger = logging.getLogger(__name__)


class ChannelNotFoundError(LookupError):
    '''Raised when a YouTube channel cannot be found
    via InnerTube (HTTP 404 / empty response).'''


class ChannelTerminatedError(Exception):
    '''Raised when InnerTube returns the
    "channel terminated by YouTube" sentinel.'''


class InvalidHandleError(ValueError):
    '''Raised when a string cannot be parsed as a
    YouTube channel handle.'''


class InconsistentIdentityError(RuntimeError):
    '''Raised when a ``bind()`` call would create an inconsistency.

    Specifically: ``bind(id_a, handle_x)`` is called and
    ``handle_map[handle_x]`` already maps to ``id_b ≠ id_a``.
    Two different channel_ids cannot share the same handle.
    Both maps are left unchanged when this is raised.
    '''


def _normalise_handle(handle: str) -> str:
    '''Strip a single leading ``@`` and surrounding whitespace.

    Mirrors the normalisation done by
    ``YouTubeChannel.__init__`` so values written here round-trip
    cleanly with values pulled out of scraped files.
    '''
    return handle.strip().removeprefix('@')


def _validate_handle(handle: str) -> str:
    '''Apply the hard handle rules: non-empty after normalisation,
    no whitespace, no ``/``. Returns the normalised form.'''
    normalised: str = _normalise_handle(handle)
    if not normalised:
        raise ValueError('channel_handle is empty')
    if any(c.isspace() for c in normalised):
        raise ValueError(
            f'channel_handle contains whitespace: {normalised!r}'
        )
    if '/' in normalised:
        raise ValueError(
            f'channel_handle contains slash: {normalised!r}'
        )
    return normalised


def is_valid_channel_handle(handle: str) -> bool:
    '''Validate *handle* against
    :data:`YouTubeChannel.CHANNEL_HANDLE_REGEX`.'''
    if not handle:
        return False
    probe: str = f'"canonicalBaseUrl":"/@{handle}"'
    match: re.Match[str] | None = (
        YouTubeChannel.CHANNEL_HANDLE_REGEX.search(probe)
    )
    return match is not None and match.group(1) == handle


class ChannelIdentityStore:
    '''Pair of maps with an inverse-invariant ``bind()``.

    For lookups, callers may go through either map directly — the
    only operation that *modifies* the pair is ``bind()``, which
    guarantees both maps reflect the same (id, handle) pair when
    it returns.
    '''

    def __init__(
        self,
        creator_map: CreatorMap,
        handle_map: HandleMap,
    ) -> None:
        self.creator_map: CreatorMap = creator_map
        self.handle_map: HandleMap = handle_map

    async def bind(
        self, channel_id: str, channel_handle: str,
    ) -> None:
        '''Write ``(channel_id, channel_handle)`` to both maps.

        The inconsistency check runs before any write.  If
        ``handle_map`` already maps ``channel_handle`` to a
        *different* ``channel_id``, both maps are left unchanged
        and ``InconsistentIdentityError`` is raised.

        Idempotent: calling ``bind(id, handle)`` twice with the
        same pair is safe and produces no error.

        If ``creator_map.put`` succeeds but ``handle_map.put``
        subsequently fails, callers will see a creator_map entry
        without its handle_map inverse — the next ``bind()`` for
        the same pair will repair it.

        Raises ``InconsistentIdentityError`` when
        ``channel_handle`` is already bound to a different
        ``channel_id`` in ``handle_map``; both maps are left
        unchanged in that case.
        '''
        if not channel_id:
            raise ValueError('channel_id is empty')
        # Defence in depth against non-canonical IDs leaking past
        # ``YouTubeChannel.normalise_channel_id``. The scrape queue
        # (``promote_to_scheduled`` / ``enqueue_scheduled``) makes
        # the same check; matching it here prevents the maps from
        # being corrupted by any caller that bypasses the queue.
        if not channel_id.startswith('UC'):
            raise ValueError(
                f'channel_id must start with uppercase UC: '
                f'{channel_id!r}'
            )
        handle: str = _validate_handle(channel_handle)
        existing: str | None = await self.handle_map.get(handle)
        if existing is not None and existing != channel_id:
            raise InconsistentIdentityError(
                f'handle {handle!r} already bound to'
                f' {existing!r},'
                f' cannot bind to {channel_id!r}'
            )
        await self.creator_map.put(channel_id, handle)
        await self.handle_map.put(handle, channel_id)


async def resolve_channel_id(
    channel_id: str, proxy: str | None = None,
) -> str | None:
    '''
    Resolve a YouTube ``channel_id`` to a ``channel_handle`` via
    InnerTube.

    Thin wrapper around ``YouTubeChannel.resolve_channel_id``. The
    static method returns the handle without a leading ``@`` (or
    ``None`` on InnerTube failure). This wrapper additionally
    returns ``None`` when the resolved name contains whitespace —
    matching the rule the deleted ``yt_resolve_channel_ids.py``
    applied before writing the result.

    Callers persist the (id, handle) pair via
    ``ChannelIdentityStore.bind()``. The ``.unresolved`` sentinel
    file is the caller's responsibility — it tracks per-host
    state, not library state.
    '''
    name: str | None = await YouTubeChannel.resolve_channel_id(
        channel_id, proxy=proxy,
    )
    if not name or ' ' in name:
        return None
    return name


async def resolve_channel_handle(
    handle: str, proxy: str | None = None,
) -> str | None:
    '''Resolve a YouTube handle to its channel_id via InnerTube.'''
    del proxy
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle=_normalise_handle(handle),
        with_download_client=False,
    )
    if not await channel._resolve_channel_id_via_innertube():
        return None
    return channel.channel_id
