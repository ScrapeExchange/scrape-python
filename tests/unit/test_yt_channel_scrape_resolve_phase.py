'''Resolve-phase tests for the new queue path.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.channel_scrape_queue import (
    ChannelState,
)
from scrape_exchange.youtube.channel_identity import (
    InconsistentIdentityError,
)


def _mock_channel(
    channel_id: str | None = None,
    resolve_returns: bool = True,
    resolve_raises: Exception | None = None,
) -> MagicMock:
    instance = MagicMock()
    instance.channel_id = channel_id
    if resolve_raises is not None:
        instance._resolve_channel_id_via_innertube = (
            AsyncMock(side_effect=resolve_raises)
        )
    else:
        instance._resolve_channel_id_via_innertube = (
            AsyncMock(return_value=resolve_returns)
        )
    instance._create_browse_client = MagicMock()
    return instance


def _mock_settings() -> MagicMock:
    s = MagicMock()
    s.channel_resolve_max_attempts = 5
    s.channel_resolve_backoff_seconds = 300
    s.channel_data_directory = '/tmp/test'
    s.proxies = []
    return s


class TestResolveOne(
    unittest.IsolatedAsyncioTestCase,
):

    @patch('tools.yt_channel_scrape.YouTubeChannel')
    async def test_success_promotes(
        self,
        MockChannel: MagicMock,
    ) -> None:
        mock_ch = _mock_channel(
            channel_id='UCabc00000000000000000000',
            resolve_returns=True,
        )
        MockChannel.return_value = mock_ch
        queue = AsyncMock()
        identity = AsyncMock()
        from tools.yt_channel_scrape import (
            _resolve_one_queued,
        )
        await _resolve_one_queued(
            'foo',
            queue=queue,
            identity=identity,
            settings=_mock_settings(),
        )
        queue.promote_to_scheduled.assert_awaited_once_with(
            'foo', 'UCabc00000000000000000000',
        )

    @patch('tools.yt_channel_scrape.YouTubeChannel')
    async def test_resolve_returns_false_marks_not_found(
        self,
        MockChannel: MagicMock,
    ) -> None:
        mock_ch = _mock_channel(
            channel_id=None,
            resolve_returns=False,
        )
        MockChannel.return_value = mock_ch
        queue = AsyncMock()
        identity = AsyncMock()
        from tools.yt_channel_scrape import (
            _resolve_one_queued,
        )
        await _resolve_one_queued(
            'badhandle',
            queue=queue,
            identity=identity,
            settings=_mock_settings(),
        )
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.NOT_FOUND,
        )

    @patch('tools.yt_channel_scrape.YouTubeChannel')
    async def test_transient_increments_attempts(
        self,
        MockChannel: MagicMock,
    ) -> None:
        mock_ch = _mock_channel(
            resolve_raises=OSError('timeout'),
        )
        MockChannel.return_value = mock_ch
        queue = AsyncMock()
        queue.get_meta.return_value = {
            'resolve_attempts': '2',
        }
        identity = AsyncMock()
        s = _mock_settings()
        s.channel_resolve_max_attempts = 5
        from tools.yt_channel_scrape import (
            _resolve_one_queued,
        )
        await _resolve_one_queued(
            'foo',
            queue=queue,
            identity=identity,
            settings=s,
        )
        queue.requeue_with_backoff.assert_awaited_once()
        queue.mark.assert_not_called()

    @patch('tools.yt_channel_scrape.YouTubeChannel')
    async def test_exhausted_marks_unresolved(
        self,
        MockChannel: MagicMock,
    ) -> None:
        mock_ch = _mock_channel(
            resolve_raises=OSError('timeout'),
        )
        MockChannel.return_value = mock_ch
        queue = AsyncMock()
        queue.get_meta.return_value = {
            'resolve_attempts': '5',
        }
        identity = AsyncMock()
        s = _mock_settings()
        s.channel_resolve_max_attempts = 5
        from tools.yt_channel_scrape import (
            _resolve_one_queued,
        )
        await _resolve_one_queued(
            'foo',
            queue=queue,
            identity=identity,
            settings=s,
        )
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.UNRESOLVED,
        )

    @patch('tools.yt_channel_scrape.YouTubeChannel')
    async def test_inconsistent_identity_marks(
        self,
        MockChannel: MagicMock,
    ) -> None:
        mock_ch = _mock_channel(
            channel_id='UCabc00000000000000000000',
            resolve_returns=True,
        )
        MockChannel.return_value = mock_ch
        queue = AsyncMock()
        identity = AsyncMock()
        identity.bind.side_effect = (
            InconsistentIdentityError('conflict')
        )
        from tools.yt_channel_scrape import (
            _resolve_one_queued,
        )
        await _resolve_one_queued(
            'foo',
            queue=queue,
            identity=identity,
            settings=_mock_settings(),
        )
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.INCONSISTENT_IDENTITY,
        )
