'''Scrape-phase tests for the new queue path.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.channel_scrape_queue import (
    ChannelState,
)
from scrape_exchange.youtube.channel_identity import (
    ChannelNotFoundError,
)


def _mock_settings() -> MagicMock:
    s = MagicMock()
    s.channel_unavailable_hard_threshold = 3
    s.exchange_url = 'https://scrape.exchange'
    return s


class TestScrapeOne(
    unittest.IsolatedAsyncioTestCase,
):

    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_success_updates_tier(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = 1_000_000
        mock_scrape.return_value = channel
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.update_tier.assert_awaited_once()
        kwargs = queue.update_tier.await_args.kwargs
        self.assertEqual(kwargs['sub_count'], 1_000_000)

    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_not_found_marks(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.side_effect = ChannelNotFoundError(
            '404',
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.NOT_FOUND,
        )
        queue.update_tier.assert_not_called()

    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_transient_soft_unavailable(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.side_effect = OSError('timeout')
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark_soft_unavailable.assert_awaited_once()

    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_no_handle_marks_unresolved(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = None
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.UNRESOLVED,
        )
        mock_scrape.assert_not_called()


class TestScrapeBatch(
    unittest.IsolatedAsyncioTestCase,
):

    @patch(
        'tools.yt_channel_scrape._scrape_one_queued',
        new_callable=AsyncMock,
    )
    async def test_runs_scrapes_concurrently_up_to_limit(
        self,
        mock_scrape: AsyncMock,
    ) -> None:
        entered = 0
        max_entered = 0
        first_two_started = asyncio.Event()
        unblock = asyncio.Event()

        async def scrape_one(*args, **kwargs) -> None:
            nonlocal entered, max_entered
            entered += 1
            max_entered = max(max_entered, entered)
            if entered == 2:
                first_two_started.set()
            await unblock.wait()
            entered -= 1

        mock_scrape.side_effect = scrape_one
        settings = _mock_settings()
        settings.channel_concurrency = 2
        settings.proxies = []
        from tools.yt_channel_scrape import (
            _scrape_queued_batch,
        )
        batch = asyncio.create_task(
            _scrape_queued_batch(
                ['UC1', 'UC2', 'UC3'],
                queue=AsyncMock(),
                settings=settings,
                fm=MagicMock(),
                creator_map_backend=AsyncMock(),
                http_client=MagicMock(),
            ),
        )
        await first_two_started.wait()
        self.assertEqual(mock_scrape.await_count, 2)
        self.assertEqual(max_entered, 2)
        unblock.set()
        await batch
        self.assertEqual(mock_scrape.await_count, 3)


class TestExistenceCheck(
    unittest.IsolatedAsyncioTestCase,
):

    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    async def test_existing_channel_uses_metadata_only(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = True
        mock_channel: MagicMock = MagicMock()
        mock_channel.subscriber_count = 1_000_000
        mock_typed.return_value = mock_channel
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        http_client: MagicMock = MagicMock()
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=http_client,
        )
        mock_typed.assert_awaited_once()
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertTrue(kwargs.get('metadata_only'))

    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    async def test_missing_channel_uses_full_scrape(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_channel: MagicMock = MagicMock()
        mock_channel.subscriber_count = 0
        mock_typed.return_value = mock_channel
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'bar'
        http_client: MagicMock = MagicMock()
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000001',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=http_client,
        )
        mock_typed.assert_awaited_once()
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertFalse(kwargs.get('metadata_only'))

    @patch(
        'tools.yt_channel_scrape'
        '._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape'
        '._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    async def test_existence_check_error_falls_back(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.side_effect = Exception(
            'network error',
        )
        mock_channel: MagicMock = MagicMock()
        mock_channel.subscriber_count = 500
        mock_typed.return_value = mock_channel
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'baz'
        http_client: MagicMock = MagicMock()
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000002',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=http_client,
        )
        mock_typed.assert_awaited_once()
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertFalse(kwargs.get('metadata_only'))
