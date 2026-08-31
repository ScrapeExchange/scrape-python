'''Scrape-phase tests for the new queue path.'''

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.channel_scrape_queue import (
    ChannelState,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.channel_identity import (
    ChannelNotFoundError,
)
from scrape_exchange.watchdog import Watchdog


def _mock_settings() -> MagicMock:
    s = MagicMock()
    s.channel_unavailable_hard_threshold = 3
    s.channel_not_found_terminal_threshold = 3
    s.channel_not_found_retry_seconds = 3600
    s.exchange_url = 'https://scrape.exchange'
    return s


def _mock_channel(
    *,
    subscriber_count: int | None = 1_000_000,
    video_count: int | None = 1,
    title: str | None = 'Channel',
    channel_handle: str | None = 'channel',
) -> MagicMock:
    channel = MagicMock()
    channel.subscriber_count = subscriber_count
    channel.video_count = video_count
    channel.video_ids = {'video-id'}
    channel.channel_id = 'UCabc00000000000000000000'
    channel.channel_handle = channel_handle
    channel.title = title
    return channel


class TestChannelEndState(unittest.TestCase):

    def test_topic_wins_over_other_terminal_signals(self) -> None:
        from tools.yt_channel_scrape import _channel_end_state

        self.assertEqual(
            _channel_end_state(_mock_channel(
                subscriber_count=0,
                video_count=0,
                title='Example - Topic',
            )),
            ChannelState.TOPIC,
        )

    def test_unknown_counts_do_not_mark_terminal(self) -> None:
        from tools.yt_channel_scrape import _channel_end_state

        self.assertIsNone(_channel_end_state(_mock_channel(
            subscriber_count=None,
            video_count=None,
        )))


class TestScrapeOne(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_topic_handle_marks_invalid_without_scrape(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            queue = AsyncMock()
            creator_map = AsyncMock()
            creator_map.get.return_value = 'Artist - Topic'
            fm = AssetFileManagement(tmp)
            channel_id: str = 'UCabc00000000000000000000'
            from tools.yt_channel_scrape import (
                _scrape_one_queued,
            )
            with (
                patch(
                    'tools.yt_channel_scrape'
                    '._channel_exists_on_exchange',
                    new_callable=AsyncMock,
                ) as mock_exists,
                patch(
                    'tools.yt_channel_scrape'
                    '._do_scrape_channel_to_disk_typed',
                    new_callable=AsyncMock,
                ) as mock_scrape,
            ):
                await _scrape_one_queued(
                    channel_id,
                    queue=queue,
                    settings=_mock_settings(),
                    fm=fm,
                    creator_map_backend=creator_map,
                    http_client=MagicMock(),
                )

            invalid_path: Path = (
                Path(tmp)
                / f'channel-{channel_id}.json.br.invalid'
            )
            self.assertTrue(invalid_path.exists())
            self.assertEqual(invalid_path.read_bytes(), b'')
            mock_exists.assert_not_awaited()
            mock_scrape.assert_not_awaited()
            queue.mark.assert_awaited_once_with(
                f'i:{channel_id}',
                state=ChannelState.TOPIC,
                last_error='topic channel skipped',
                extra={'channel_handle': 'Artist - Topic'},
            )

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
        channel = _mock_channel(subscriber_count=1_000_000)
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
        queue.set_meta.assert_awaited_once_with(
            'i:UCabc00000000000000000000',
            subscriber_count='1000000',
        )

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
    async def test_missing_subscriber_count_schedules_full_retry(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = True
        mock_scrape.return_value = _mock_channel(
            subscriber_count=None,
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

        queue.retry_missing_subscriber_count.assert_awaited_once()
        retry_kwargs = (
            queue.retry_missing_subscriber_count.await_args.kwargs
        )
        self.assertEqual(
            retry_kwargs['channel_id'],
            'UCabc00000000000000000000',
        )
        self.assertIsInstance(retry_kwargs['now'], float)
        queue.update_tier.assert_not_awaited()
        queue.set_meta.assert_not_awaited()
        queue.clear_force_rescrape.assert_not_awaited()

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
    async def test_low_subscribers_marks_terminal(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.return_value = _mock_channel(
            subscriber_count=9,
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
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.LOW_SUBS,
        )
        queue.update_tier.assert_not_awaited()

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
    async def test_ten_subscribers_remains_schedulable(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.return_value = _mock_channel(
            subscriber_count=10,
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
        queue.mark.assert_not_awaited()
        queue.update_tier.assert_awaited_once()

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
    async def test_topic_marks_terminal(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.return_value = _mock_channel(
            subscriber_count=0,
            title='Example - Topic',
            video_count=0,
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'example-topic'
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
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.TOPIC,
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
    async def test_topic_handle_marks_terminal(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.return_value = _mock_channel(
            subscriber_count=10,
            title='Example',
            channel_handle='example-topic',
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'example-topic'
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
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.TOPIC,
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
    async def test_no_videos_marks_terminal(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_scrape.return_value = _mock_channel(
            subscriber_count=10,
            video_count=0,
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
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.NO_VIDEOS,
        )
        queue.update_tier.assert_not_awaited()

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
    async def test_topic_scrape_precedes_low_subs(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = 0
        channel.video_count = 0
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = 'artist-topic'
        channel.title = 'Artist'
        mock_scrape.return_value = channel
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'artist'
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
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.TOPIC,
        )

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
        (
            queue.mark_not_found_confirmed
            .assert_awaited_once_with(
                'i:UCabc00000000000000000000',
                last_error='404',
            )
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
    async def test_no_content_with_unknown_video_count_is_retryable(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = None
        channel.video_count = None
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = 'empty'
        channel.title = 'Empty Channel'
        from tools.yt_channel_scrape import (
            ChannelNoContentError,
            _scrape_one_queued,
        )
        mock_scrape.side_effect = ChannelNoContentError(
            'scraped but has no content',
            channel,
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'empty'
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark_not_found_confirmed.assert_not_awaited()
        queue.mark.assert_not_awaited()
        queue.mark_soft_unavailable.assert_awaited_once_with(
            'UCabc00000000000000000000',
            last_error='scraped but has no content',
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
    async def test_no_content_with_positive_video_count_is_retryable(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = None
        channel.video_count = 37
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = 'active'
        channel.title = 'Active Channel'
        from tools.yt_channel_scrape import (
            ChannelNoContentError,
            _scrape_one_queued,
        )
        mock_scrape.side_effect = ChannelNoContentError(
            'scraped but has no content',
            channel,
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'active'
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark_not_found_confirmed.assert_not_awaited()
        queue.mark.assert_not_awaited()
        queue.mark_soft_unavailable.assert_awaited_once_with(
            'UCabc00000000000000000000',
            last_error='scraped but has no content',
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
    async def test_no_content_with_zero_video_count_marks_no_videos(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = None
        channel.video_count = 0
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = 'empty'
        channel.title = 'Empty Channel'
        from tools.yt_channel_scrape import (
            ChannelNoContentError,
            _scrape_one_queued,
        )
        mock_scrape.side_effect = ChannelNoContentError(
            'scraped but has no content',
            channel,
        )
        queue = AsyncMock()
        creator_map = AsyncMock()
        creator_map.get.return_value = 'empty'
        await _scrape_one_queued(
            'UCabc00000000000000000000',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.mark_not_found_confirmed.assert_not_awaited()
        queue.mark.assert_awaited_once()
        self.assertEqual(
            queue.mark.await_args.kwargs['state'],
            ChannelState.NO_VIDEOS,
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
    async def test_terminal_page_message_stays_soft_unavailable(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        message: str = (
            'This channel was removed because it violated our '
            'Community Guidelines.'
        )
        mock_exists.return_value = False
        mock_scrape.side_effect = RuntimeError(message)
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
        queue.mark_soft_unavailable.assert_awaited_once_with(
            'UCabc00000000000000000000',
            last_error=message,
        )
        queue.mark.assert_not_awaited()

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
    async def test_no_handle_scrapes_by_id_not_unresolved(
        self,
        mock_scrape: AsyncMock,
        mock_exists: AsyncMock,
    ) -> None:
        # A missing creator_map handle no longer diverts the channel to
        # the terminal 'unresolved' state; it is scraped by channel_id.
        mock_exists.return_value = False
        channel = MagicMock()
        channel.subscriber_count = 5
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = None
        channel.title = None
        mock_scrape.return_value = channel
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
        mock_scrape.assert_awaited_once()
        for call in queue.mark.await_args_list:
            self.assertNotEqual(
                call.kwargs.get('state'),
                ChannelState.UNRESOLVED,
            )


class TestScrapeWorkers(
    unittest.IsolatedAsyncioTestCase,
):
    def setUp(self) -> None:
        self.watchdog = Watchdog(loop_timeout=0.0, work_timeout=0.0)
        self.watchdog.touch_work = MagicMock()
        Watchdog.set_instance(self.watchdog)

    def tearDown(self) -> None:
        Watchdog.reset()

    @patch(
        'tools.yt_channel_scrape._scrape_one_queued',
        new_callable=AsyncMock,
    )
    async def test_workers_keep_claiming_while_peer_is_slow(
        self,
        mock_scrape: AsyncMock,
    ) -> None:
        slow_started: asyncio.Event = asyncio.Event()
        fast_completed: asyncio.Event = asyncio.Event()
        unblock_slow: asyncio.Event = asyncio.Event()
        pop_count: int = 0

        async def pop_scheduled(
            count: int,
            *,
            now: float,
        ) -> list[str]:
            nonlocal pop_count
            self.assertEqual(count, 1)
            self.assertGreater(now, 0.0)
            pop_count += 1
            if pop_count == 1:
                return ['UCslow']
            if pop_count == 2:
                return ['UCfast']
            return []

        async def scrape_one(channel_id: str, **kwargs) -> None:
            if channel_id == 'UCslow':
                slow_started.set()
                await unblock_slow.wait()
                return
            if channel_id == 'UCfast':
                fast_completed.set()
                return
            self.fail(f'unexpected channel_id {channel_id}')

        mock_scrape.side_effect = scrape_one
        settings = _mock_settings()
        settings.channel_queue_idle_poll_seconds = 0.01
        shutdown_event: asyncio.Event = asyncio.Event()
        queue = AsyncMock()
        queue.pop_scheduled.side_effect = pop_scheduled
        active_started_at: dict[str, float] = {}
        completed_count: int = 0

        def record_completed() -> None:
            nonlocal completed_count
            completed_count += 1

        from tools.yt_channel_scrape import (
            _scrape_scheduled_worker,
        )
        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(
                _scrape_scheduled_worker(
                    worker_index=index,
                    queue=queue,
                    settings=settings,
                    fm=MagicMock(),
                    creator_map_backend=AsyncMock(),
                    http_client=MagicMock(),
                    shutdown_event=shutdown_event,
                    active_started_at=active_started_at,
                    record_completed=record_completed,
                )
            )
            for index in range(2)
        ]
        try:
            await slow_started.wait()
            await fast_completed.wait()
            self.assertEqual(completed_count, 1)
            self.assertIn('0:UCslow', active_started_at)
        finally:
            shutdown_event.set()
            unblock_slow.set()
            await asyncio.gather(*tasks)

        self.assertEqual(mock_scrape.await_count, 2)
        self.assertEqual(completed_count, 2)

    @patch(
        'tools.yt_channel_scrape._scrape_one_queued',
        new_callable=AsyncMock,
    )
    async def test_unexpected_scrape_error_marks_soft_unavailable(
        self,
        mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = RuntimeError('redis pool saturated')
        settings = _mock_settings()
        settings.channel_queue_idle_poll_seconds = 0.01
        shutdown_event: asyncio.Event = asyncio.Event()
        queue = AsyncMock()
        queue.pop_scheduled.return_value = ['UC1']
        active_started_at: dict[str, float] = {}

        def record_completed() -> None:
            shutdown_event.set()

        from tools.yt_channel_scrape import (
            _scrape_scheduled_worker,
        )

        await _scrape_scheduled_worker(
            worker_index=0,
            queue=queue,
            settings=settings,
            fm=MagicMock(),
            creator_map_backend=AsyncMock(),
            http_client=MagicMock(),
            shutdown_event=shutdown_event,
            active_started_at=active_started_at,
            record_completed=record_completed,
        )

        queue.mark_soft_unavailable.assert_awaited_once_with(
            'UC1',
            last_error='redis pool saturated',
        )
        self.assertGreaterEqual(
            self.watchdog.touch_work.call_count,
            1,
        )

    async def test_long_running_workers_report_progress(self) -> None:
        active_started_at: dict[str, float] = {
            '0:UCslow': 1.0,
        }
        shutdown_event: asyncio.Event = asyncio.Event()

        from tools.yt_channel_scrape import (
            _report_scrape_worker_progress,
        )

        with patch(
            'tools.yt_channel_scrape.'
            'CHANNEL_BATCH_PROGRESS_INTERVAL_SECONDS',
            0.01,
        ):
            with self.assertLogs(level='INFO') as logs:
                reporter: asyncio.Task[None] = asyncio.create_task(
                    _report_scrape_worker_progress(
                        active_started_at,
                        get_completed_count=lambda: 3,
                        worker_count=4,
                        shutdown_event=shutdown_event,
                    ),
                )
                await asyncio.sleep(0.03)
                shutdown_event.set()
                await reporter

        self.assertIn(
            'channel scrape worker progress',
            '\n'.join(logs.output),
        )
        self.assertGreaterEqual(
            self.watchdog.touch_work.call_count,
            1,
        )

    async def test_worker_count_keeps_existing_proxy_floor(
        self,
    ) -> None:
        settings = _mock_settings()
        settings.channel_concurrency = 2
        settings.proxies = ['p1', 'p2', 'p3']
        from tools.yt_channel_scrape import (
            _channel_scrape_worker_count,
        )

        self.assertEqual(
            _channel_scrape_worker_count(settings),
            3,
        )


class TestTypedScrape(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_success_metric_uses_channel_status(self) -> None:
        channel: MagicMock = MagicMock()
        channel.channel_id = 'UCabc00000000000000000000'
        channel.channel_handle = 'channel'
        channel.about_page_succeeded = True
        settings: MagicMock = _mock_settings()
        settings.channel_data_directory = '/tmp'
        from tools.yt_channel_scrape import (
            _do_scrape_channel_to_disk_typed,
        )

        with (
            patch(
                'tools.yt_channel_scrape.YouTubeChannel',
                return_value=channel,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._try_scrape_channel_typed',
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._reject_topic_channel_if_needed',
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape._persist_scraped_channel',
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                'tools.yt_channel_scrape.METRIC_CHANNELS_SCRAPED',
            ) as completed,
            patch(
                'tools.yt_channel_scrape.METRIC_SCRAPE_DURATION',
            ),
        ):
            await _do_scrape_channel_to_disk_typed(
                settings,
                MagicMock(),
                'channel',
                'channel-UCabc00000000000000000000.json.br',
                {
                    'channel_id': 'UCabc00000000000000000000',
                    'channel_status': 'existing',
                },
                metadata_only=True,
            )

        self.assertEqual(
            completed.labels.call_args.kwargs['channel_status'],
            'existing',
        )

    @patch(
        'tools.yt_channel_scrape._persist_scraped_channel',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape._channel_has_no_content',
        return_value=True,
    )
    @patch(
        'tools.yt_channel_scrape._try_scrape_channel_typed',
        new_callable=AsyncMock,
    )
    async def test_no_content_raises_distinct_error(
        self,
        mock_try: AsyncMock,
        mock_no_content: MagicMock,
        mock_persist: AsyncMock,
    ) -> None:
        mock_try.return_value = None
        settings = _mock_settings()
        settings.channel_data_directory = '/tmp'
        from tools.yt_channel_scrape import (
            ChannelNoContentError,
            _do_scrape_channel_to_disk_typed,
        )
        with self.assertRaises(ChannelNoContentError):
            await _do_scrape_channel_to_disk_typed(
                settings,
                MagicMock(),
                'empty',
                'channel-UCabc00000000000000000000.json.br',
                {'channel_id': 'UCabc00000000000000000000'},
                metadata_only=False,
            )
        mock_no_content.assert_called_once()
        mock_persist.assert_not_awaited()


class TestExistenceCheck(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_not_found_response_returns_missing(self) -> None:
        http_client: AsyncMock = AsyncMock()
        response: MagicMock = MagicMock()
        response.status_code = 404
        http_client.get.return_value = response
        from tools.yt_channel_scrape import (
            _channel_exists_on_exchange,
        )

        result: bool | None = await _channel_exists_on_exchange(
            http_client,
            'https://api.scrape.exchange',
            'UCabc00000000000000000000',
        )

        self.assertIs(result, False)

    async def test_transport_error_returns_unknown(self) -> None:
        http_client: AsyncMock = AsyncMock()
        http_client.get.side_effect = OSError('network error')
        from tools.yt_channel_scrape import (
            _channel_exists_on_exchange,
        )

        result: bool | None = await _channel_exists_on_exchange(
            http_client,
            'https://api.scrape.exchange',
            'UCabc00000000000000000000',
        )

        self.assertIsNone(result)

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
        extra: dict[str, str] = mock_typed.await_args.args[4]
        self.assertEqual(extra['channel_status'], 'existing')

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
        self.assertEqual(
            mock_typed.await_args.args[3],
            'channel-UCabc00000000000000000001.json.br',
        )
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertFalse(kwargs.get('metadata_only'))
        extra: dict[str, str] = mock_typed.await_args.args[4]
        self.assertEqual(extra['channel_status'], 'new')

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
    async def test_unknown_existence_falls_back_to_full_scrape(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = None
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
        extra: dict[str, str] = mock_typed.await_args.args[4]
        self.assertEqual(extra['channel_status'], 'unknown')


class TestForceRescrapeMode(
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
    async def test_full_mode_checks_existence_for_label(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = True
        mock_channel: MagicMock = MagicMock()
        mock_channel.subscriber_count = 250
        mock_typed.return_value = mock_channel
        queue: AsyncMock = AsyncMock()
        queue.get_meta.return_value = {
            'force_rescrape_mode': 'full',
        }
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000003',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        mock_exists.assert_awaited_once()
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertFalse(kwargs.get('metadata_only'))
        extra: dict[str, str] = mock_typed.await_args.args[4]
        self.assertEqual(extra['channel_status'], 'existing')
        queue.clear_force_rescrape.assert_awaited_once_with(
            'i:UCabc00000000000000000003',
        )

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
    async def test_metadata_mode_checks_existence_for_label(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = False
        mock_channel: MagicMock = MagicMock()
        mock_channel.subscriber_count = 250
        mock_typed.return_value = mock_channel
        queue: AsyncMock = AsyncMock()
        queue.get_meta.return_value = {
            'force_rescrape_mode': 'metadata',
        }
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000004',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        mock_exists.assert_awaited_once()
        kwargs: dict = mock_typed.await_args.kwargs
        self.assertTrue(kwargs.get('metadata_only'))
        extra: dict[str, str] = mock_typed.await_args.args[4]
        self.assertEqual(extra['channel_status'], 'new')
        queue.clear_force_rescrape.assert_awaited_once_with(
            'i:UCabc00000000000000000004',
        )

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
    async def test_transient_failure_keeps_force_metadata(
        self,
        mock_exists: AsyncMock,
        mock_typed: AsyncMock,
    ) -> None:
        mock_exists.return_value = True
        mock_typed.side_effect = RuntimeError('timeout')
        queue: AsyncMock = AsyncMock()
        queue.get_meta.return_value = {
            'force_rescrape_mode': 'full',
        }
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        from tools.yt_channel_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'UCabc00000000000000000005',
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        mock_exists.assert_awaited_once()
        queue.clear_force_rescrape.assert_not_called()


class TestScrapeOneHandleOptional(
    unittest.IsolatedAsyncioTestCase,
):
    '''A missing creator-map handle permits scraping by channel ID.

    The scrape must discover a canonical handle before persistence;
    successful discovery self-heals the identity and name maps.
    '''

    def _patches(self):
        return (
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock, return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._do_scrape_channel_to_disk_typed',
                new_callable=AsyncMock,
            ),
        )

    async def test_missing_handle_moves_to_soft_unavailable(self) -> None:
        channel_id: str = 'UCabc00000000000000000000'
        channel: MagicMock = MagicMock()
        channel.subscriber_count = 10
        channel.video_count = 1
        channel.channel_id = channel_id
        channel.channel_handle = None
        channel.title = None
        channel.about_page_succeeded = True
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = None
        from tools.yt_channel_scrape import _scrape_one_queued

        with (
            patch(
                'tools.yt_channel_scrape._channel_exists_on_exchange',
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape.YouTubeChannel',
                return_value=channel,
            ),
            patch(
                'tools.yt_channel_scrape._try_scrape_channel_typed',
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._reject_topic_channel_if_needed',
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape._channel_has_no_content',
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape._persist_scraped_channel',
                new_callable=AsyncMock,
                return_value=True,
            ) as persist,
        ):
            await _scrape_one_queued(
                channel_id,
                queue=queue,
                settings=_mock_settings(),
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
            )

        persist.assert_not_awaited()
        queue.mark_soft_unavailable.assert_awaited_once_with(
            channel_id,
            last_error=(
                f'channel {channel_id!r} scraped without a '
                'channel_handle'
            ),
        )
        queue.update_tier.assert_not_awaited()

    async def test_self_heal_binds_and_name_maps(self) -> None:
        p_exists, p_scrape = self._patches()
        with p_exists, p_scrape as mock_scrape:
            channel = MagicMock()
            channel.subscriber_count = 10
            channel.channel_id = 'UCabc00000000000000000000'
            channel.channel_handle = 'discovered'
            channel.title = 'Discovered Title'
            mock_scrape.return_value = channel
            queue = AsyncMock()
            creator_map = AsyncMock()
            creator_map.get.return_value = None
            identity = AsyncMock()
            name_map = AsyncMock()
            from tools.yt_channel_scrape import _scrape_one_queued
            await _scrape_one_queued(
                'UCabc00000000000000000000',
                queue=queue,
                settings=_mock_settings(),
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
                identity=identity,
                name_map=name_map,
            )
            identity.bind.assert_awaited_once_with(
                'UCabc00000000000000000000', 'discovered',
            )
            name_map.put.assert_awaited_once_with(
                'Discovered Title', 'UCabc00000000000000000000',
            )

    async def test_self_heal_bind_inconsistent_swallowed(
        self,
    ) -> None:
        from scrape_exchange.youtube.channel_identity import (
            InconsistentIdentityError,
        )
        p_exists, p_scrape = self._patches()
        with p_exists, p_scrape as mock_scrape:
            channel = MagicMock()
            channel.subscriber_count = 10
            channel.channel_id = 'UCabc00000000000000000000'
            channel.channel_handle = 'discovered'
            channel.title = None
            mock_scrape.return_value = channel
            queue = AsyncMock()
            creator_map = AsyncMock()
            creator_map.get.return_value = None
            identity = AsyncMock()
            identity.bind.side_effect = InconsistentIdentityError('x')
            from tools.yt_channel_scrape import _scrape_one_queued
            await _scrape_one_queued(
                'UCabc00000000000000000000',
                queue=queue,
                settings=_mock_settings(),
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
                identity=identity,
                name_map=AsyncMock(),
            )
            # Bind raised, but the scrape still counts as success.
            queue.update_tier.assert_awaited_once()
