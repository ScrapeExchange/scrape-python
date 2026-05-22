'''Unit test that ``process_channel`` honors the circuit-breaker
contract: set_no_feeds() is suppressed when
``report.suppress_channel_failure`` is True, and the rollback
list is applied.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube._rss_circuit_state import (
    CircuitReport,
    CircuitState,
    CircuitTransition,
)


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestProcessChannelCircuitContract(unittest.TestCase):
    '''Drives the report→suppress→rollback path with a stub
    breaker. The actual fetch_rss is mocked out to return a
    404 ValueError.'''

    def test_404_with_suppress_does_not_call_set_no_feeds(
        self,
    ) -> None:
        from tools import yt_rss_scrape

        creator_queue = MagicMock()
        creator_queue.has_had_feed = AsyncMock(return_value=True)
        creator_queue.get_no_feeds = AsyncMock(return_value=None)
        creator_queue.set_no_feeds = AsyncMock()
        creator_queue.rollback_no_feeds = AsyncMock()
        creator_queue.update_tier = AsyncMock()
        creator_queue.mark_had_feed = AsyncMock()
        creator_queue.clear_no_feeds = AsyncMock()

        breaker = MagicMock()
        breaker.acquire = AsyncMock(return_value=0.0)
        suppressed_state = CircuitState(
            mode='regular', is_open=True,
            open_until_ts=9999.0, current_cooldown_s=60,
            consecutive_404s=0, consecutive_successes=0,
        )
        breaker.report = AsyncMock(return_value=CircuitReport(
            transition=CircuitTransition(
                from_state='closed-regular',
                to_state='open-regular',
                cooldown_seconds=60,
            ),
            suppress_channel_failure=True,
            rollback_channel_ids=['UCA', 'UCB'],
            state_after=suppressed_state,
        ))

        with patch.object(
            yt_rss_scrape, '_fetch_rss_safe',
            new=AsyncMock(return_value=ValueError(
                'RSS feed not found',
            )),
        ), patch.object(
            yt_rss_scrape, 'update_channel',
            new=AsyncMock(return_value=(True, 0, None)),
        ), patch.object(
            yt_rss_scrape, '_get_rss_circuit_breaker',
            return_value=breaker,
        ):
            settings = MagicMock()
            settings.rss_max_no_feed_failures = 10
            settings.rss_max_no_feed_failures_had_feed = 50
            settings.video_data_directory = '/tmp/x'

            client = MagicMock()
            creator_map = MagicMock()
            creator_map.get = AsyncMock(return_value='handle')
            name_map = MagicMock()
            channel_validator = MagicMock()
            video_queue = MagicMock()

            result = _run(yt_rss_scrape.process_channel(
                channel_handle='handle',
                channel_id='UCX',
                client=client,
                creator_queue=creator_queue,
                settings=settings,
                creator_map_backend=creator_map,
                name_map_backend=name_map,
                channel_validator=channel_validator,
                tier=1,
                video_queue=video_queue,
            ))

            # set_no_feeds suppressed.
            creator_queue.set_no_feeds.assert_not_called()
            # Rollback applied for the F-1 channels.
            self.assertEqual(
                creator_queue.rollback_no_feeds.await_count, 2,
            )
            creator_queue.rollback_no_feeds.assert_any_await(
                'UCA',
            )
            creator_queue.rollback_no_feeds.assert_any_await(
                'UCB',
            )
            # Outcome: transient failure → channel stays in
            # queue for retry. The breaker, not set_no_feeds,
            # provides backoff.
            self.assertIs(result, False)


class TestProcessChannelNon404Errors(unittest.TestCase):
    '''The breaker must NOT be contacted for 5xx, network, or
    timeout errors per the spec — only 404 (ValueError) drives
    the breaker.'''

    def _build_mocks(self, rss_result):
        creator_queue = MagicMock()
        creator_queue.has_had_feed = AsyncMock(return_value=True)
        creator_queue.get_no_feeds = AsyncMock(return_value=None)
        creator_queue.set_no_feeds = AsyncMock()
        creator_queue.rollback_no_feeds = AsyncMock()
        creator_queue.update_tier = AsyncMock()
        creator_queue.mark_had_feed = AsyncMock()
        creator_queue.clear_no_feeds = AsyncMock()

        breaker = MagicMock()
        breaker.acquire = AsyncMock(return_value=0.0)
        breaker.report = AsyncMock()
        return creator_queue, breaker, rss_result

    def _run_process(self, creator_queue, breaker, rss_result):
        from tools import yt_rss_scrape
        with patch.object(
            yt_rss_scrape, '_fetch_rss_safe',
            new=AsyncMock(return_value=rss_result),
        ), patch.object(
            yt_rss_scrape, 'update_channel',
            new=AsyncMock(return_value=(True, 0, None)),
        ), patch.object(
            yt_rss_scrape, '_get_rss_circuit_breaker',
            return_value=breaker,
        ):
            settings = MagicMock()
            settings.rss_max_no_feed_failures = 10
            settings.rss_max_no_feed_failures_had_feed = 50
            settings.video_data_directory = '/tmp/x'

            client = MagicMock()
            creator_map = MagicMock()
            creator_map.get = AsyncMock(return_value='handle')
            name_map = MagicMock()
            channel_validator = MagicMock()
            video_queue = MagicMock()

            return _run(yt_rss_scrape.process_channel(
                channel_handle='handle',
                channel_id='UCX',
                client=client,
                creator_queue=creator_queue,
                settings=settings,
                creator_map_backend=creator_map,
                name_map_backend=name_map,
                channel_validator=channel_validator,
                tier=1,
                video_queue=video_queue,
            ))

    def test_5xx_does_not_contact_breaker(self) -> None:
        creator_queue, breaker, rss_result = self._build_mocks(
            RuntimeError('Server error fetching RSS feed'),
        )
        result = self._run_process(
            creator_queue, breaker, rss_result,
        )
        breaker.report.assert_not_called()
        creator_queue.set_no_feeds.assert_not_called()
        creator_queue.rollback_no_feeds.assert_not_called()
        # 5xx returns True (channel reschedules normally).
        self.assertIs(result, True)

    def test_network_error_does_not_contact_breaker(self) -> None:
        # A bare Exception simulates a network/timeout error
        # surfaced by _fetch_rss_safe.
        creator_queue, breaker, rss_result = self._build_mocks(
            Exception('Connection reset by peer'),
        )
        result = self._run_process(
            creator_queue, breaker, rss_result,
        )
        breaker.report.assert_not_called()
        # Pre-breaker behavior preserved: set_no_feeds called.
        creator_queue.set_no_feeds.assert_called_once()
        creator_queue.rollback_no_feeds.assert_not_called()
        self.assertIs(result, False)


class TestStateGaugeReaper(unittest.TestCase):

    def test_reaper_publishes_state_gauges(self) -> None:
        from tools import yt_rss_scrape
        from scrape_exchange.scraper_metrics import (
            METRIC_RSS_CIRCUIT_STATE,
            METRIC_RSS_CIRCUIT_OPEN_SECONDS,
        )

        breaker = MagicMock()
        breaker._backend = MagicMock()
        breaker._backend.read_state = AsyncMock(
            return_value=CircuitState(
                mode='impaired',
                is_open=False,
                open_until_ts=0.0,
                current_cooldown_s=240,
                consecutive_404s=0,
                consecutive_successes=5,
            ),
        )

        async def run() -> None:
            await yt_rss_scrape._publish_circuit_gauges_once(
                breaker,
            )

        _run(run())

        # State gauge: closed-impaired = 1, others = 0.
        v: float = METRIC_RSS_CIRCUIT_STATE.labels(
            platform='youtube', state='closed-impaired',
        )._value.get()
        self.assertEqual(v, 1.0)
        v = METRIC_RSS_CIRCUIT_STATE.labels(
            platform='youtube', state='open-impaired',
        )._value.get()
        self.assertEqual(v, 0.0)
        # Open-seconds gauge tracks current_cooldown_s.
        v = METRIC_RSS_CIRCUIT_OPEN_SECONDS.labels(
            platform='youtube',
        )._value.get()
        self.assertEqual(v, 240.0)


if __name__ == '__main__':
    unittest.main()
