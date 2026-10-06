'''
Unit tests for retiring a pooled InnerTube Web client after a
transport error in channel-tab browse calls.

httpcore 0.16.3 (pinned via innertube's httpx<0.24) leaves a failed
proxy tunnel in the connection pool; enough of them exhaust the pool
and every later request raises PoolTimeout. Retiring the client
generation that saw the error drops the leaked connections.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from scrape_exchange.youtube import youtube_channel_tabs as tabs_mod
from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs


CHANNEL_ID: str = 'UCtest123'
PROXY: str = 'http://proxy.test:3128'

_TABS_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel_tabs',
)
_TABS_LOGGER_PRIOR_LEVEL: int = _TABS_LOGGER.level


def setUpModule() -> None:
    _TABS_LOGGER.setLevel(logging.CRITICAL)


def tearDownModule() -> None:
    _TABS_LOGGER.setLevel(_TABS_LOGGER_PRIOR_LEVEL)


@patch(
    'scrape_exchange.youtube.youtube_channel_tabs'
    '.AsyncYouTubeClient._delay',
    new_callable=AsyncMock,
)
class TestBrowseRetiresPooledClient(unittest.IsolatedAsyncioTestCase):

    async def test_transport_error_retires_and_retries(
        self, mock_delay: AsyncMock,
    ) -> None:
        broken: MagicMock = MagicMock(name='broken')
        broken.browse.side_effect = httpx.PoolTimeout('pool full')
        fresh: MagicMock = MagicMock(name='fresh')
        fresh.browse.return_value = {'data': 'ok'}
        borrow: MagicMock = MagicMock(side_effect=[broken, fresh])
        release: AsyncMock = AsyncMock()
        retire: AsyncMock = AsyncMock(return_value=True)
        with patch.object(
            tabs_mod, 'borrow_pooled_innertube_for_entry', borrow,
        ), patch.object(
            tabs_mod, 'release_pooled_innertube', release,
        ), patch.object(
            tabs_mod, 'refresh_pooled_web_innertube_for_entry', retire,
        ):
            tabs: YouTubeChannelTabs = YouTubeChannelTabs(
                CHANNEL_ID, PROXY,
            )
            result: dict = await tabs._browse(max_retries=2)
        self.assertEqual(result, {'data': 'ok'})
        retire.assert_awaited_once_with(PROXY, challenged=broken)
        self.assertEqual(
            [c.args[0] for c in release.await_args_list],
            [broken, fresh],
        )

    async def test_non_transport_error_does_not_retire(
        self, mock_delay: AsyncMock,
    ) -> None:
        client: MagicMock = MagicMock()
        client.browse.side_effect = [ValueError('bad json'), {'ok': 1}]
        retire: AsyncMock = AsyncMock()
        with patch.object(
            tabs_mod, 'borrow_pooled_innertube_for_entry',
            MagicMock(return_value=client),
        ), patch.object(
            tabs_mod, 'release_pooled_innertube', AsyncMock(),
        ), patch.object(
            tabs_mod, 'refresh_pooled_web_innertube_for_entry', retire,
        ):
            tabs: YouTubeChannelTabs = YouTubeChannelTabs(
                CHANNEL_ID, PROXY,
            )
            await tabs._browse(max_retries=2)
        retire.assert_not_awaited()

    async def test_pinned_client_is_not_borrowed_or_retired(
        self, mock_delay: AsyncMock,
    ) -> None:
        borrow: MagicMock = MagicMock()
        retire: AsyncMock = AsyncMock()
        with patch.object(
            tabs_mod, 'borrow_pooled_innertube_for_entry', borrow,
        ), patch.object(
            tabs_mod, 'refresh_pooled_web_innertube_for_entry', retire,
        ):
            tabs: YouTubeChannelTabs = YouTubeChannelTabs(
                CHANNEL_ID, PROXY,
            )
            pinned: MagicMock = MagicMock()
            pinned.browse.side_effect = [
                httpx.ConnectError('down'), {'ok': 1},
            ]
            tabs.client = pinned
            await tabs._browse(max_retries=2)
        borrow.assert_not_called()
        retire.assert_not_awaited()
        self.assertEqual(pinned.browse.call_count, 2)


@patch(
    'scrape_exchange.youtube.youtube_channel_tabs'
    '.AsyncYouTubeClient._delay',
    new_callable=AsyncMock,
)
class TestBrowseNon429RequestError(unittest.IsolatedAsyncioTestCase):
    '''A non-429 InnerTube error is logged and waited on once per
    failed attempt.'''

    async def test_single_log_and_delay_per_attempt(
        self, mock_delay: AsyncMock,
    ) -> None:
        from innertube.errors import RequestError
        from innertube.models import Error

        tabs: YouTubeChannelTabs = YouTubeChannelTabs(CHANNEL_ID, PROXY)
        pinned: MagicMock = MagicMock()
        pinned.browse.side_effect = [
            RequestError(Error(500, 'boom', 'backendError')),
            {'ok': 1},
        ]
        tabs.client = pinned
        with self.assertLogs(_TABS_LOGGER, level='ERROR') as logs:
            result: dict = await tabs._browse(max_retries=2)
        self.assertEqual(result, {'ok': 1})
        self.assertEqual(mock_delay.await_count, 1)
        self.assertEqual(
            [r.getMessage() for r in logs.records],
            ['InnerTube BROWSE error'],
        )


class TestResolverCallRetiresPooledClient(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_transport_error_retires(self) -> None:
        from scrape_exchange.youtube import youtube_channel as yc
        client: MagicMock = MagicMock()
        retire: AsyncMock = AsyncMock()
        release: AsyncMock = AsyncMock()

        def call(c: MagicMock) -> dict:
            raise httpx.ConnectError('down')

        with patch.object(
            yc, 'borrow_pooled_innertube_for_entry',
            MagicMock(return_value=client),
        ), patch.object(
            yc, 'release_pooled_innertube', release,
        ), patch.object(
            yc, 'refresh_pooled_web_innertube_for_entry', retire,
        ):
            with self.assertRaises(httpx.ConnectError):
                await yc._resolver_innertube_call(PROXY, call)
            self.assertEqual(
                await yc._resolver_innertube_call(
                    PROXY, lambda c: {'ok': 1},
                ),
                {'ok': 1},
            )
        retire.assert_awaited_once_with(PROXY, challenged=client)
        self.assertEqual(release.await_count, 2)


if __name__ == '__main__':
    unittest.main()
