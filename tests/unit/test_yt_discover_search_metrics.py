'''
Unit tests for the Prometheus metrics of tools/yt_discover_search.py.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest
from typing import Any
from unittest import mock

import httpx
from prometheus_client import REGISTRY

from tools import yt_discover_search as ds
from tools.yt_discover_search import (
    POPULARITY_CHANNEL_PARAMS,
    POPULARITY_VIDEO_PARAMS,
    DiscoverSearchSettings,
    DiscoveredChannel,
)


def _value(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _pages(market: str, facet: str, page_type: str, outcome: str) -> float:
    return _value('discover_search_pages_total', {
        'platform': 'youtube', 'market': market, 'facet': facet,
        'page_type': page_type, 'outcome': outcome,
    })


class TestFacetLabel(unittest.TestCase):

    def test_labels(self) -> None:
        self.assertEqual(
            ds._facet_label(POPULARITY_CHANNEL_PARAMS), 'channel',
        )
        self.assertEqual(ds._facet_label(POPULARITY_VIDEO_PARAMS), 'video')
        self.assertEqual(ds._facet_label('xyz'), 'other')
        self.assertEqual(ds._facet_label(None), 'other')


class TestSettings(unittest.TestCase):

    def test_metrics_port_default_and_env(self) -> None:
        with mock.patch.dict('os.environ', {}, clear=True):
            settings: DiscoverSearchSettings = DiscoverSearchSettings(
                _cli_parse_args=[], _env_file=None,
            )
        self.assertEqual(settings.metrics_port, 9550)
        with mock.patch.dict(
            'os.environ', {'DISCOVER_METRICS_PORT': '9551'}, clear=True,
        ):
            settings = DiscoverSearchSettings(
                _cli_parse_args=[], _env_file=None,
            )
        self.assertEqual(settings.metrics_port, 9551)


class TestStartMetricsServer(unittest.TestCase):

    def test_starts_and_tolerates_port_in_use(self) -> None:
        with mock.patch.object(ds, 'start_metrics_server') as start:
            ds._start_metrics(9550)
            start.assert_called_once_with(9550)
            start.side_effect = OSError('in use')
            ds._start_metrics(9550)


class TestPageMetrics(unittest.IsolatedAsyncioTestCase):

    async def test_pages_and_channels_are_counted(self) -> None:
        payloads: list[dict[str, Any] | None] = [{'page': 1}, None]
        channel: DiscoveredChannel = mock.Mock(spec=DiscoveredChannel)
        before_ok: float = _pages('ZZ', 'channel', 'initial', 'success')
        before_fail: float = _pages(
            'ZZ', 'channel', 'continuation', 'failed',
        )
        before_found: float = _value(
            'discover_channels_found_total',
            {'platform': 'youtube', 'facet': 'channel'},
        )

        async def page(*args: object, **kwargs: object) -> Any:
            return payloads.pop(0)

        with mock.patch.object(
            ds, '_search_page_localized_with_retry', side_effect=page,
        ), mock.patch.object(
            ds, 'extract_channels', return_value=[channel, channel],
        ), mock.patch.object(
            ds, '_get_continuation_token', return_value='next',
        ):
            found: list[DiscoveredChannel] = [
                c async for c in ds.discover_popular_for_market(
                    'term', params=POPULARITY_CHANNEL_PARAMS,
                    gl='ZZ', hl='en', continuations=3,
                    limiter=mock.Mock(), proxy='http://p.test:3128',
                )
            ]
        self.assertEqual(len(found), 2)
        self.assertEqual(
            _pages('ZZ', 'channel', 'initial', 'success'), before_ok + 1,
        )
        self.assertEqual(
            _pages('ZZ', 'channel', 'continuation', 'failed'),
            before_fail + 1,
        )
        self.assertEqual(
            _value(
                'discover_channels_found_total',
                {'platform': 'youtube', 'facet': 'channel'},
            ),
            before_found + 2,
        )


class TestErrorAndLatencyMetrics(unittest.IsolatedAsyncioTestCase):

    async def test_transient_errors_are_counted(self) -> None:
        labels: dict[str, str] = {
            'platform': 'youtube', 'error_type': 'ConnectError',
        }
        before: float = _value('discover_search_errors_total', labels)
        calls: list[int] = []

        async def fetch(proxy: str | None) -> dict[str, Any]:
            calls.append(1)
            if len(calls) == 1:
                raise httpx.ConnectError('down')
            return {'ok': 1}

        limiter: mock.Mock = mock.Mock()
        limiter.report_proxy_result = mock.AsyncMock()
        with mock.patch.object(
            ds.YouTubeRateLimiter, 'get', return_value=limiter,
        ), mock.patch.object(ds.asyncio, 'sleep', mock.AsyncMock()):
            result: dict[str, Any] | None = await ds._fetch_page_with_retry(
                fetch, proxy='http://p.test:3128', proxy_lease=None,
                log_extra={},
            )
        self.assertEqual(result, {'ok': 1})
        self.assertEqual(
            _value('discover_search_errors_total', labels), before + 1,
        )

    async def test_search_latency_is_observed(self) -> None:
        labels: dict[str, str] = {
            'platform': 'youtube', 'page_type': 'initial',
        }
        before: float = _value(
            'discover_search_duration_seconds_count', labels,
        )
        limiter: mock.Mock = mock.Mock()
        limiter.acquire = mock.AsyncMock()
        client: mock.Mock = mock.Mock()
        client.search.return_value = {'ok': 1}

        async def run(fn: Any) -> Any:
            return fn()

        with mock.patch.object(
            ds, 'pooled_innertube_localized_for_entry', return_value=client,
        ), mock.patch.object(
            ds, 'run_on_innertube_executor', side_effect=run,
        ):
            await ds._innertube_search_localized(
                'term', params=None, continuation=None,
                proxy='http://p.test:3128', gl='US', hl='en',
                limiter=limiter,
            )
        self.assertEqual(
            _value('discover_search_duration_seconds_count', labels),
            before + 1,
        )


class TestEnqueueAndJobMetrics(unittest.TestCase):

    def test_enqueue_outcomes_are_counted(self) -> None:
        enqueued: dict[str, str] = {
            'platform': 'youtube', 'outcome': 'enqueued',
        }
        known: dict[str, str] = {
            'platform': 'youtube', 'outcome': 'already_scraped',
        }
        before_enqueued: float = _value(
            'discover_enqueue_outcomes_total', enqueued,
        )
        before_known: float = _value(
            'discover_enqueue_outcomes_total', known,
        )
        ds._record_enqueue_outcomes(
            {'enqueued': 3, 'already_scraped': 2, 'on_exchange': 0},
        )
        self.assertEqual(
            _value('discover_enqueue_outcomes_total', enqueued),
            before_enqueued + 3,
        )
        self.assertEqual(
            _value('discover_enqueue_outcomes_total', known),
            before_known + 2,
        )


class TestHttpStatusMetric(unittest.IsolatedAsyncioTestCase):

    async def test_innertube_http_errors_counted_by_status(self) -> None:
        from innertube.errors import RequestError
        from innertube.models import Error

        labels: dict[str, str] = {'platform': 'youtube', 'status': '429'}
        before: float = _value('discover_search_http_errors_total', labels)
        calls: list[int] = []

        async def fetch(proxy: str | None) -> dict[str, Any]:
            calls.append(1)
            if len(calls) == 1:
                raise RequestError(Error(429, 'slow down', 'rateLimited'))
            return {'ok': 1}

        limiter: mock.Mock = mock.Mock()
        limiter.report_proxy_result = mock.AsyncMock()
        with mock.patch.object(
            ds.YouTubeRateLimiter, 'get', return_value=limiter,
        ), mock.patch.object(ds.asyncio, 'sleep', mock.AsyncMock()):
            await ds._fetch_page_with_retry(
                fetch, proxy='http://p.test:3128', proxy_lease=None,
                log_extra={},
            )
        self.assertEqual(
            _value('discover_search_http_errors_total', labels), before + 1,
        )


if __name__ == '__main__':
    unittest.main()
