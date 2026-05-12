'''Tests for scrape_exchange.proxy_phase_metrics.

Covers both paths:

* The RSS trace callback (httpcore trace extension) — invoked
  directly with httpcore-style event names.
* The HTML curl-info reader — invoked with a fake
  ``httpx.Response`` whose ``extensions['curl']['infos']``
  matches the shape ``httpx_curl_cffi`` emits.

Both share the same three histograms keyed by
``(call_type, proxy_file, proxy_network)``. Histograms are
module-level singletons, so the tests check deltas rather
than absolute counts to remain order-independent.
'''

import asyncio
import unittest
from unittest import mock

import httpx
from curl_cffi import CurlInfo

from scrape_exchange.proxy_phase_metrics import (
    METRIC_PROXY_HTTP_CONNECT_SECONDS,
    METRIC_PROXY_REQUESTS,
    METRIC_PROXY_TCP_CONNECT_SECONDS,
    METRIC_PROXY_TLS_HANDSHAKE_SECONDS,
    install_innertube_phase_tracing,
    make_innertube_phase_trace,
    make_rss_phase_trace,
    record_html_phase_metrics,
)


# Tests run under the python interpreter directly so
# _SCRAPER_LABEL resolves to whatever sys.argv[0] points at
# (e.g. 'unittest' → 'unknown'). Read it back instead of
# hard-coding to keep the assertions robust.
from scrape_exchange.proxy_phase_metrics import (
    _SCRAPER_LABEL as _EXPECTED_SCRAPER,
)


def _counter_value(
    metric,
    *,
    call_type: str,
    proxy_file: str,
    reused: str,
    scraper: str = _EXPECTED_SCRAPER,
) -> float:
    target: dict[str, str] = {
        'scraper': scraper,
        'call_type': call_type,
        'proxy_file': proxy_file,
        'reused': reused,
    }
    for family in metric.collect():
        for sample in family.samples:
            if (
                sample.name.endswith('_total')
                and sample.labels == target
            ):
                return float(sample.value)
    return 0.0


def _hist_count(
    metric,
    *,
    call_type: str,
    proxy_file: str,
    scraper: str = _EXPECTED_SCRAPER,
) -> int:
    target: dict[str, str] = {
        'scraper': scraper,
        'call_type': call_type,
        'proxy_file': proxy_file,
    }
    for family in metric.collect():
        for sample in family.samples:
            if (
                sample.name.endswith('_count')
                and sample.labels == target
            ):
                return int(sample.value)
    return 0


def _hist_sum(
    metric,
    *,
    call_type: str,
    proxy_file: str,
    scraper: str = _EXPECTED_SCRAPER,
) -> float:
    target: dict[str, str] = {
        'scraper': scraper,
        'call_type': call_type,
        'proxy_file': proxy_file,
    }
    for family in metric.collect():
        for sample in family.samples:
            if (
                sample.name.endswith('_sum')
                and sample.labels == target
            ):
                return float(sample.value)
    return 0.0


class TestRssPhaseTrace(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_full_sequence_emits_all_three(
        self,
    ) -> None:
        labels: dict[str, str] = {
            'call_type': 'rss',
            'proxy_file': 'hype.proxies',
        }
        before_tcp: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_conn: int = _hist_count(
            METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
        )
        before_tls: int = _hist_count(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )

        trace = make_rss_phase_trace(
            proxy_file='hype.proxies',
        )
        await trace(
            'connection.connect_tcp.started', {'host': 'p'},
        )
        await asyncio.sleep(0.001)
        await trace(
            'connection.connect_tcp.complete', {},
        )
        await asyncio.sleep(0.001)
        await trace(
            'connection.start_tls.started', {},
        )
        await asyncio.sleep(0.001)
        await trace(
            'connection.start_tls.complete', {},
        )

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
            ),
            before_conn + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls + 1,
        )

    async def test_tcp_only_when_start_tls_never_fires(
        self,
    ) -> None:
        '''Mid-flight CONNECT failure leaves the sequence
        without start_tls events. Only the TCP phase
        should be observed.'''
        labels: dict[str, str] = {
            'call_type': 'rss',
            'proxy_file': 'partial',
        }
        before_tcp: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_conn: int = _hist_count(
            METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
        )

        trace = make_rss_phase_trace(
            proxy_file='partial',
        )
        await trace('connection.connect_tcp.started', {})
        await asyncio.sleep(0.001)
        await trace('connection.connect_tcp.complete', {})

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
            ),
            before_conn,
        )

    async def test_keepalive_reuse_records_nothing(
        self,
    ) -> None:
        '''Keep-alive reuse skips connect_tcp / start_tls
        events. A trace seeing only request/response
        events records no samples.'''
        labels: dict[str, str] = {
            'call_type': 'rss',
            'proxy_file': 'reused',
        }
        before_tcp: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_tls: int = _hist_count(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )
        trace = make_rss_phase_trace(
            proxy_file='reused',
        )
        await trace(
            'http11.send_request_headers.started', {},
        )
        await trace(
            'http11.send_request_headers.complete', {},
        )
        await trace(
            'http11.receive_response_headers.started', {},
        )
        await trace(
            'http11.receive_response_headers.complete', {},
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls,
        )


def _response_with_curl_infos(
    infos: dict[int, float],
) -> httpx.Response:
    '''Build a minimal httpx.Response carrying *infos* in
    the ``curl`` extension, matching the wire shape
    produced by ``httpx_curl_cffi``.
    '''
    resp: httpx.Response = httpx.Response(
        status_code=200, content=b'',
    )
    resp.extensions['curl'] = {
        'infos': infos, 'response': mock.MagicMock(),
    }
    return resp


class TestRecordHtmlPhaseMetrics(unittest.TestCase):

    def test_fresh_connection_emits_tcp_and_tls(
        self,
    ) -> None:
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'hype.proxies',
        }
        before_tcp_count: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_tls_count: int = _hist_count(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )
        before_tcp_sum: float = _hist_sum(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_tls_sum: float = _hist_sum(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )
        resp: httpx.Response = _response_with_curl_infos({
            int(CurlInfo.NAMELOOKUP_TIME): 0.005,
            int(CurlInfo.CONNECT_TIME): 0.110,
            int(CurlInfo.APPCONNECT_TIME): 1.250,
        })

        record_html_phase_metrics(
            resp,
            proxy_file='hype.proxies',
        )

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp_count + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls_count + 1,
        )
        # TCP delta = CONNECT - NAMELOOKUP = 0.105s
        # TLS delta = APPCONNECT - CONNECT = 1.140s
        self.assertAlmostEqual(
            _hist_sum(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp_sum + 0.105,
            places=4,
        )
        self.assertAlmostEqual(
            _hist_sum(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls_sum + 1.140,
            places=4,
        )

    def test_keepalive_reuse_emits_nothing(self) -> None:
        '''curl reports 0 for CONNECT_TIME and
        APPCONNECT_TIME when the connection was reused
        from the pool.'''
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'reused_html',
        }
        before: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        resp: httpx.Response = _response_with_curl_infos({
            int(CurlInfo.NAMELOOKUP_TIME): 0.0,
            int(CurlInfo.CONNECT_TIME): 0.0,
            int(CurlInfo.APPCONNECT_TIME): 0.0,
        })

        record_html_phase_metrics(
            resp,
            proxy_file='reused_html',
        )

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before,
        )

    def test_missing_curl_extension_is_noop(self) -> None:
        '''A response built without the curl extension
        (e.g. a vanilla httpx.AsyncClient call or a mock
        response in another test) must not crash.'''
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'plain',
        }
        before: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        resp: httpx.Response = httpx.Response(
            status_code=200, content=b'',
        )
        record_html_phase_metrics(
            resp,
            proxy_file='plain',
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before,
        )

    def test_tls_skipped_when_appconnect_missing(
        self,
    ) -> None:
        '''A plain-HTTP request has APPCONNECT_TIME=0. TCP
        still fires; TLS does not.'''
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'plain_http',
        }
        before_tcp: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_tls: int = _hist_count(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )
        resp: httpx.Response = _response_with_curl_infos({
            int(CurlInfo.NAMELOOKUP_TIME): 0.001,
            int(CurlInfo.CONNECT_TIME): 0.050,
            int(CurlInfo.APPCONNECT_TIME): 0.0,
        })

        record_html_phase_metrics(
            resp,
            proxy_file='plain_http',
        )

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls,
        )


class TestInnerTubePhaseTrace(unittest.TestCase):

    def test_sync_callback_emits_three_phases(self) -> None:
        labels: dict[str, str] = {
            'call_type': 'innertube',
            'proxy_file': 'it.proxies',
        }
        before_tcp: int = _hist_count(
            METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
        )
        before_conn: int = _hist_count(
            METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
        )
        before_tls: int = _hist_count(
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
        )

        trace = make_innertube_phase_trace(
            proxy_file='it.proxies',
        )
        trace('connection.connect_tcp.started', {})
        trace('connection.connect_tcp.complete', {})
        trace('connection.start_tls.started', {})
        trace('connection.start_tls.complete', {})

        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TCP_CONNECT_SECONDS, **labels,
            ),
            before_tcp + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_HTTP_CONNECT_SECONDS, **labels,
            ),
            before_conn + 1,
        )
        self.assertEqual(
            _hist_count(
                METRIC_PROXY_TLS_HANDSHAKE_SECONDS, **labels,
            ),
            before_tls + 1,
        )


class TestInstallInnerTubePhaseTracing(unittest.TestCase):

    def test_injects_trace_extension_into_outgoing_request(
        self,
    ) -> None:
        '''The patched send sets ``request.extensions['trace']``
        to a callable before delegating to the original
        send.'''
        session: mock.MagicMock = mock.MagicMock()
        session._phase_tracing_installed = False
        original_send: mock.MagicMock = session.send
        install_innertube_phase_tracing(
            session,
            proxy_file='it.proxies',
        )
        request: mock.MagicMock = mock.MagicMock()
        request.extensions = {}
        session.send(request)
        self.assertIn('trace', request.extensions)
        self.assertTrue(callable(request.extensions['trace']))
        original_send.assert_called_once_with(request)

    def test_does_not_overwrite_existing_trace_extension(
        self,
    ) -> None:
        '''If a caller already set extensions["trace"], do
        not clobber it.'''
        session: mock.MagicMock = mock.MagicMock()
        session._phase_tracing_installed = False
        install_innertube_phase_tracing(
            session,
            proxy_file='it.proxies',
        )
        marker: object = object()
        request: mock.MagicMock = mock.MagicMock()
        request.extensions = {'trace': marker}
        session.send(request)
        self.assertIs(request.extensions['trace'], marker)

    def test_install_is_idempotent(self) -> None:
        '''Calling install twice on the same session must not
        double-wrap send.'''
        session: mock.MagicMock = mock.MagicMock()
        session._phase_tracing_installed = False
        install_innertube_phase_tracing(
            session,
            proxy_file='it.proxies',
        )
        send_after_first: object = session.send
        install_innertube_phase_tracing(
            session,
            proxy_file='it.proxies',
        )
        self.assertIs(session.send, send_after_first)

    def test_request_counter_fresh_connection(self) -> None:
        '''Fresh-connection request: TCP started + headers
        sent → counter increments with reused="false".'''
        labels: dict[str, str] = {
            'call_type': 'innertube',
            'proxy_file': 'req_fresh',
        }
        before: float = _counter_value(
            METRIC_PROXY_REQUESTS,
            **labels, reused='false',
        )
        trace = make_innertube_phase_trace(
            proxy_file='req_fresh',
        )
        trace('connection.connect_tcp.started', {})
        trace('connection.connect_tcp.complete', {})
        trace('connection.start_tls.started', {})
        trace('connection.start_tls.complete', {})
        trace('http11.send_request_headers.started', {})
        self.assertEqual(
            _counter_value(
                METRIC_PROXY_REQUESTS,
                **labels, reused='false',
            ),
            before + 1.0,
        )

    def test_request_counter_keepalive_reuse(self) -> None:
        '''Reused connection: no connect_tcp events fire, but
        send_request_headers does → counter increments with
        reused="true".'''
        labels: dict[str, str] = {
            'call_type': 'innertube',
            'proxy_file': 'req_reused',
        }
        before: float = _counter_value(
            METRIC_PROXY_REQUESTS,
            **labels, reused='true',
        )
        trace = make_innertube_phase_trace(
            proxy_file='req_reused',
        )
        # Only request-level events — no connect.
        trace('http11.send_request_headers.started', {})
        self.assertEqual(
            _counter_value(
                METRIC_PROXY_REQUESTS,
                **labels, reused='true',
            ),
            before + 1.0,
        )

    def test_connect_exchange_send_headers_skipped(
        self,
    ) -> None:
        '''On an HTTPS-through-HTTP-proxy fresh connection,
        httpcore fires send_request_headers.started TWICE —
        once for the proxy CONNECT request itself (between
        connect_tcp.complete and start_tls.started), and once
        for the actual HTTPS request after start_tls.complete.
        Only the second event should increment the counter,
        otherwise fresh requests are double-counted.'''
        labels: dict[str, str] = {
            'call_type': 'innertube',
            'proxy_file': 'req_connect',
        }
        before: float = _counter_value(
            METRIC_PROXY_REQUESTS,
            **labels, reused='false',
        )
        trace = make_innertube_phase_trace(
            proxy_file='req_connect',
        )
        trace('connection.connect_tcp.started', {})
        trace('connection.connect_tcp.complete', {})
        # CONNECT exchange to the proxy fires
        # send_request_headers BEFORE start_tls — must NOT
        # count toward request total.
        trace('http11.send_request_headers.started', {})
        trace('http11.receive_response_headers.started', {})
        trace('http11.receive_response_headers.complete', {})
        trace('connection.start_tls.started', {})
        trace('connection.start_tls.complete', {})
        # Real HTTPS request fires send_request_headers AFTER
        # start_tls — this is the one that should count.
        trace('http11.send_request_headers.started', {})
        self.assertEqual(
            _counter_value(
                METRIC_PROXY_REQUESTS,
                **labels, reused='false',
            ),
            before + 1.0,
        )


class TestRequestCounterHtml(unittest.TestCase):

    def test_fresh_connection_counts_as_not_reused(
        self,
    ) -> None:
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'html_fresh',
        }
        before: float = _counter_value(
            METRIC_PROXY_REQUESTS,
            **labels, reused='false',
        )
        resp: httpx.Response = _response_with_curl_infos({
            int(CurlInfo.NAMELOOKUP_TIME): 0.005,
            int(CurlInfo.CONNECT_TIME): 0.110,
            int(CurlInfo.APPCONNECT_TIME): 1.250,
        })
        record_html_phase_metrics(
            resp,
            proxy_file='html_fresh',
        )
        self.assertEqual(
            _counter_value(
                METRIC_PROXY_REQUESTS,
                **labels, reused='false',
            ),
            before + 1.0,
        )

    def test_reused_connection_counts_as_reused(
        self,
    ) -> None:
        '''CONNECT_TIME=0 means curl reused a pooled
        connection.'''
        labels: dict[str, str] = {
            'call_type': 'html',
            'proxy_file': 'html_reused',
        }
        before: float = _counter_value(
            METRIC_PROXY_REQUESTS,
            **labels, reused='true',
        )
        resp: httpx.Response = _response_with_curl_infos({
            int(CurlInfo.NAMELOOKUP_TIME): 0.0,
            int(CurlInfo.CONNECT_TIME): 0.0,
            int(CurlInfo.APPCONNECT_TIME): 0.0,
        })
        record_html_phase_metrics(
            resp,
            proxy_file='html_reused',
        )
        self.assertEqual(
            _counter_value(
                METRIC_PROXY_REQUESTS,
                **labels, reused='true',
            ),
            before + 1.0,
        )


if __name__ == '__main__':
    unittest.main()
