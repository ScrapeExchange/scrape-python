'''
Response transfer metrics: bytes received, time waiting for the
response headers and time receiving the body, for InnerTube
requests and for requests through TransferMetricsCurlTransport.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import gzip
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import patch

import httpx
import httpx2
from prometheus_client import REGISTRY

from scrape_exchange import proxy_phase_metrics
from scrape_exchange.proxy_phase_metrics import (
    TransferMetricsCurlTransport,
    install_innertube_phase_tracing,
)

PROXY_FILE: str = 'transfer-test.proxies'


def _sample(name: str, labels: dict[str, str]) -> float:
    value: float | None = REGISTRY.get_sample_value(name, labels)
    return value or 0.0


class _Clock:
    '''Settable stand-in for time.perf_counter.'''

    def __init__(self) -> None:
        self.now: float = 0.0

    def __call__(self) -> float:
        return self.now


class _FakeResponse:

    def __init__(self, num_bytes: int) -> None:
        self.num_bytes_downloaded: int = num_bytes


class _FakeSession:
    '''Plays a list of ``(time, trace event)`` pairs into the
    request's trace extension, as httpcore2 would, and returns a
    response that downloaded *num_bytes*.'''

    def __init__(
        self,
        clock: _Clock,
        events: list[tuple[float, str]],
        num_bytes: int,
    ) -> None:
        self._clock: _Clock = clock
        self._events: list[tuple[float, str]] = events
        self._num_bytes: int = num_bytes

    def send(self, request: Any, **kwargs: Any) -> _FakeResponse:
        trace: Any = request.extensions.get('trace')
        for at, name in self._events:
            self._clock.now = at
            trace(name, {})
        return _FakeResponse(self._num_bytes)


# A proxied request: the CONNECT exchange fires request and header
# events too, before the real request's events.
PROXIED_EVENTS: list[tuple[float, str]] = [
    (1.0, 'connection.connect_tcp.started'),
    (1.1, 'connection.connect_tcp.complete'),
    (1.1, 'http11.send_request_headers.started'),
    (1.1, 'http11.send_request_body.complete'),
    (1.3, 'http11.receive_response_headers.complete'),
    (1.3, 'connection.start_tls.started'),
    (1.5, 'connection.start_tls.complete'),
    (1.5, 'http11.send_request_headers.started'),
    (10.0, 'http11.send_request_body.complete'),
    (10.4, 'http11.receive_response_headers.complete'),
    (10.4, 'http11.receive_response_body.started'),
    (11.4, 'http11.receive_response_body.complete'),
]


class TestInnerTubeTransfer(unittest.TestCase):

    def setUp(self) -> None:
        self.clock: _Clock = _Clock()
        patcher = patch.object(
            proxy_phase_metrics.time, 'perf_counter', self.clock,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _labels(self, call: str) -> dict[str, str]:
        return {
            'platform': 'youtube',
            'scraper': proxy_phase_metrics._SCRAPER_LABEL,
            'call': call,
            'proxy_file': PROXY_FILE,
        }

    def _send(
        self,
        url: str,
        events: list[tuple[float, str]],
        num_bytes: int = 12345,
        extensions: dict[str, Any] | None = None,
    ) -> None:
        session: _FakeSession = _FakeSession(
            self.clock, events, num_bytes,
        )
        install_innertube_phase_tracing(session, proxy_file=PROXY_FILE)
        request: httpx2.Request = httpx2.Request(
            'POST', url, extensions=extensions or {},
        )
        session.send(request)

    def test_records_bytes_wait_and_body(self) -> None:
        labels: dict[str, str] = self._labels('player')
        before: dict[str, float] = {
            name: _sample(name, labels) for name in (
                'api_response_bytes_sum',
                'api_response_bytes_count',
                'api_response_wait_seconds_sum',
                'api_response_body_seconds_sum',
            )
        }
        self._send(
            'https://www.youtube.com/youtubei/v1/player?key=x',
            PROXIED_EVENTS,
        )
        self.assertEqual(
            _sample('api_response_bytes_sum', labels)
            - before['api_response_bytes_sum'],
            12345,
        )
        self.assertEqual(
            _sample('api_response_bytes_count', labels)
            - before['api_response_bytes_count'],
            1,
        )
        # The real request's events, not the CONNECT exchange's.
        self.assertAlmostEqual(
            _sample('api_response_wait_seconds_sum', labels)
            - before['api_response_wait_seconds_sum'],
            0.4,
        )
        self.assertAlmostEqual(
            _sample('api_response_body_seconds_sum', labels)
            - before['api_response_body_seconds_sum'],
            1.0,
        )

    def test_call_is_last_path_segment(self) -> None:
        labels: dict[str, str] = self._labels('resolve_url')
        before: float = _sample('api_response_bytes_count', labels)
        self._send(
            'https://www.youtube.com/youtubei/v1/navigation/'
            'resolve_url',
            PROXIED_EVENTS,
        )
        self.assertEqual(
            _sample('api_response_bytes_count', labels) - before, 1,
        )

    def test_unread_body_records_nothing(self) -> None:
        labels: dict[str, str] = self._labels('next')
        before: float = _sample('api_response_bytes_count', labels)
        events: list[tuple[float, str]] = [
            event for event in PROXIED_EVENTS
            if 'receive_response_body' not in event[1]
        ]
        self._send(
            'https://www.youtube.com/youtubei/v1/next', events,
        )
        self.assertEqual(
            _sample('api_response_bytes_count', labels), before,
        )

    def test_caller_trace_records_nothing(self) -> None:
        labels: dict[str, str] = self._labels('browse')
        before: float = _sample('api_response_bytes_count', labels)
        self._send(
            'https://www.youtube.com/youtubei/v1/browse',
            [],
            extensions={'trace': lambda name, info: None},
        )
        self.assertEqual(
            _sample('api_response_bytes_count', labels), before,
        )


# Incompressible, so the size on the wire is close to the size of
# the data and well below the decompressed size.
WIRE_BODY: bytes = gzip.compress(os.urandom(200_000) + b'x' * 400_000)
HEADER_DELAY: float = 0.2
BODY_DELAY: float = 0.2


class _Handler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        time.sleep(HEADER_DELAY)
        self.send_response(200)
        self.send_header('Content-Encoding', 'gzip')
        self.send_header('Content-Length', str(len(WIRE_BODY)))
        self.end_headers()
        self.wfile.flush()
        time.sleep(BODY_DELAY)
        self.wfile.write(WIRE_BODY)

    do_POST = do_GET

    def log_message(self, *args: Any) -> None:
        pass


class _ServerCase(unittest.IsolatedAsyncioTestCase):
    '''Runs a local HTTP server that sends a gzipped body after a
    delay before the headers and another before the body.'''

    @classmethod
    def setUpClass(cls) -> None:
        cls.server: ThreadingHTTPServer = ThreadingHTTPServer(
            ('127.0.0.1', 0), _Handler,
        )
        threading.Thread(
            target=cls.server.serve_forever, daemon=True,
        ).start()
        cls.url: str = f'http://127.0.0.1:{cls.server.server_port}/'

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()


class TestCurlTransportTransfer(_ServerCase):

    def _client(self, call: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=TransferMetricsCurlTransport(
                platform='test', scraper='test_scraper', call=call,
            ),
        )

    def _labels(self, call: str) -> dict[str, str]:
        return {
            'platform': 'test',
            'scraper': 'test_scraper',
            'call': call,
            'proxy_file': 'none',
        }

    async def test_records_wire_bytes_and_times(self) -> None:
        labels: dict[str, str] = self._labels('read')
        before_bytes: float = _sample('api_response_bytes_sum', labels)
        before_wait: float = _sample(
            'api_response_wait_seconds_sum', labels,
        )
        before_body: float = _sample(
            'api_response_body_seconds_sum', labels,
        )
        async with self._client('read') as client:
            response: httpx.Response = await client.get(self.url)
        # curl decompresses; the metric counts bytes on the wire.
        self.assertEqual(len(response.content), 600_000)
        self.assertEqual(
            _sample('api_response_bytes_sum', labels) - before_bytes,
            len(WIRE_BODY),
        )
        self.assertGreaterEqual(
            _sample('api_response_wait_seconds_sum', labels)
            - before_wait,
            HEADER_DELAY * 0.9,
        )
        self.assertGreaterEqual(
            _sample('api_response_body_seconds_sum', labels)
            - before_body,
            BODY_DELAY * 0.9,
        )

    async def test_unread_body_records_nothing(self) -> None:
        labels: dict[str, str] = self._labels('unread')
        before: float = _sample('api_response_bytes_count', labels)
        async with (
            self._client('unread') as client,
            client.stream('GET', self.url) as response,
        ):
            self.assertEqual(response.status_code, 200)
        self.assertEqual(
            _sample('api_response_bytes_count', labels), before,
        )


class TestInnerTubeTransferRealClient(_ServerCase):
    '''The InnerTube hook on a real httpx2 session: the trace
    events it relies on are the ones httpcore2 emits.'''

    async def test_records_wire_bytes_and_times(self) -> None:
        labels: dict[str, str] = {
            'platform': 'youtube',
            'scraper': proxy_phase_metrics._SCRAPER_LABEL,
            'call': 'player',
            'proxy_file': PROXY_FILE,
        }
        before_bytes: float = _sample('api_response_bytes_sum', labels)
        before_wait: float = _sample(
            'api_response_wait_seconds_sum', labels,
        )
        before_body: float = _sample(
            'api_response_body_seconds_sum', labels,
        )
        with httpx2.Client() as session:
            install_innertube_phase_tracing(
                session, proxy_file=PROXY_FILE,
            )
            response: httpx2.Response = session.send(
                session.build_request(
                    'POST', f'{self.url}youtubei/v1/player',
                ),
            )
        self.assertEqual(len(response.content), 600_000)
        self.assertEqual(
            _sample('api_response_bytes_sum', labels) - before_bytes,
            len(WIRE_BODY),
        )
        self.assertGreaterEqual(
            _sample('api_response_wait_seconds_sum', labels)
            - before_wait,
            HEADER_DELAY * 0.9,
        )
        self.assertGreaterEqual(
            _sample('api_response_body_seconds_sum', labels)
            - before_body,
            BODY_DELAY * 0.9,
        )


if __name__ == '__main__':
    unittest.main()
