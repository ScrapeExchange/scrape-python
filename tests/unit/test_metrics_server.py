'''Unit test for the prom_client backlog wrapper.'''

import unittest
from unittest.mock import patch

from prometheus_client.exposition import ThreadingWSGIServer

from scrape_exchange.metrics_server import start_metrics_server


class TestMetricsServer(unittest.TestCase):

    def test_sets_request_queue_size_then_starts_server(
        self,
    ) -> None:
        '''start_metrics_server must set
        ``ThreadingWSGIServer.request_queue_size`` before invoking
        ``start_http_server`` so the server's listen() syscall picks
        up the larger backlog.'''

        original: int = ThreadingWSGIServer.request_queue_size
        observed: dict[str, int] = {}

        def fake_start(port: int) -> None:
            observed['queue_size'] = (
                ThreadingWSGIServer.request_queue_size
            )
            observed['port'] = port

        try:
            with patch(
                'scrape_exchange.metrics_server._start_http_server',
                fake_start,
            ):
                start_metrics_server(9999, backlog=64)
            self.assertEqual(observed['queue_size'], 64)
            self.assertEqual(observed['port'], 9999)
        finally:
            ThreadingWSGIServer.request_queue_size = original

    def test_default_backlog_is_128(self) -> None:
        original: int = ThreadingWSGIServer.request_queue_size
        observed: dict[str, int] = {}

        def fake_start(port: int) -> None:
            observed['queue_size'] = (
                ThreadingWSGIServer.request_queue_size
            )

        try:
            with patch(
                'scrape_exchange.metrics_server._start_http_server',
                fake_start,
            ):
                start_metrics_server(9999)
            self.assertEqual(observed['queue_size'], 128)
        finally:
            ThreadingWSGIServer.request_queue_size = original


if __name__ == '__main__':
    unittest.main()
