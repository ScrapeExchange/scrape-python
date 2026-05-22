'''
Tests for start_aggregating_metrics_server: it must wipe and
recreate the multiproc dir, export PROMETHEUS_MULTIPROC_DIR to the
environment, and start an HTTP server backed by
MultiProcessCollector.
'''
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class TestStartAggregatingMetricsServer(unittest.TestCase):

    def setUp(self) -> None:
        self._saved_env = os.environ.pop(
            'PROMETHEUS_MULTIPROC_DIR', None,
        )

    def tearDown(self) -> None:
        os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)
        if self._saved_env is not None:
            os.environ[
                'PROMETHEUS_MULTIPROC_DIR'
            ] = self._saved_env

    def test_creates_dir_when_missing(self) -> None:
        from scrape_exchange.metrics_server import (
            start_aggregating_metrics_server,
        )
        with tempfile.TemporaryDirectory() as tmp:
            target: Path = Path(tmp) / 'metrics'
            self.assertFalse(target.exists())
            with patch(
                'scrape_exchange.metrics_server'
                '._start_http_server',
            ):
                start_aggregating_metrics_server(
                    _free_port(), target,
                )
            self.assertTrue(target.is_dir())

    def test_wipes_existing_dir_contents(self) -> None:
        from scrape_exchange.metrics_server import (
            start_aggregating_metrics_server,
        )
        with tempfile.TemporaryDirectory() as tmp:
            target: Path = Path(tmp) / 'metrics'
            target.mkdir()
            ghost: Path = target / 'ghost_pid_12345.db'
            ghost.write_bytes(b'stale')
            with patch(
                'scrape_exchange.metrics_server'
                '._start_http_server',
            ):
                start_aggregating_metrics_server(
                    _free_port(), target,
                )
            self.assertFalse(ghost.exists())
            self.assertTrue(target.is_dir())

    def test_sets_env_var(self) -> None:
        from scrape_exchange.metrics_server import (
            start_aggregating_metrics_server,
        )
        with tempfile.TemporaryDirectory() as tmp:
            target: Path = Path(tmp) / 'metrics'
            with patch(
                'scrape_exchange.metrics_server'
                '._start_http_server',
            ):
                start_aggregating_metrics_server(
                    _free_port(), target,
                )
            self.assertEqual(
                os.environ.get('PROMETHEUS_MULTIPROC_DIR'),
                str(target),
            )

    def test_calls_start_http_server_with_registry(self) -> None:
        from scrape_exchange.metrics_server import (
            start_aggregating_metrics_server,
        )
        port: int = _free_port()
        with tempfile.TemporaryDirectory() as tmp:
            target: Path = Path(tmp) / 'metrics'
            with patch(
                'scrape_exchange.metrics_server'
                '._start_http_server',
            ) as mocked:
                start_aggregating_metrics_server(port, target)
            self.assertEqual(mocked.call_count, 1)
            args, kwargs = mocked.call_args
            self.assertEqual(args[0], port)
            self.assertIn('registry', kwargs)


if __name__ == '__main__':
    unittest.main()
