'''
Verify the worker-side metrics HTTP server is bound when running
standalone (no PROMETHEUS_MULTIPROC_DIR) and skipped when the
supervisor has set the multiproc env var.
'''
import os
import unittest
from unittest.mock import patch


class TestStartMetricsServerOrSkip(unittest.TestCase):

    def setUp(self) -> None:
        self._saved: str | None = os.environ.pop(
            'PROMETHEUS_MULTIPROC_DIR', None,
        )

    def tearDown(self) -> None:
        os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)
        if self._saved is not None:
            os.environ['PROMETHEUS_MULTIPROC_DIR'] = self._saved

    def test_calls_when_env_unset(self) -> None:
        from scrape_exchange.scraper_runner import (
            _start_metrics_server_or_skip,
        )
        with patch(
            'scrape_exchange.scraper_runner.start_metrics_server',
        ) as mocked:
            _start_metrics_server_or_skip(9400)
        mocked.assert_called_once_with(9400)

    def test_skips_when_env_set(self) -> None:
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = '/nonexistent/multiproc'
        from scrape_exchange.scraper_runner import (
            _start_metrics_server_or_skip,
        )
        with patch(
            'scrape_exchange.scraper_runner.start_metrics_server',
        ) as mocked:
            _start_metrics_server_or_skip(9400)
        mocked.assert_not_called()

    def test_oserror_is_swallowed(self) -> None:
        from scrape_exchange.scraper_runner import (
            _start_metrics_server_or_skip,
        )
        with patch(
            'scrape_exchange.scraper_runner.start_metrics_server',
            side_effect=OSError('port in use'),
        ):
            # Must not raise; the OSError is logged and swallowed
            _start_metrics_server_or_skip(9400)


if __name__ == '__main__':
    unittest.main()
