'''Smoke test that the 4 new circuit-breaker metrics exist with
the expected names and label sets. Prometheus client objects
expose ``_name`` and ``_labelnames``; the test reads those.'''

import unittest


class TestCircuitMetrics(unittest.TestCase):

    def test_metrics_defined(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_RSS_CIRCUIT_TRANSITIONS,
            METRIC_RSS_CIRCUIT_STATE,
            METRIC_RSS_CIRCUIT_OPEN_SECONDS,
            METRIC_RSS_CIRCUIT_WAIT_SECONDS,
        )
        self.assertEqual(
            METRIC_RSS_CIRCUIT_TRANSITIONS._name,
            'rss_circuit_transitions',
        )
        self.assertEqual(
            set(METRIC_RSS_CIRCUIT_TRANSITIONS._labelnames),
            {'platform', 'from_state', 'to_state'},
        )
        self.assertEqual(
            METRIC_RSS_CIRCUIT_STATE._name, 'rss_circuit_state',
        )
        self.assertEqual(
            set(METRIC_RSS_CIRCUIT_STATE._labelnames),
            {'platform', 'state'},
        )
        self.assertEqual(
            METRIC_RSS_CIRCUIT_OPEN_SECONDS._name,
            'rss_circuit_current_open_seconds',
        )
        self.assertEqual(
            METRIC_RSS_CIRCUIT_WAIT_SECONDS._name,
            'rss_circuit_wait_seconds',
        )


if __name__ == '__main__':
    unittest.main()
