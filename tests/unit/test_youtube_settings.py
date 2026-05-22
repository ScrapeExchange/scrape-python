import os
import unittest
from unittest.mock import patch

from scrape_exchange.youtube.settings import YouTubeScraperSettings


class TestRssCircuitSettings(unittest.TestCase):
    '''Verify the RSS circuit-breaker fields parse from
    env vars and use the documented defaults.'''

    def setUp(self) -> None:
        # Strip any inherited env that would shadow our defaults.
        for name in (
            'RSS_CIRCUIT_FAIL_THRESHOLD',
            'RSS_CIRCUIT_WINDOW_SIZE',
            'RSS_CIRCUIT_INITIAL_OPEN_SECONDS',
            'RSS_CIRCUIT_MAX_OPEN_SECONDS',
            'RSS_CIRCUIT_IMPAIRED_REOPEN_THRESHOLD',
            'RSS_CIRCUIT_RECOVERY_THRESHOLD',
            'RSS_CIRCUIT_WAIT_JITTER_SECONDS',
        ):
            os.environ.pop(name, None)

    def test_defaults(self) -> None:
        s: YouTubeScraperSettings = YouTubeScraperSettings(
            _env_file=None,
            _cli_parse_args=[],
        )
        self.assertEqual(s.rss_circuit_fail_threshold, 8)
        self.assertEqual(s.rss_circuit_window_size, 10)
        self.assertEqual(s.rss_circuit_initial_open_seconds, 60)
        self.assertEqual(s.rss_circuit_max_open_seconds, 7200)
        self.assertEqual(
            s.rss_circuit_impaired_reopen_threshold, 3,
        )
        self.assertEqual(s.rss_circuit_recovery_threshold, 50)
        self.assertEqual(
            s.rss_circuit_wait_jitter_seconds, 30.0,
        )

    def test_env_overrides(self) -> None:
        with patch.dict(os.environ, {
            'RSS_CIRCUIT_FAIL_THRESHOLD': '15',
            'RSS_CIRCUIT_WINDOW_SIZE': '30',
            'RSS_CIRCUIT_INITIAL_OPEN_SECONDS': '120',
            'RSS_CIRCUIT_MAX_OPEN_SECONDS': '3600',
            'RSS_CIRCUIT_IMPAIRED_REOPEN_THRESHOLD': '5',
            'RSS_CIRCUIT_RECOVERY_THRESHOLD': '25',
            'RSS_CIRCUIT_WAIT_JITTER_SECONDS': '7.5',
        }):
            s: YouTubeScraperSettings = YouTubeScraperSettings(
                _env_file=None,
                _cli_parse_args=[],
            )
        self.assertEqual(s.rss_circuit_fail_threshold, 15)
        self.assertEqual(s.rss_circuit_window_size, 30)
        self.assertEqual(s.rss_circuit_initial_open_seconds, 120)
        self.assertEqual(s.rss_circuit_max_open_seconds, 3600)
        self.assertEqual(
            s.rss_circuit_impaired_reopen_threshold, 5,
        )
        self.assertEqual(s.rss_circuit_recovery_threshold, 25)
        self.assertEqual(
            s.rss_circuit_wait_jitter_seconds, 7.5,
        )
