'''Tests for the YouTube rate-limit settings.

Verifies defaults, env-var overrides, and that the
defaults are reflected in :data:`_DEFAULT_CONFIGS` on the
``YouTubeRateLimiter``.
'''

import importlib
import unittest
from unittest import mock


class TestRateLimitSettingsDefaults(unittest.TestCase):

    def test_defaults_when_no_env(self) -> None:
        with mock.patch.dict('os.environ', {}, clear=True):
            import scrape_exchange.youtube.rate_limit_settings as r
            importlib.reload(r)
            self.assertEqual(
                r.YT_RATE_LIMITS.browse_refill_per_min,
                150.0,
            )
            self.assertEqual(
                r.YT_RATE_LIMITS.player_refill_per_min,
                30.0,
            )
            self.assertEqual(
                r.YT_RATE_LIMITS.next_refill_per_min,
                150.0,
            )
            self.assertEqual(
                r.YT_RATE_LIMITS.html_refill_per_min,
                9.0,
            )
            self.assertEqual(
                r.YT_RATE_LIMITS.rss_refill_per_min,
                30.0,
            )


class TestRateLimitSettingsEnvOverride(unittest.TestCase):

    def test_player_override(self) -> None:
        with mock.patch.dict(
            'os.environ',
            {'PLAYER_REFILL_PER_MIN': '60'},
            clear=True,
        ):
            import scrape_exchange.youtube.rate_limit_settings as r
            importlib.reload(r)
            self.assertEqual(
                r.YT_RATE_LIMITS.player_refill_per_min,
                60.0,
            )

    def test_rss_override(self) -> None:
        with mock.patch.dict(
            'os.environ',
            {'RSS_REFILL_PER_MIN': '45'},
            clear=True,
        ):
            import scrape_exchange.youtube.rate_limit_settings as r
            importlib.reload(r)
            self.assertEqual(
                r.YT_RATE_LIMITS.rss_refill_per_min,
                45.0,
            )


class TestDefaultConfigsUseSettings(unittest.TestCase):
    '''The rate limiter's _DEFAULT_CONFIGS values must reflect
    YT_RATE_LIMITS — verifies the wire-in is intact.'''

    def test_player_default_in_configs(self) -> None:
        import scrape_exchange.youtube.youtube_rate_limiter as yrl
        self.assertAlmostEqual(
            yrl._DEFAULT_CONFIGS[
                yrl.YouTubeCallType.PLAYER
            ].refill_rate,
            30.0 / 60,
            places=6,
        )

    def test_rss_default_in_configs(self) -> None:
        import scrape_exchange.youtube.youtube_rate_limiter as yrl
        self.assertAlmostEqual(
            yrl._DEFAULT_CONFIGS[
                yrl.YouTubeCallType.RSS
            ].refill_rate,
            30.0 / 60,
            places=6,
        )

    def test_browse_default_in_configs(self) -> None:
        import scrape_exchange.youtube.youtube_rate_limiter as yrl
        self.assertAlmostEqual(
            yrl._DEFAULT_CONFIGS[
                yrl.YouTubeCallType.BROWSE
            ].refill_rate,
            150.0 / 60,
            places=6,
        )


if __name__ == '__main__':
    unittest.main()
