'''
Unit tests for InstagramRateLimiter defaults and env overrides.
'''

import os
import unittest

from unittest.mock import patch

from scrape_exchange.instagram.instagram_rate_limiter import (
    InstagramCallType,
    InstagramRateLimiter,
)
from scrape_exchange.rate_limiter import _BucketConfig


class TestInstagramRateLimiter(unittest.TestCase):

    def setUp(self) -> None:
        InstagramRateLimiter.reset()

    def tearDown(self) -> None:
        InstagramRateLimiter.reset()

    def test_default_bucket_rates(self) -> None:
        rl: InstagramRateLimiter = InstagramRateLimiter()
        configs: dict[InstagramCallType, _BucketConfig] = (
            rl.default_configs
        )
        self.assertAlmostEqual(
            configs[InstagramCallType.CREATOR].refill_rate,
            1 / 60,
        )
        self.assertEqual(configs[InstagramCallType.CREATOR].burst, 1)
        self.assertAlmostEqual(
            configs[InstagramCallType.BOOTSTRAP].refill_rate,
            2 / 60,
        )

    def test_env_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {'IG_CREATOR_RPM': '6', 'IG_BOOTSTRAP_RPM': '12'},
        ):
            rl: InstagramRateLimiter = InstagramRateLimiter()
            configs: dict[InstagramCallType, _BucketConfig] = (
                rl.default_configs
            )
            self.assertAlmostEqual(
                configs[InstagramCallType.CREATOR].refill_rate,
                0.1,
            )
            self.assertAlmostEqual(
                configs[InstagramCallType.BOOTSTRAP].refill_rate,
                0.2,
            )

    def test_creator_falls_back_to_global_rpm(self) -> None:
        with patch.dict(os.environ, {'IG_GLOBAL_RPM': '3'}):
            os.environ.pop('IG_CREATOR_RPM', None)
            rl: InstagramRateLimiter = InstagramRateLimiter()
            configs: dict[InstagramCallType, _BucketConfig] = (
                rl.default_configs
            )
            self.assertAlmostEqual(
                configs[InstagramCallType.CREATOR].refill_rate,
                3 / 60,
            )


if __name__ == '__main__':
    unittest.main()
