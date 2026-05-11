'''Verify METRIC_SCRAPE_DURATION exposes low-end buckets so RSS
and channel scrapes (typically sub-second) get useful percentile
resolution alongside the existing video buckets.'''

import unittest

from scrape_exchange.scraper_metrics import METRIC_SCRAPE_DURATION


class TestScrapeDurationBuckets(unittest.TestCase):

    def test_low_end_buckets_present(self) -> None:
        bounds: tuple[float, ...] = tuple(
            METRIC_SCRAPE_DURATION._upper_bounds
        )
        self.assertIn(0.1, bounds)
        self.assertIn(0.25, bounds)

    def test_existing_buckets_preserved(self) -> None:
        bounds: tuple[float, ...] = tuple(
            METRIC_SCRAPE_DURATION._upper_bounds
        )
        for existing in (
            0.5, 1.0, 2.5, 5.0, 10.0,
            30.0, 60.0, 120.0, 300.0,
        ):
            self.assertIn(existing, bounds)

    def test_buckets_are_monotonic(self) -> None:
        bounds: list[float] = list(
            METRIC_SCRAPE_DURATION._upper_bounds
        )
        # _upper_bounds includes float('inf') as the final entry;
        # the whole list must be non-decreasing.
        self.assertEqual(bounds, sorted(bounds))


if __name__ == '__main__':
    unittest.main()
