'''
Verify ``scrape_queue_size`` carries a ``worker_id`` label. Per-
process callers pass ``get_worker_id()`` so each worker has its
own series and ``sum by (entity, state)`` gives a fleet total.
Shared-state callers pass ``worker_id=''`` so all workers collide
on one series and ``livemostrecent`` gives the correct shared
value without inflating dashboards.
'''
import unittest


class TestScrapeQueueSizeLabel(unittest.TestCase):

    def test_scrape_queue_size_has_worker_id_label(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SCRAPE_QUEUE_SIZE,
        )
        labelnames = list(METRIC_SCRAPE_QUEUE_SIZE._labelnames)
        self.assertIn('worker_id', labelnames)

    def test_label_set_complete(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SCRAPE_QUEUE_SIZE,
        )
        labelnames = set(METRIC_SCRAPE_QUEUE_SIZE._labelnames)
        self.assertEqual(
            labelnames,
            {'platform', 'scraper', 'entity', 'state', 'worker_id'},
        )


if __name__ == '__main__':
    unittest.main()
