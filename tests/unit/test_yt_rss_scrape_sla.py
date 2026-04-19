'''
Unit tests for the SLA classifier in
tools/yt_rss_scrape.py.

Verifies that ``_is_on_time`` returns the correct
classification for the on-time / overdue boundary
under different ``eligibility_fraction`` values.
'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType


def _load_yt_rss_scrape() -> ModuleType:
    import sys
    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']

    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_rss_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert (
        spec is not None
        and spec.loader is not None
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()
is_on_time = yt_rss_scrape._is_on_time


class TestIsOnTime(unittest.TestCase):

    def test_half_fraction_on_time_just_inside(
        self,
    ) -> None:
        '''With f=0.5, deadline = scheduled + 0.5*I.

        Channel scheduled at T=1000, interval=3600s
        (1h). deadline = 1000 + 0.5*3600 = 2800.
        Processed at 2799 → on-time.
        '''
        self.assertTrue(
            is_on_time(
                scheduled_time=1000.0,
                now=2799.0,
                interval_seconds=3600.0,
                eligibility_fraction=0.5,
            )
        )

    def test_half_fraction_overdue_just_outside(
        self,
    ) -> None:
        self.assertFalse(
            is_on_time(
                scheduled_time=1000.0,
                now=2801.0,
                interval_seconds=3600.0,
                eligibility_fraction=0.5,
            )
        )

    def test_half_fraction_on_time_at_boundary(
        self,
    ) -> None:
        '''Boundary is inclusive (now <= deadline).'''
        self.assertTrue(
            is_on_time(
                scheduled_time=1000.0,
                now=2800.0,
                interval_seconds=3600.0,
                eligibility_fraction=0.5,
            )
        )

    def test_full_fraction_always_overdue(
        self,
    ) -> None:
        '''f=1.0 collapses deadline to scheduled.

        Claim cutoff guarantees now > scheduled, so
        the on-time branch can never fire. This pins
        the historical 0% behaviour.
        '''
        self.assertFalse(
            is_on_time(
                scheduled_time=1000.0,
                now=1000.001,
                interval_seconds=3600.0,
                eligibility_fraction=1.0,
            )
        )

    def test_full_fraction_on_time_at_exact_match(
        self,
    ) -> None:
        '''At exactly now == scheduled with f=1.0, the
        boundary is inclusive: on-time. Doesn't
        happen in practice because of claim cutoff,
        but the math should hold.'''
        self.assertTrue(
            is_on_time(
                scheduled_time=1000.0,
                now=1000.0,
                interval_seconds=3600.0,
                eligibility_fraction=1.0,
            )
        )


if __name__ == '__main__':
    unittest.main()
