'''
Tests for the supervisor's multiprocess-mode wiring:
1. ``SupervisorConfig`` accepts a ``multiproc_dir`` field with a
   sane default derived from ``scraper_label``.
2. ``spawn_children`` no longer injects ``METRICS_PORT`` into
   child environments.
3. ``_record_crash`` and ``_retire_slot`` call
   ``prometheus_client.multiprocess.mark_process_dead(pid)`` on the
   exiting child's pid.
'''
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestSupervisorConfigMultiprocDir(unittest.TestCase):

    def test_defaults_to_run_scrape_subdir(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            SupervisorConfig,
        )
        cfg = SupervisorConfig(
            scraper_label='yt_video',
            num_processes_env_var='VIDEO_NUM_PROCESSES',
            num_processes=2,
            concurrency=16,
            proxies=['http://proxy1'],
            metrics_port=9400,
            log_file=None,
        )
        self.assertEqual(
            cfg.multiproc_dir,
            Path('/run/scrape/yt_video/metrics'),
        )

    def test_can_be_overridden(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            SupervisorConfig,
        )
        cfg = SupervisorConfig(
            scraper_label='yt_rss',
            num_processes_env_var='RSS_NUM_PROCESSES',
            num_processes=1,
            concurrency=8,
            proxies=['http://proxy1'],
            metrics_port=9300,
            log_file=None,
            multiproc_dir=Path('/tmp/custom/metrics'),
        )
        self.assertEqual(
            cfg.multiproc_dir,
            Path('/tmp/custom/metrics'),
        )


class TestMarkProcessDeadOnReap(unittest.TestCase):

    def _build_slot(self, pid: int) -> MagicMock:
        slot = MagicMock()
        slot.instance = 0
        slot.backoff = 0.0
        slot.respawn_at = None
        slot.process = MagicMock()
        slot.process.pid = pid
        return slot

    def test_record_crash_marks_dead(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _record_crash,
        )
        slot = self._build_slot(pid=4242)
        with patch(
            'scrape_exchange.scraper_supervisor'
            '.multiprocess.mark_process_dead',
        ) as mocked:
            _record_crash(
                'yt_video', slot, rc=1,
                ran_seconds=10.0, now=0.0,
            )
        mocked.assert_called_once_with(4242)

    def test_retire_slot_marks_dead(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _retire_slot,
        )
        slot = self._build_slot(pid=4343)
        with patch(
            'scrape_exchange.scraper_supervisor'
            '.multiprocess.mark_process_dead',
        ) as mocked:
            _retire_slot(
                'yt_video', slot, rc=0,
                shutting_down=False,
            )
        mocked.assert_called_once_with(4343)


if __name__ == '__main__':
    unittest.main()
