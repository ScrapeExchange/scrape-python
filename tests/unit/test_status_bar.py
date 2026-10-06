'''
Unit tests for the migration tool's status bar.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import io
import unittest

from scrape_exchange.redis_compaction import SetCopyStats, VideoMetaStats
from scrape_exchange.status_bar import StatusBar


class _Clock:

    def __init__(self) -> None:
        self.now: float = 0.0

    def __call__(self) -> float:
        return self.now


class _Tty(io.StringIO):

    def isatty(self) -> bool:
        return True


class TestStatusBar(unittest.TestCase):

    def test_render_with_total(self) -> None:
        clock: _Clock = _Clock()
        bar: StatusBar = StatusBar(
            'video-meta (dry-run)', 1000, estimated=True,
            stream=io.StringIO(), clock=clock,
        )
        clock.now = 10.0
        line: str = bar.render(
            VideoMetaStats(scanned=250, migrated=240, skipped=10),
        )
        self.assertIn('[######------------------]', line)
        self.assertIn('25.0%', line)
        self.assertIn('250/~1,000', line)
        self.assertIn('25/s', line)
        self.assertIn('ETA 0:00:30', line)
        self.assertIn('elapsed 0:00:10', line)
        self.assertIn('migrated=240', line)
        self.assertNotIn('scanned=', line)

    def test_render_without_total(self) -> None:
        clock: _Clock = _Clock()
        bar: StatusBar = StatusBar(
            'uploaded (apply)', 0, stream=io.StringIO(), clock=clock,
        )
        clock.now = 2.0
        line: str = bar.render(SetCopyStats(scanned=100, copied=100))
        self.assertNotIn('[', line)
        self.assertNotIn('ETA', line)
        self.assertIn('100/?', line)
        self.assertIn('legacy_deleted=False', line)

    def test_log_output_is_throttled(self) -> None:
        clock: _Clock = _Clock()
        out: io.StringIO = io.StringIO()
        bar: StatusBar = StatusBar(
            'x', 10, stream=out, clock=clock, log_interval=30,
        )
        bar.update(SetCopyStats(scanned=1))
        clock.now = 10.0
        bar.update(SetCopyStats(scanned=2))
        clock.now = 31.0
        bar.update(SetCopyStats(scanned=3))
        bar.finish(SetCopyStats(scanned=10))
        lines: list[str] = out.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertIn('1/10', lines[0])
        self.assertIn('3/10', lines[1])
        self.assertIn('100.0%', lines[2])

    def test_tty_redraws_in_place(self) -> None:
        clock: _Clock = _Clock()
        out: _Tty = _Tty()
        bar: StatusBar = StatusBar('x', 10, stream=out, clock=clock)
        bar.update(SetCopyStats(scanned=1))
        clock.now = 2.0
        bar.update(SetCopyStats(scanned=5))
        bar.finish(SetCopyStats(scanned=10))
        text: str = out.getvalue()
        self.assertEqual(text.count('\r\x1b[K'), 3)
        self.assertTrue(text.endswith('\n'))
        self.assertEqual(text.count('\n'), 1)

    def test_inplace_override_without_tty(self) -> None:
        out: io.StringIO = io.StringIO()
        bar: StatusBar = StatusBar(
            'x', 10, stream=out, clock=_Clock(), inplace=True,
        )
        bar.update(SetCopyStats(scanned=1))
        bar.finish(SetCopyStats(scanned=10))
        self.assertEqual(out.getvalue().count('\r\x1b[K'), 2)

    def test_lines_override_on_tty(self) -> None:
        out: _Tty = _Tty()
        bar: StatusBar = StatusBar(
            'x', 10, stream=out, clock=_Clock(), inplace=False,
        )
        bar.update(SetCopyStats(scanned=1))
        self.assertNotIn('\r', out.getvalue())
        self.assertTrue(out.getvalue().endswith('\n'))


if __name__ == '__main__':
    unittest.main()
