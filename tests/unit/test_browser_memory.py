'''Unit tests for scrape_exchange.browser_memory, against a fake /proc.'''

import os
import tempfile
import unittest
from pathlib import Path

from scrape_exchange.browser_memory import BrowserMemory, browser_memory

ROOT_PID: int = 100


def _add_process(
    proc: Path, pid: int, ppid: int, exe: str | None, pss_kb: int | None,
    comm: str = 'proc',
) -> None:
    pid_dir: Path = proc / str(pid)
    pid_dir.mkdir()
    (pid_dir / 'stat').write_text(f'{pid} ({comm}) S {ppid} 1 1 0\n')
    if exe is not None:
        os.symlink(exe, pid_dir / 'exe')
    if pss_kb is not None:
        (pid_dir / 'smaps_rollup').write_text(
            '00400000-7fff [rollup]\n'
            f'Rss:              {pss_kb * 2} kB\n'
            f'Pss:              {pss_kb} kB\n'
        )


class TestBrowserMemory(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.proc: Path = Path(self._tmp.name)
        (self.proc / 'self').mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_sums_browser_and_driver_descendants(self) -> None:
        _add_process(self.proc, ROOT_PID, 1, '/usr/bin/python3', 50_000)
        _add_process(self.proc, 101, ROOT_PID, '/usr/bin/node', 60_000)
        _add_process(
            self.proc, 102, 101, '/opt/camoufox/camoufox-bin', 200_000,
            comm='camoufox-bin',
        )
        # Content process: its name has a space and a parenthesis.
        _add_process(
            self.proc, 103, 102, '/opt/camoufox/camoufox-bin', 100_000,
            comm='Isolated Web Co) x',
        )
        usage: BrowserMemory | None = browser_memory(ROOT_PID, self.proc)
        self.assertEqual(usage, BrowserMemory(
            browser_bytes=300_000 * 1024,
            browser_processes=2,
            driver_bytes=60_000 * 1024,
            driver_processes=1,
        ))

    def test_ignores_unrelated_and_unreadable_processes(self) -> None:
        _add_process(self.proc, ROOT_PID, 1, '/usr/bin/python3', 50_000)
        # Another process tree's browser.
        _add_process(self.proc, 200, 1, '/opt/camoufox/camoufox-bin', 999)
        # A descendant that is neither browser nor driver.
        _add_process(self.proc, 101, ROOT_PID, '/usr/bin/sh', 999)
        # Browser whose exe and smaps are not readable.
        _add_process(self.proc, 102, ROOT_PID, None, 999)
        _add_process(
            self.proc, 103, ROOT_PID, '/opt/camoufox/camoufox-bin', None,
        )
        usage: BrowserMemory | None = browser_memory(ROOT_PID, self.proc)
        self.assertEqual(usage, BrowserMemory())

    def test_no_proc_returns_none(self) -> None:
        self.assertIsNone(
            browser_memory(ROOT_PID, self.proc / 'missing'),
        )

    def test_live_proc_reads_this_process(self) -> None:
        if not Path('/proc/self').is_dir():
            self.skipTest('needs Linux /proc')
        usage: BrowserMemory | None = browser_memory()
        self.assertIsNotNone(usage)


if __name__ == '__main__':
    unittest.main()
