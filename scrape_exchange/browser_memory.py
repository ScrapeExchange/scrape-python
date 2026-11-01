'''
Memory used by the browsers a scraper process launched.

Playwright starts a Node.js driver as a child of the Python process,
and the driver starts the browser, whose content and utility
processes are its children. Every one of them is a descendant of the
scraper process, so walking ``/proc`` from that PID finds them all
without asking Playwright for PIDs it does not expose.

Memory is reported as PSS (proportional set size): shared pages are
split between the processes that map them, so summing PSS over many
browser processes does not count the shared Firefox libraries once
per process the way RSS would.

Linux only: :func:`browser_memory` returns None where ``/proc`` is
missing.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
from dataclasses import dataclass
from pathlib import Path

PROC_ROOT: Path = Path('/proc')

# Executable basenames: Camoufox's Firefox and Playwright's driver.
BROWSER_EXE_PREFIX: str = 'camoufox'
DRIVER_EXE: str = 'node'


@dataclass(frozen=True)
class BrowserMemory:
    browser_bytes: int = 0
    browser_processes: int = 0
    driver_bytes: int = 0
    driver_processes: int = 0


def _parent_pid(proc_dir: Path) -> int | None:
    '''Parent PID from ``/proc/<pid>/stat``. The command name may
    contain spaces and parentheses, so parse after the last ``)``.'''
    try:
        stat: str = (proc_dir / 'stat').read_text()
    except OSError:
        return None
    fields: list[str] = stat[stat.rfind(')') + 2:].split()
    if len(fields) < 2:
        return None
    try:
        return int(fields[1])
    except ValueError:
        return None


def _children_by_parent(proc_root: Path) -> dict[int, list[int]]:
    children: dict[int, list[int]] = {}
    entry: Path
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        parent: int | None = _parent_pid(entry)
        if parent is not None:
            children.setdefault(parent, []).append(int(entry.name))
    return children


def _descendants(root_pid: int, proc_root: Path) -> list[int]:
    children: dict[int, list[int]] = _children_by_parent(proc_root)
    found: list[int] = []
    pending: list[int] = list(children.get(root_pid, []))
    while pending:
        pid: int = pending.pop()
        found.append(pid)
        pending.extend(children.get(pid, []))
    return found


def _exe_name(proc_dir: Path) -> str | None:
    try:
        return os.path.basename(os.readlink(proc_dir / 'exe'))
    except OSError:
        return None


def _pss_bytes(proc_dir: Path) -> int | None:
    '''PSS from ``/proc/<pid>/smaps_rollup``, in bytes.'''
    try:
        rollup: str = (proc_dir / 'smaps_rollup').read_text()
    except OSError:
        return None
    line: str
    for line in rollup.splitlines():
        if line.startswith('Pss:'):
            parts: list[str] = line.split()
            try:
                return int(parts[1]) * 1024
            except (IndexError, ValueError):
                return None
    return None


def browser_memory(
    root_pid: int | None = None, proc_root: Path = PROC_ROOT,
) -> BrowserMemory | None:
    '''
    Sum the PSS of every Camoufox and Playwright driver process
    descended from *root_pid* (default: this process).

    Processes that exit or deny access mid-scan are skipped. Returns
    None when *proc_root* does not exist (not Linux).
    '''
    if not proc_root.is_dir():
        return None
    pid_root: int = os.getpid() if root_pid is None else root_pid
    browser_bytes: int = 0
    browser_processes: int = 0
    driver_bytes: int = 0
    driver_processes: int = 0
    pid: int
    for pid in _descendants(pid_root, proc_root):
        proc_dir: Path = proc_root / str(pid)
        exe: str | None = _exe_name(proc_dir)
        if exe is None:
            continue
        is_browser: bool = exe.startswith(BROWSER_EXE_PREFIX)
        if not is_browser and exe != DRIVER_EXE:
            continue
        pss: int | None = _pss_bytes(proc_dir)
        if pss is None:
            continue
        if is_browser:
            browser_bytes += pss
            browser_processes += 1
        else:
            driver_bytes += pss
            driver_processes += 1
    return BrowserMemory(
        browser_bytes=browser_bytes,
        browser_processes=browser_processes,
        driver_bytes=driver_bytes,
        driver_processes=driver_processes,
    )
