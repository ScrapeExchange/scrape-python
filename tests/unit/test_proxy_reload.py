'''
Unit tests for reloading proxies when the proxy files change:
scrape_exchange.proxy_file_watcher, the supervisor's rolling child
restart and the standalone scraper's graceful stop.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import os
import random
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange import scraper_supervisor
from scrape_exchange.proxy_file_watcher import ProxyFileWatcher
from scrape_exchange.scraper_runner import (
    PROXY_RELOAD_EXIT_CODE,
    _watch_proxy_files,
)
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    _ChildSlot,
    reassign_proxy_slices,
    spawn_children,
    supervise_children,
)

OLD: list[str] = ['http://proxy1.test:3128', 'http://proxy2.test:3128']
NEW: list[str] = ['http://proxy1.test:3128', 'http://proxy3.test:3128']


def _write(path: Path, proxies: list[str]) -> None:
    # Write to a temp file and rename, as editors and deploy tools do,
    # so the inode changes even within one mtime tick.
    tmp: Path = path.with_suffix('.tmp')
    tmp.write_text('\n'.join(proxies) + '\n', encoding='utf-8')
    os.replace(tmp, path)


class TestProxyFileWatcher(unittest.TestCase):

    def setUp(self) -> None:
        self._dir: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self._dir.cleanup)
        self.path: Path = Path(self._dir.name) / 'proxies.lst'
        _write(self.path, OLD)

    def test_unchanged_files_report_nothing(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        self.assertIsNone(watcher.check())
        self.assertIsNone(watcher.check())

    def test_changed_proxies_are_reported_once(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        self.assertIsNone(watcher.check())
        _write(self.path, NEW)
        self.assertEqual(watcher.check(), NEW)
        self.assertIsNone(watcher.check())
        self.assertEqual(watcher.proxies, NEW)

    def test_edit_before_first_check_is_seen(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        _write(self.path, NEW)
        self.assertEqual(watcher.check(), NEW)

    def test_reordered_or_commented_file_is_not_a_change(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        _write(self.path, ['# reordered'] + list(reversed(OLD)))
        self.assertIsNone(watcher.check())

    def test_broken_file_keeps_current_proxies(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        _write(self.path, ['http://proxy1.test:notaport'])
        with self.assertLogs(
            'scrape_exchange.proxy_file_watcher', 'WARNING',
        ):
            self.assertIsNone(watcher.check())
        self.assertEqual(watcher.proxies, OLD)
        # Fixing the file is picked up.
        _write(self.path, NEW)
        self.assertEqual(watcher.check(), NEW)

    def test_empty_or_missing_file_keeps_current_proxies(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path], OLD)
        _write(self.path, ['# nothing here'])
        with self.assertLogs(
            'scrape_exchange.proxy_file_watcher', 'WARNING',
        ):
            self.assertIsNone(watcher.check())
        self.path.unlink()
        with self.assertLogs(
            'scrape_exchange.proxy_file_watcher', 'WARNING',
        ):
            self.assertIsNone(watcher.check())
        self.assertEqual(watcher.proxies, OLD)

    def test_baseline_read_from_files(self) -> None:
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path])
        self.assertEqual(watcher.proxies, OLD)
        self.assertIsNone(watcher.check())
        _write(self.path, NEW)
        self.assertEqual(watcher.check(), NEW)

    def test_narrowed_startup_list_is_not_a_change(self) -> None:
        # Scrapers may use a subset of the file (random subset, first
        # N); that must not look like an edit.
        watcher: ProxyFileWatcher = ProxyFileWatcher([self.path])
        self.assertIsNone(watcher.check())

    def test_poll_respects_interval(self) -> None:
        now: list[float] = [0.0]
        watcher: ProxyFileWatcher = ProxyFileWatcher(
            [self.path], OLD, interval=60.0, clock=lambda: now[0],
        )
        _write(self.path, NEW)
        now[0] = 30.0
        self.assertIsNone(watcher.poll())
        now[0] = 61.0
        self.assertEqual(watcher.poll(), NEW)


class _ReloadableProcess:
    '''Popen stand-in: runs until it receives SIGTERM, then reports
    exit code 0 on the next poll.'''

    def __init__(self, pid: int) -> None:
        self.pid: int = pid
        self.signals: list[int] = []
        self.kill_calls: int = 0
        self._rc: int | None = None

    def poll(self) -> int | None:
        return self._rc

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)
        if sig == signal.SIGTERM:
            self._rc = 0

    def kill(self) -> None:
        self.kill_calls += 1
        self._rc = -9


class _StubbornProcess(_ReloadableProcess):
    '''Ignores SIGTERM; only kill() stops it.'''

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)


class _FakeWatcher:
    '''Reports *proxies* as changed on the poll numbered *on_poll*.'''

    def __init__(self, proxies: list[str], on_poll: int) -> None:
        self._proxies: list[str] = proxies
        self._on_poll: int = on_poll
        self.polls: int = 0

    def poll(self) -> list[str] | None:
        self.polls += 1
        if self.polls == self._on_poll:
            return list(self._proxies)
        return None


class TestSupervisorProxyReload(unittest.TestCase):

    def setUp(self) -> None:
        self._now: float = 1_000.0
        patcher = patch(
            'scrape_exchange.scraper_supervisor'
            '.multiprocess.mark_process_dead',
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.spawned: list[tuple[int, dict[str, str]]] = []
        self._next_pid: int = 100

    def _fake_spawn(self, slot: _ChildSlot) -> None:
        self.spawned.append((slot.instance, dict(slot.spawn_env)))
        self._next_pid += 1
        slot.process = _ReloadableProcess(self._next_pid)
        slot.spawn_time = self._now
        slot.respawn_at = None

    def _run(
        self, slots: list[_ChildSlot], watcher: object, ticks: int,
        grace: float = 60.0, split: bool = False,
        changed: list[list[str]] | None = None,
    ) -> list[list[tuple[int, int | None]]]:
        '''Run the supervise loop for *ticks* iterations and record,
        per tick, each slot's (instance, pid or None).'''

        shutdown_state: dict[str, float | None] = {'deadline': None}
        history: list[list[tuple[int, int | None]]] = []

        def fake_sleep(secs: float) -> None:
            self._now += secs
            history.append([
                (s.instance, s.process.pid if s.process else None)
                for s in slots
            ])
            if len(history) >= ticks:
                shutdown_state['deadline'] = self._now + 3600
                for s in slots:
                    if s.process is not None:
                        s.process.kill()

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', self._fake_spawn,
        ), patch.object(
            scraper_supervisor.time, 'sleep', fake_sleep,
        ):
            supervise_children(
                'test', slots, shutdown_state,
                proxy_watcher=watcher,
                reload_grace_seconds=grace,
                split_proxy_pool=split,
                on_proxies_changed=(
                    changed.append if changed is not None else None
                ),
            )
        return history

    def _slot(
        self, instance: int, process: object,
        proxies: list[str] | None = None,
    ) -> _ChildSlot:
        chunk: list[str] = OLD if proxies is None else proxies
        return _ChildSlot(
            instance=instance,
            spawn_env={'PROXIES': ','.join(chunk)},
            spawn_argv=['/bin/true'],
            process=process,
            spawn_time=self._now,
            proxy_target=len(chunk),
        )

    def test_children_restart_one_at_a_time(self) -> None:
        first: _ReloadableProcess = _ReloadableProcess(1)
        second: _ReloadableProcess = _ReloadableProcess(2)
        slots: list[_ChildSlot] = [
            self._slot(1, first), self._slot(2, second),
        ]
        history = self._run(slots, _FakeWatcher(NEW, on_poll=1), 6)

        self.assertEqual(first.signals, [signal.SIGTERM])
        self.assertEqual(second.signals, [signal.SIGTERM])
        self.assertEqual([i for i, _ in self.spawned], [1, 2])
        # Never were both children down at the same time.
        for tick in history:
            self.assertTrue(
                any(pid is not None for _, pid in tick), tick,
            )
        # A reload is not a crash: no backoff was applied.
        self.assertEqual(slots[0].backoff, slots[1].backoff)

    def test_change_is_reported_once(self) -> None:
        changed: list[list[str]] = []
        slots: list[_ChildSlot] = [
            self._slot(1, _ReloadableProcess(1)),
        ]
        self._run(
            slots, _FakeWatcher(NEW, on_poll=1), 4, changed=changed,
        )
        self.assertEqual(changed, [NEW])

    def test_full_pool_children_get_new_list(self) -> None:
        slots: list[_ChildSlot] = [
            self._slot(1, _ReloadableProcess(1)),
            self._slot(2, _ReloadableProcess(2)),
        ]
        self._run(slots, _FakeWatcher(NEW, on_poll=1), 6)
        for _, env in self.spawned:
            self.assertEqual(env['PROXIES'], ','.join(NEW))

    def test_split_pool_restarts_only_changed_slices(self) -> None:
        a: str = 'http://slice1.test:1'
        b: str = 'http://slice2.test:1'
        c: str = 'http://slice3.test:1'
        first: _ReloadableProcess = _ReloadableProcess(1)
        second: _ReloadableProcess = _ReloadableProcess(2)
        slots: list[_ChildSlot] = [
            self._slot(1, first, [a]), self._slot(2, second, [b]),
        ]
        # b is removed, c is new: only child 2 restarts, with c.
        self._run(
            slots, _FakeWatcher([a, c], on_poll=1), 6, split=True,
        )
        self.assertEqual(first.signals, [])
        self.assertEqual(second.signals, [signal.SIGTERM])
        self.assertEqual(self.spawned, [(2, {'PROXIES': c})])

    def test_split_pool_child_without_proxies_is_stopped(self) -> None:
        a: str = 'http://slice1.test:1'
        b: str = 'http://slice2.test:1'
        second: _ReloadableProcess = _ReloadableProcess(2)
        slots: list[_ChildSlot] = [
            self._slot(1, _ReloadableProcess(1), [a]),
            self._slot(2, second, [b]),
        ]
        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', 'ERROR',
        ):
            self._run(
                slots, _FakeWatcher([a], on_poll=1), 6, split=True,
            )
        self.assertEqual(second.signals, [signal.SIGTERM])
        self.assertEqual(self.spawned, [])
        self.assertTrue(slots[1].no_proxies)

    def test_no_change_no_restart(self) -> None:
        proc: _ReloadableProcess = _ReloadableProcess(1)
        self._run(
            [self._slot(1, proc)], _FakeWatcher(NEW, on_poll=-1), 4,
        )
        self.assertEqual(proc.signals, [])
        self.assertEqual(self.spawned, [])

    def test_child_ignoring_sigterm_is_killed_after_grace(self) -> None:
        stubborn: _StubbornProcess = _StubbornProcess(1)
        self._run(
            [self._slot(1, stubborn)], _FakeWatcher(NEW, on_poll=1),
            ticks=8, grace=2.0,
        )
        self.assertEqual(stubborn.signals, [signal.SIGTERM])
        self.assertGreaterEqual(stubborn.kill_calls, 1)
        self.assertEqual([i for i, _ in self.spawned], [1])


class TestReassignProxySlices(unittest.TestCase):

    def test_unchanged_list_keeps_slices(self) -> None:
        slices: list[list[str]] = [['a', 'b'], ['c', 'd']]
        self.assertEqual(
            reassign_proxy_slices(slices, [2, 2], ['a', 'b', 'c', 'd']),
            slices,
        )

    def test_removed_proxy_is_replaced_by_spare(self) -> None:
        result: list[list[str]] = reassign_proxy_slices(
            [['a', 'b'], ['c', 'd']], [2, 2], ['a', 'c', 'd', 'e', 'f'],
            rng=random.Random(1),
        )
        self.assertEqual(result[1], ['c', 'd'])
        self.assertEqual(result[0][0], 'a')
        self.assertIn(result[0][1], ('e', 'f'))

    def test_slices_stay_disjoint(self) -> None:
        result: list[list[str]] = reassign_proxy_slices(
            [['a'], ['b'], ['c']], [2, 2, 2], ['a', 'x', 'y', 'z'],
            rng=random.Random(2),
        )
        flat: list[str] = [p for chunk in result for p in chunk]
        self.assertEqual(len(flat), len(set(flat)))
        self.assertEqual(sorted(flat), ['a', 'x', 'y', 'z'])

    def test_not_enough_proxies_leaves_short_slices(self) -> None:
        result: list[list[str]] = reassign_proxy_slices(
            [['a'], ['b']], [1, 1], ['a'],
        )
        self.assertEqual(result, [['a'], []])


class TestSplitChildrenUseTheirSlice(unittest.TestCase):

    def _spawn(self, split: bool) -> list[dict[str, str]]:
        config: SupervisorConfig = SupervisorConfig(
            scraper_label='test',
            num_processes_env_var='TEST_NUM_PROCESSES',
            num_processes=2,
            concurrency=1,
            proxies=OLD,
            metrics_port=0,
            log_file=None,
            split_proxy_pool=split,
        )
        envs: list[dict[str, str]] = []

        def fake_spawn(slot: _ChildSlot) -> None:
            envs.append(dict(slot.spawn_env))

        with patch.dict(
            os.environ, {'PROXY_FILES': '/data/proxies/a.lst'},
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ), patch.object(scraper_supervisor.time, 'sleep'):
            spawn_children(config, [[OLD[0]], [OLD[1]]])
        return envs

    def test_split_pool_blanks_proxy_files(self) -> None:
        for env in self._spawn(split=True):
            self.assertEqual(env['PROXY_FILES'], '')

    def test_full_pool_keeps_proxy_files(self) -> None:
        for env in self._spawn(split=False):
            self.assertEqual(env['PROXY_FILES'], '/data/proxies/a.lst')

    def test_child_settings_use_the_slice(self) -> None:
        from scrape_exchange.settings import ScraperSettings
        env: dict[str, str] = self._spawn(split=True)[1]
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'p.lst'
            _write(path, OLD)
            with patch.dict(os.environ, {
                'PROXY_FILES': env['PROXY_FILES'],
                'PROXIES': env['PROXIES'],
            }):
                settings: ScraperSettings = ScraperSettings(
                    _env_file=None, _cli_parse_args=False,
                )
                self.assertEqual(list(settings.proxies), [OLD[1]])


class TestStandaloneProxyReload(unittest.IsolatedAsyncioTestCase):

    async def test_change_clears_cooldowns_and_stops_scraper(self) -> None:
        watcher: MagicMock = MagicMock()
        watcher.interval = 0.0
        watcher.check = MagicMock(side_effect=[None, NEW])
        limiter: MagicMock = MagicMock()
        limiter.clear_proxy_auth_cooldowns = AsyncMock(return_value=1)
        reloads: list[bool] = []

        async def scraper() -> None:
            await asyncio.sleep(3600)

        main_task: asyncio.Task[None] = asyncio.create_task(scraper())
        await _watch_proxy_files(
            watcher, limiter, main_task, 'test',
            on_reload=lambda: reloads.append(True),
        )
        with self.assertRaises(asyncio.CancelledError):
            await main_task
        limiter.clear_proxy_auth_cooldowns.assert_awaited_once_with(NEW)
        self.assertEqual(reloads, [True])

    def test_reload_exit_code_is_nonzero(self) -> None:
        self.assertNotEqual(PROXY_RELOAD_EXIT_CODE, 0)


if __name__ == '__main__':
    unittest.main()
