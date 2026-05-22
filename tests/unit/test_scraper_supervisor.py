'''
Unit tests for scrape_exchange/scraper_supervisor.py.

Exercises the pure helpers (``split_proxies``,
``chunks_are_disjoint_cover``, ``publish_config_metrics``) and the
pure bits of ``spawn_children``. The subprocess spawn itself is
patched so we can assert on the child env without actually
forking.
'''

import os
import subprocess
import time
import unittest

from unittest.mock import patch, MagicMock

from scrape_exchange.worker_id import get_worker_id
import scrape_exchange.scraper_supervisor as scraper_supervisor
from scrape_exchange.scraper_supervisor import (
    METRIC_CONCURRENCY,
    METRIC_NUM_PROCESSES,
    SupervisorConfig,
    _kill_children,
    _should_escalate,
    chunks_are_disjoint_cover,
    install_signal_forwarders,
    publish_config_metrics,
    proxy_pool_for_children,
    spawn_children,
    split_proxies,
)


class TestSplitProxies(unittest.TestCase):

    def test_zero_or_negative_raises(self) -> None:
        with self.assertRaises(ValueError):
            split_proxies(['a', 'b'], 0)
        with self.assertRaises(ValueError):
            split_proxies(['a', 'b'], -1)

    def test_round_robin_even(self) -> None:
        chunks = split_proxies(['a', 'b', 'c', 'd'], 2)
        self.assertEqual(chunks, [['a', 'c'], ['b', 'd']])

    def test_round_robin_uneven(self) -> None:
        chunks = split_proxies(['a', 'b', 'c', 'd', 'e'], 2)
        self.assertEqual(chunks, [['a', 'c', 'e'], ['b', 'd']])

    def test_more_chunks_than_proxies_has_empty(self) -> None:
        chunks = split_proxies(['a', 'b'], 4)
        self.assertEqual(chunks, [['a'], ['b'], [], []])

    def test_preserves_input_order_within_chunks(self) -> None:
        chunks = split_proxies(['p1', 'p2', 'p3'], 3)
        self.assertEqual(chunks, [['p1'], ['p2'], ['p3']])


class TestChunksAreDisjointCover(unittest.TestCase):

    def test_disjoint_round_robin_split_is_ok(self) -> None:
        proxies: list[str] = ['a', 'b', 'c', 'd']
        chunks = split_proxies(proxies, 2)
        self.assertTrue(chunks_are_disjoint_cover(chunks, proxies))

    def test_overlap_rejected(self) -> None:
        proxies: list[str] = ['a', 'b', 'c']
        chunks = [['a', 'b'], ['b', 'c']]  # 'b' in both
        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='ERROR',
        ):
            self.assertFalse(
                chunks_are_disjoint_cover(chunks, proxies),
            )

    def test_drop_rejected(self) -> None:
        proxies: list[str] = ['a', 'b', 'c']
        chunks = [['a'], ['b']]  # 'c' missing
        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='ERROR',
        ):
            self.assertFalse(
                chunks_are_disjoint_cover(chunks, proxies),
            )

    def test_extra_rejected(self) -> None:
        proxies: list[str] = ['a', 'b']
        chunks = [['a'], ['b'], ['c']]
        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='ERROR',
        ):
            self.assertFalse(
                chunks_are_disjoint_cover(chunks, proxies),
            )

    def test_different_set_rejected(self) -> None:
        proxies: list[str] = ['a', 'b']
        chunks = [['x'], ['y']]
        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='ERROR',
        ):
            self.assertFalse(
                chunks_are_disjoint_cover(chunks, proxies),
            )


class TestProxyPoolForChildren(unittest.TestCase):

    def test_each_child_receives_full_proxy_pool(self) -> None:
        proxies: list[str] = ['a', 'b', 'c']
        chunks: list[list[str]] = proxy_pool_for_children(
            proxies, 2,
        )

        self.assertEqual(chunks, [proxies, proxies])
        self.assertIsNot(chunks[0], proxies)
        self.assertIsNot(chunks[1], proxies)

    def test_zero_or_negative_raises(self) -> None:
        with self.assertRaises(ValueError):
            proxy_pool_for_children(['a'], 0)


class TestPublishConfigMetrics(unittest.TestCase):

    def setUp(self) -> None:
        # Ensure the labels exist on the gauge so .labels(...)._value
        # has a defined value to inspect. The metric is process-wide,
        # so use unique scraper labels to avoid interference.
        self.scraper_label: str = 'pytest-scraper'

    def test_supervisor_gauges(self) -> None:
        publish_config_metrics(
            role='supervisor',
            scraper_label=self.scraper_label,
            num_processes=4, concurrency=3,
        )
        wid: str = get_worker_id()
        np_val: float = METRIC_NUM_PROCESSES.labels(
            platform='youtube',
            role='supervisor',
            scraper=self.scraper_label,
            worker_id=wid,
        )._value.get()
        conc_val: float = METRIC_CONCURRENCY.labels(
            platform='youtube',
            role='supervisor',
            scraper=self.scraper_label,
            worker_id=wid,
        )._value.get()
        self.assertEqual(np_val, 4.0)
        self.assertEqual(conc_val, 3.0)

    def test_worker_gauges_separate_from_supervisor(self) -> None:
        publish_config_metrics(
            role='supervisor',
            scraper_label=self.scraper_label,
            num_processes=4, concurrency=3,
        )
        publish_config_metrics(
            role='worker',
            scraper_label=self.scraper_label,
            num_processes=1, concurrency=3,
        )
        wid: str = get_worker_id()
        sup_np: float = METRIC_NUM_PROCESSES.labels(
            platform='youtube',
            role='supervisor',
            scraper=self.scraper_label,
            worker_id=wid,
        )._value.get()
        w_np: float = METRIC_NUM_PROCESSES.labels(
            platform='youtube',
            role='worker',
            scraper=self.scraper_label,
            worker_id=wid,
        )._value.get()
        self.assertEqual(sup_np, 4.0)
        self.assertEqual(w_np, 1.0)


class TestSpawnChildrenEnv(unittest.TestCase):
    '''
    Verify that spawn_children constructs the right per-child env
    without actually forking. We patch ``subprocess.Popen`` and
    capture the ``env`` kwarg for each invocation.
    '''

    def _make_config(
        self, log_file: str | None = None,
        log_file_env_var: str | None = 'CHANNEL_LOG_FILE',
    ) -> SupervisorConfig:
        return SupervisorConfig(
            scraper_label='channel',
            num_processes_env_var='CHANNEL_NUM_PROCESSES',
            num_processes=3,
            concurrency=5,
            proxies=['http://a', 'http://b', 'http://c'],
            metrics_port=9600,
            log_file=log_file,
            log_file_env_var=log_file_env_var,
            metrics_port_env_var='CHANNEL_METRICS_PORT',
        )

    def test_env_overrides_per_child(self) -> None:
        config: SupervisorConfig = self._make_config(
            log_file='/var/log/channel.log',
        )
        chunks: list[list[str]] = [
            ['http://a'], ['http://b'], ['http://c'],
        ]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 12345
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(len(captured_envs), 3)
        # Worker 1 — check process count, proxies, log file
        self.assertEqual(
            captured_envs[0]['CHANNEL_NUM_PROCESSES'], '1',
        )
        self.assertEqual(captured_envs[0]['PROXIES'], 'http://a')
        # Children no longer receive per-worker METRICS_PORT;
        # they inherit PROMETHEUS_MULTIPROC_DIR instead.
        self.assertNotIn('METRICS_PORT', captured_envs[0])
        self.assertNotIn(
            'CHANNEL_METRICS_PORT', captured_envs[0],
        )
        self.assertEqual(
            captured_envs[0]['LOG_FILE'],
            '/var/log/channel-1.log',
        )
        # The scraper-specific log-file env var must also be
        # written so the child's pydantic settings don't resolve
        # the higher-priority alias back to the parent's base
        # path.
        self.assertEqual(
            captured_envs[0]['CHANNEL_LOG_FILE'],
            '/var/log/channel-1.log',
        )
        # Worker 2 — no METRICS_PORT, correct log file
        self.assertNotIn('METRICS_PORT', captured_envs[1])
        self.assertNotIn(
            'CHANNEL_METRICS_PORT', captured_envs[1],
        )
        self.assertEqual(
            captured_envs[1]['LOG_FILE'],
            '/var/log/channel-2.log',
        )
        self.assertEqual(
            captured_envs[1]['CHANNEL_LOG_FILE'],
            '/var/log/channel-2.log',
        )
        # Worker 3 — no METRICS_PORT, correct log file
        self.assertNotIn('METRICS_PORT', captured_envs[2])
        self.assertNotIn(
            'CHANNEL_METRICS_PORT', captured_envs[2],
        )
        self.assertEqual(
            captured_envs[2]['LOG_FILE'],
            '/var/log/channel-3.log',
        )
        self.assertEqual(
            captured_envs[2]['CHANNEL_LOG_FILE'],
            '/var/log/channel-3.log',
        )

    def test_dev_stdout_log_file_is_not_suffixed(self) -> None:
        '''
        ``/dev/stdout`` is a stream target; suffixing it would
        produce ``/dev/stdout-1`` which is nonsense. Each child
        must inherit the parent's stdout unchanged.
        '''
        config: SupervisorConfig = self._make_config(
            log_file='/dev/stdout',
        )
        chunks: list[list[str]] = [['http://a'], ['http://b']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_FILE', None)
            os.environ.pop('CHANNEL_LOG_FILE', None)
            with patch(
                'scrape_exchange.scraper_supervisor.subprocess.Popen',
                side_effect=fake_popen,
            ):
                spawn_children(config, chunks)

        for env in captured_envs:
            self.assertNotIn('LOG_FILE', env)
            self.assertNotIn('CHANNEL_LOG_FILE', env)

    def test_no_log_file_env_var_falls_back_to_base_log_file(
        self,
    ) -> None:
        '''
        A scraper without a scraper-specific log file env var
        (older integrations or tests) should still write the
        suffixed path to plain ``LOG_FILE``.
        '''
        config: SupervisorConfig = self._make_config(
            log_file='/var/log/channel.log',
            log_file_env_var=None,
        )
        chunks: list[list[str]] = [['http://a']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(
            captured_envs[0]['LOG_FILE'],
            '/var/log/channel-1.log',
        )
        self.assertNotIn('CHANNEL_LOG_FILE', captured_envs[0])

    def test_no_log_file_means_no_log_file_env_override(
        self,
    ) -> None:
        config: SupervisorConfig = self._make_config(log_file=None)
        chunks: list[list[str]] = [['http://a'], ['http://b']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        # Ensure the parent env has no pre-existing LOG_FILE so we
        # can assert the child env doesn't pick it up from anywhere.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_FILE', None)
            with patch(
                'scrape_exchange.scraper_supervisor.subprocess.Popen',
                side_effect=fake_popen,
            ):
                spawn_children(config, chunks)

        self.assertNotIn('LOG_FILE', captured_envs[0])
        self.assertNotIn('LOG_FILE', captured_envs[1])

    def test_multi_proxy_chunk_serialised_csv(self) -> None:
        config: SupervisorConfig = self._make_config()
        chunks: list[list[str]] = [
            ['http://a', 'http://c', 'http://e'],
            ['http://b', 'http://d'],
        ]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(
            captured_envs[0]['PROXIES'],
            'http://a,http://c,http://e',
        )
        self.assertEqual(
            captured_envs[1]['PROXIES'],
            'http://b,http://d',
        )


class TestShouldEscalate(unittest.TestCase):

    def test_none_state_returns_false(self) -> None:
        self.assertFalse(_should_escalate(None))

    def test_no_deadline_returns_false(self) -> None:
        self.assertFalse(
            _should_escalate({'deadline': None})
        )

    def test_future_deadline_returns_false(self) -> None:
        future: float = time.monotonic() + 3600
        self.assertFalse(
            _should_escalate({'deadline': future})
        )

    def test_past_deadline_returns_true(self) -> None:
        past: float = time.monotonic() - 1
        self.assertTrue(
            _should_escalate({'deadline': past})
        )


class TestKillChildren(unittest.TestCase):

    def test_sends_kill_to_running_children(self) -> None:
        alive: MagicMock = MagicMock()
        alive.poll.return_value = None

        dead: MagicMock = MagicMock()
        dead.poll.return_value = 0

        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='WARNING',
        ):
            _kill_children('test', [alive, dead])

        alive.kill.assert_called_once()
        dead.kill.assert_not_called()

    def test_ignores_process_lookup_error(self) -> None:
        child: MagicMock = MagicMock()
        child.poll.return_value = None
        child.kill.side_effect = ProcessLookupError

        with self.assertLogs(
            'scrape_exchange.scraper_supervisor', level='WARNING',
        ):
            _kill_children('test', [child])
        child.kill.assert_called_once()


class TestInstallSignalForwarders(unittest.TestCase):

    def test_sets_deadline_on_first_signal(self) -> None:
        '''
        Simulates a SIGTERM by capturing the handler
        installed via signal.signal and invoking it directly.
        '''
        from scrape_exchange.scraper_supervisor import _ChildSlot
        shutdown_state: dict[str, float | None] = {
            'deadline': None,
        }
        handlers: dict[int, object] = {}

        def fake_signal(
            signum: int, handler: object,
        ) -> None:
            handlers[signum] = handler

        child: MagicMock = MagicMock()
        child.poll.return_value = None
        slot: _ChildSlot = _ChildSlot(
            instance=1,
            spawn_env={},
            spawn_argv=[],
            process=child,
            spawn_time=time.monotonic(),
        )

        with patch(
            'scrape_exchange.scraper_supervisor.signal.signal',
            side_effect=fake_signal,
        ):
            install_signal_forwarders(
                [slot], shutdown_state, grace_seconds=10,
            )

        import signal
        handler = handlers[signal.SIGTERM]
        handler(signal.SIGTERM, None)

        self.assertIsNotNone(shutdown_state['deadline'])
        child.send_signal.assert_called_once_with(
            signal.SIGTERM,
        )

    def test_deadline_set_only_once(self) -> None:
        from scrape_exchange.scraper_supervisor import _ChildSlot
        shutdown_state: dict[str, float | None] = {
            'deadline': None,
        }
        handlers: dict[int, object] = {}

        def fake_signal(
            signum: int, handler: object,
        ) -> None:
            handlers[signum] = handler

        child: MagicMock = MagicMock()
        child.poll.return_value = None
        slot: _ChildSlot = _ChildSlot(
            instance=1,
            spawn_env={},
            spawn_argv=[],
            process=child,
            spawn_time=time.monotonic(),
        )

        with patch(
            'scrape_exchange.scraper_supervisor.signal.signal',
            side_effect=fake_signal,
        ):
            install_signal_forwarders(
                [slot], shutdown_state, grace_seconds=10,
            )

        import signal
        handler = handlers[signal.SIGTERM]
        handler(signal.SIGTERM, None)
        first_deadline: float = shutdown_state['deadline']

        handler(signal.SIGTERM, None)
        self.assertEqual(
            shutdown_state['deadline'], first_deadline,
        )


class TestComputeNextBackoff(unittest.TestCase):
    '''Per-child backoff: doubles on consecutive crashes,
    caps at 60s, resets to 1s when the previous run was
    stable for >= 60s.'''

    def test_doubles_below_cap(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _compute_next_backoff,
        )
        seq: list[float] = []
        b: float = 1.0
        for _ in range(8):
            b = _compute_next_backoff(b, ran_seconds=0.0)
            seq.append(b)
        self.assertEqual(
            seq,
            [2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0],
        )

    def test_holds_at_cap(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _compute_next_backoff,
        )
        self.assertEqual(
            _compute_next_backoff(60.0, ran_seconds=5.0), 60.0,
        )

    def test_resets_when_ran_at_threshold(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _compute_next_backoff,
        )
        self.assertEqual(
            _compute_next_backoff(32.0, ran_seconds=60.0), 1.0,
        )

    def test_resets_when_ran_well_past_threshold(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _compute_next_backoff,
        )
        self.assertEqual(
            _compute_next_backoff(32.0, ran_seconds=600.0), 1.0,
        )

    def test_just_below_threshold_still_doubles(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            _compute_next_backoff,
        )
        self.assertEqual(
            _compute_next_backoff(32.0, ran_seconds=59.999),
            60.0,
        )


class TestSupervisorRespawnsMetric(unittest.TestCase):
    '''METRIC_SUPERVISOR_RESPAWNS must carry scraper and
    instance labels so the dashboard can per-slot flap-rate.'''

    def test_metric_has_expected_labelnames(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SUPERVISOR_RESPAWNS,
        )
        self.assertEqual(
            METRIC_SUPERVISOR_RESPAWNS._labelnames,
            ('scraper', 'instance'),
        )

    def test_metric_can_be_incremented(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SUPERVISOR_RESPAWNS,
        )
        METRIC_SUPERVISOR_RESPAWNS.labels(
            scraper='unit-test', instance='99',
        ).inc()  # smoke


class TestChildSlotConstruction(unittest.TestCase):
    '''spawn_children returns one _ChildSlot per chunk with the
    spawn args needed to relaunch the child later.'''

    def test_returns_one_slot_per_chunk(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            SupervisorConfig, _ChildSlot, spawn_children,
        )
        config: SupervisorConfig = SupervisorConfig(
            scraper_label='test',
            num_processes_env_var='TEST_NUM_PROCESSES',
            num_processes=2,
            concurrency=4,
            proxies=['http://1:1', 'http://2:2'],
            metrics_port=9000,
            log_file=None,
        )
        with patch.object(
            scraper_supervisor.subprocess, 'Popen',
        ) as fake_popen:
            fake_popen.return_value = MagicMock(pid=42)
            slots: list[_ChildSlot] = spawn_children(
                config,
                [['http://1:1'], ['http://2:2']],
            )
        self.assertEqual(len(slots), 2)
        for slot in slots:
            self.assertIsNotNone(slot.process)
            self.assertEqual(slot.backoff, 1.0)
            self.assertIsNotNone(slot.spawn_time)
            self.assertIsNone(slot.respawn_at)
        self.assertEqual(
            [s.instance for s in slots], [1, 2],
        )

    def test_slot_captures_spawn_env(self) -> None:
        '''The slot must remember the env it was spawned with so a
        respawn can replay it without re-deriving.'''
        from scrape_exchange.scraper_supervisor import (
            SupervisorConfig, spawn_children,
        )
        config: SupervisorConfig = SupervisorConfig(
            scraper_label='test',
            num_processes_env_var='TEST_NUM_PROCESSES',
            num_processes=1,
            concurrency=1,
            proxies=['http://only:1'],
            metrics_port=9000,
            log_file=None,
        )
        with patch.object(
            scraper_supervisor.subprocess, 'Popen',
        ) as fake_popen:
            fake_popen.return_value = MagicMock(pid=1)
            slots = spawn_children(
                config, [['http://only:1']],
            )
        self.assertEqual(len(slots), 1)
        slot = slots[0]
        self.assertEqual(
            slot.spawn_env['PROXIES'], 'http://only:1',
        )
        # Children no longer receive a per-worker METRICS_PORT;
        # they write gauge files to PROMETHEUS_MULTIPROC_DIR.
        self.assertNotIn('METRICS_PORT', slot.spawn_env)
        self.assertEqual(
            slot.spawn_env['TEST_NUM_PROCESSES'], '1',
        )

    def test_slot_captures_spawn_argv(self) -> None:
        '''The slot must capture spawn_argv so _spawn_one_slot
        can replay the exact command on respawn without
        re-deriving it.'''
        import sys
        from scrape_exchange.scraper_supervisor import (
            SupervisorConfig, spawn_children,
        )
        config: SupervisorConfig = SupervisorConfig(
            scraper_label='test',
            num_processes_env_var='TEST_NUM_PROCESSES',
            num_processes=1,
            concurrency=1,
            proxies=['http://only:1'],
            metrics_port=9000,
            log_file=None,
        )
        with patch.object(
            scraper_supervisor.subprocess, 'Popen',
        ) as fake_popen:
            fake_popen.return_value = MagicMock(pid=1)
            slots = spawn_children(
                config, [['http://only:1']],
            )
        self.assertEqual(len(slots), 1)
        slot = slots[0]
        # spawn_argv[0] must be the current Python interpreter so
        # respawned children use the same venv.
        self.assertEqual(slot.spawn_argv[0], sys.executable)
        # spawn_argv[1] is the resolved absolute path to the
        # calling script (os.path.abspath(sys.argv[0])).
        self.assertEqual(
            slot.spawn_argv[1], os.path.abspath(sys.argv[0]),
        )


class TestInstallSignalForwardersLiveLookup(unittest.TestCase):

    def test_replaced_process_receives_signal(self) -> None:
        '''Respawning a slot's process AFTER install must still
        cause the new process to receive forwarded signals.
        Pins the central correctness claim of the refactor.'''
        import signal
        from scrape_exchange.scraper_supervisor import (
            _ChildSlot, install_signal_forwarders,
        )
        handlers: dict[int, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            handlers[signum] = handler

        initial_proc: MagicMock = MagicMock()
        initial_proc.poll.return_value = None
        slot: _ChildSlot = _ChildSlot(
            instance=1, spawn_env={}, spawn_argv=[],
            process=initial_proc,
            spawn_time=time.monotonic(),
        )
        shutdown_state: dict[str, float | None] = {
            'deadline': None,
        }

        with patch.object(
            scraper_supervisor.signal, 'signal', fake_signal,
        ):
            install_signal_forwarders([slot], shutdown_state)

        # Replace the slot's process AFTER install — simulates a
        # respawn cycle where the old child died and a new one was
        # spawned to take its place.
        new_proc: MagicMock = MagicMock()
        new_proc.poll.return_value = None
        slot.process = new_proc

        # Trigger the SIGTERM handler.
        handlers[signal.SIGTERM](signal.SIGTERM, None)

        # The handler must have signalled the NEW process, not the
        # initial one.
        new_proc.send_signal.assert_called_once_with(signal.SIGTERM)
        initial_proc.send_signal.assert_not_called()


class _FakeProcess:
    '''Minimal Popen-like stub. Tests drive ``poll_returns`` (a
    list of values returned in order on consecutive .poll() calls).
    .pid is fixed; .terminate / .kill record their calls.'''

    def __init__(
        self, pid: int, poll_returns: list[int | None],
    ) -> None:
        self.pid: int = pid
        self._poll_returns: list[int | None] = list(poll_returns)
        self.kill_calls: int = 0
        self.terminate_calls: int = 0

    def poll(self) -> int | None:
        if not self._poll_returns:
            return 0  # final call: report a clean exit
        return self._poll_returns.pop(0)

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1


class TestSuperviseChildren(unittest.TestCase):
    '''supervise_children: per-slot respawn with backoff and reset.

    Each test drives a controllable monotonic clock so backoff
    timings are deterministic.'''

    def setUp(self) -> None:
        self._now: float = 1_000.0
        # mark_process_dead requires PROMETHEUS_MULTIPROC_DIR to
        # be set; patch it out so these unit tests don't depend
        # on the filesystem or environment variable.
        patcher = patch(
            'scrape_exchange.scraper_supervisor'
            '.multiprocess.mark_process_dead',
        )
        self._mock_mark_dead = patcher.start()
        self.addCleanup(patcher.stop)

    def _clock(self) -> float:
        return self._now

    def _advance(self, seconds: float) -> None:
        self._now += seconds

    def _make_slot(
        self,
        instance: int,
        process: _FakeProcess,
        spawn_time: float | None = None,
    ) -> '_ChildSlot':
        from scrape_exchange.scraper_supervisor import _ChildSlot
        return _ChildSlot(
            instance=instance,
            spawn_env={},
            spawn_argv=['/bin/true'],
            process=process,
            spawn_time=spawn_time
            if spawn_time is not None else self._now,
        )

    def test_sibling_isolation_one_crash_one_respawn(
        self,
    ) -> None:
        '''A single crash schedules one respawn; sibling
        untouched.'''
        from scrape_exchange.scraper_supervisor import (
            supervise_children,
        )
        crashed_proc: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[1],
        )
        live_proc: _FakeProcess = _FakeProcess(
            pid=2, poll_returns=[None] * 50,
        )
        crashed_slot = self._make_slot(1, crashed_proc)
        live_slot = self._make_slot(2, live_proc)

        spawn_calls: list[int] = []

        def fake_spawn(slot: object) -> None:
            spawn_calls.append(slot.instance)
            slot.process = _FakeProcess(
                pid=99, poll_returns=[None] * 50,
            )
            slot.spawn_time = self._now

        # Run the loop for ~3 seconds of fake time, enough for the
        # 1s backoff to elapse and respawn to fire.
        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ):
            # Drive clock forward in 0.6s ticks until respawn fires
            # plus one extra tick. Use a per-iteration shutdown hook
            # to break out cleanly.
            shutdown_state: dict[str, float | None] = {
                'deadline': None,
            }
            ticks: list[float] = []

            def fake_sleep(secs: float) -> None:
                ticks.append(secs)
                self._advance(secs)
                if len(ticks) >= 5:
                    shutdown_state['deadline'] = self._now

            with patch.object(
                scraper_supervisor.time, 'sleep', fake_sleep,
            ):
                rc: int = supervise_children(
                    'test', [crashed_slot, live_slot],
                    shutdown_state,
                )
        self.assertEqual(rc, 0)
        self.assertIn(1, spawn_calls)
        self.assertNotIn(2, spawn_calls)

    def test_clean_exit_does_not_respawn(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            supervise_children,
        )
        clean_proc: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[0],
        )
        slot = self._make_slot(1, clean_proc)

        spawn_calls: list[int] = []

        def fake_spawn(s: object) -> None:
            spawn_calls.append(s.instance)

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ), patch.object(
            scraper_supervisor.time, 'sleep',
            side_effect=lambda s: self._advance(s),
        ):
            rc: int = supervise_children(
                'test', [slot], {'deadline': None},
            )
        self.assertEqual(rc, 0)
        self.assertEqual(spawn_calls, [])

    def test_shutdown_skips_respawn(self) -> None:
        '''A crash that arrives after shutdown is requested must
        not schedule a respawn.'''
        from scrape_exchange.scraper_supervisor import (
            supervise_children,
        )
        crashed_proc: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[1],
        )
        slot = self._make_slot(1, crashed_proc)

        spawn_calls: list[int] = []

        def fake_spawn(s: object) -> None:
            spawn_calls.append(s.instance)

        shutdown_state: dict[str, float | None] = {
            'deadline': self._now + 30.0,
        }

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ), patch.object(
            scraper_supervisor.time, 'sleep',
            side_effect=lambda s: self._advance(s),
        ):
            rc: int = supervise_children(
                'test', [slot], shutdown_state,
            )
        self.assertEqual(rc, 0)
        self.assertEqual(spawn_calls, [])

    def test_repeated_crashes_double_backoff(self) -> None:
        '''Two crashes within the stable threshold double the
        backoff.'''
        from scrape_exchange.scraper_supervisor import (
            supervise_children, _ChildSlot,
        )
        # First process crashes immediately; respawn produces a
        # second process that also crashes immediately.
        first: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[1],
        )
        slot: _ChildSlot = self._make_slot(1, first)
        observed_backoffs: list[float] = []

        def fake_spawn(s: _ChildSlot) -> None:
            observed_backoffs.append(s.backoff)
            s.process = _FakeProcess(pid=99, poll_returns=[1])
            s.spawn_time = self._now

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ):
            shutdown_state: dict[str, float | None] = {
                'deadline': None,
            }
            ticks: int = 0

            def fake_sleep(secs: float) -> None:
                nonlocal ticks
                ticks += 1
                self._advance(secs)
                # 40 ticks * 0.5s = 20s, enough for backoffs
                # 2s + 4s + 8s = 14s before the 3rd respawn
                # fires, with headroom before shutdown.
                if ticks >= 40:
                    shutdown_state['deadline'] = self._now

            with patch.object(
                scraper_supervisor.time, 'sleep', fake_sleep,
            ):
                supervise_children(
                    'test', [slot], shutdown_state,
                )
        # First respawn fires with backoff=2 (doubled from 1).
        # Second respawn — also a crash within the threshold —
        # uses backoff=4. Etc.
        self.assertEqual(
            observed_backoffs[:3], [2.0, 4.0, 8.0],
        )

    def test_stable_run_resets_backoff(self) -> None:
        '''A respawned process that runs for >= 60s before crashing
        resets the slot's backoff to 1.'''
        from scrape_exchange.scraper_supervisor import (
            supervise_children, _ChildSlot,
        )
        # Set the initial slot to look like it has already crashed
        # several times: backoff = 32.
        crashed: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[1],
        )
        slot: _ChildSlot = self._make_slot(
            1, crashed, spawn_time=self._now - 120.0,
        )
        slot.backoff = 32.0
        observed_backoffs: list[float] = []

        def fake_spawn(s: _ChildSlot) -> None:
            observed_backoffs.append(s.backoff)
            s.process = _FakeProcess(pid=99, poll_returns=[1])
            s.spawn_time = self._now

        shutdown_state: dict[str, float | None] = {
            'deadline': None,
        }
        ticks: int = 0

        def fake_sleep(secs: float) -> None:
            nonlocal ticks
            ticks += 1
            self._advance(secs)
            if ticks >= 5:
                shutdown_state['deadline'] = self._now

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ), patch.object(
            scraper_supervisor.time, 'sleep', fake_sleep,
        ):
            supervise_children(
                'test', [slot], shutdown_state,
            )
        # Initial run lasted 120s >= 60s, so respawn after this
        # crash uses backoff=1 (reset), not 64.
        self.assertEqual(observed_backoffs[0], 1.0)

    def test_independent_slot_state(self) -> None:
        '''Two slots crashing simultaneously each get their own
        backoff progression — slot A's flap doesn't affect slot
        B.'''
        from scrape_exchange.scraper_supervisor import (
            supervise_children, _ChildSlot,
        )
        proc_a: _FakeProcess = _FakeProcess(
            pid=1, poll_returns=[1],
        )
        proc_b: _FakeProcess = _FakeProcess(
            pid=2, poll_returns=[None] * 50,
        )
        slot_a: _ChildSlot = self._make_slot(1, proc_a)
        slot_b: _ChildSlot = self._make_slot(2, proc_b)
        per_instance_backoffs: dict[int, list[float]] = {
            1: [], 2: [],
        }

        def fake_spawn(s: _ChildSlot) -> None:
            per_instance_backoffs[s.instance].append(s.backoff)
            s.process = _FakeProcess(
                pid=99, poll_returns=[None] * 50,
            )
            s.spawn_time = self._now

        shutdown_state: dict[str, float | None] = {
            'deadline': None,
        }
        ticks: int = 0

        def fake_sleep(secs: float) -> None:
            nonlocal ticks
            ticks += 1
            self._advance(secs)
            if ticks >= 10:
                shutdown_state['deadline'] = self._now

        with patch.object(
            scraper_supervisor.time, 'monotonic',
            side_effect=lambda: self._now,
        ), patch.object(
            scraper_supervisor, '_spawn_one_slot', fake_spawn,
        ), patch.object(
            scraper_supervisor.time, 'sleep', fake_sleep,
        ):
            supervise_children(
                'test', [slot_a, slot_b], shutdown_state,
            )
        # Slot A crashed once -> exactly one respawn at backoff 2.
        # Slot B never crashed -> no respawn.
        self.assertEqual(per_instance_backoffs[1], [2.0])
        self.assertEqual(per_instance_backoffs[2], [])


if __name__ == '__main__':
    unittest.main()
