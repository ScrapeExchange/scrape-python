'''
Shared supervisor primitives for the scraper tools.

The video, RSS, and channel scrapers all follow the same pattern
when running with more than one process: a supervisor splits the
proxy pool into disjoint chunks, spawns one child subprocess per
chunk, each child runs as a leaf worker with its own Prometheus
metrics port and log file, and the supervisor forwards signals and
reaps children on shutdown.

This module extracts those primitives so each scraper can delegate
the boilerplate to a single well-tested implementation. It also
exposes two Prometheus gauges (``scraper_num_processes`` and
``scraper_concurrency``) that both the supervisor and each leaf
worker publish so the Grafana dashboard can see how the scraper
fleet is currently configured.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import random
import signal
import subprocess
import sys
import time

from dataclasses import dataclass, field
from pathlib import Path

from prometheus_client import Gauge, multiprocess

from scrape_exchange.metrics_server import (
    start_aggregating_metrics_server,
)

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.scraper_metrics import METRIC_SUPERVISOR_RESPAWNS
from scrape_exchange.worker_id import get_worker_id

# Environment variable the supervisor writes the pre-fetched JWT
# into for child processes. Workers check this before calling
# ``ExchangeClient.get_jwt_token`` themselves.
EXCHANGE_JWT_ENV_VAR: str = 'EXCHANGE_JWT'

_LOGGER: logging.Logger = logging.getLogger(__name__)


# Gauges the supervisor and every leaf worker publish so the
# dashboard can see the configured process/concurrency layout. The
# ``role`` label discriminates supervisor-level state from
# worker-level state:
#
#   role="supervisor" → num_processes = N, concurrency = per-child C
#     (the supervisor does not run any async workers itself; the
#      concurrency value it publishes describes what it told each
#      child to use)
#   role="worker"     → num_processes = 1, concurrency = C
#     (each child confirms its own view)
#
# Aggregations:
#   sum(scraper_num_processes{role="worker"}) by (scraper)
#     → total child processes currently visible on the fleet
#   sum(scraper_concurrency{role="worker"}) by (scraper)
#     → total async worker tasks currently visible on the fleet
METRIC_NUM_PROCESSES: Gauge = Gauge(
    'scraper_num_processes',
    'Number of child scraper processes configured for this '
    'scraper tree as seen from the current process.',
    ['platform', 'role', 'scraper', 'worker_id'],
    multiprocess_mode='max',
)
METRIC_CONCURRENCY: Gauge = Gauge(
    'scraper_concurrency',
    'Number of concurrent async tasks per worker process as seen '
    'from the current process.',
    ['platform', 'role', 'scraper', 'worker_id'],
    multiprocess_mode='max',
)


# Log-file targets that can't be meaningfully suffixed with a
# ``.<worker_instance>`` marker. When the configured log file is
# one of these, :func:`spawn_children` leaves the child log-file
# env vars untouched and every child inherits the parent's stream.
_NON_FILE_LOG_TARGETS: frozenset[str] = frozenset({
    '/dev/stdout', '/dev/stderr', '-',
})


# Per-child respawn backoff parameters. The supervisor doubles a
# slot's backoff on each consecutive crash, caps at _BACKOFF_MAX_SECONDS,
# and resets to the initial value if the previous run was stable.
#
# _BACKOFF_MAX_SECONDS and _BACKOFF_STABLE_THRESHOLD_SECONDS happen to
# share the value 60.0, but they are independent knobs:
#   - _BACKOFF_MAX_SECONDS caps the wait between respawns.
#   - _BACKOFF_STABLE_THRESHOLD_SECONDS decides what counts as a
#     "healthy" run (long enough to reset the backoff on the next crash).
# Changing one does not imply changing the other.
_BACKOFF_INITIAL_SECONDS: float = 1.0
_BACKOFF_MAX_SECONDS: float = 60.0
_BACKOFF_STABLE_THRESHOLD_SECONDS: float = 60.0


# Smooth the worker spawn burst at startup. Without this, N workers
# all open their initial Redis (rate-limit + scrape-exchange-rate-
# limit) connections simultaneously, which can swamp upstream
# accept queues / userspace docker-proxy / NAT and cause SYN drops
# that crash workers at startup. With ``N=12`` and a 0.2s stagger,
# total startup overhead is ~2.4s.
_SPAWN_STAGGER_SECONDS: float = 0.2


def _compute_next_backoff(
    prev_backoff: float, ran_seconds: float,
) -> float:
    '''Return the next backoff for a slot whose previous run
    lasted ``ran_seconds`` before crashing. Resets to
    ``_BACKOFF_INITIAL_SECONDS`` when the run was stable
    (``ran_seconds >= _BACKOFF_STABLE_THRESHOLD_SECONDS``);
    otherwise doubles the backoff capped at
    ``_BACKOFF_MAX_SECONDS``.

    Caller is required to pass
    ``prev_backoff >= _BACKOFF_INITIAL_SECONDS``; the function
    does not normalise zero or negative inputs.
    '''

    if ran_seconds >= _BACKOFF_STABLE_THRESHOLD_SECONDS:
        return _BACKOFF_INITIAL_SECONDS
    return min(prev_backoff * 2, _BACKOFF_MAX_SECONDS)


@dataclass
class _ChildSlot:
    '''Per-child supervisor state.

    Holds everything the supervisor needs to respawn this slot
    without re-deriving env / chunk / log-file / port from the
    parent config. ``process`` is ``None`` while a slot is
    waiting to respawn.'''

    instance: int
    spawn_env: dict[str, str]
    spawn_argv: list[str]
    process: subprocess.Popen | None = None
    backoff: float = _BACKOFF_INITIAL_SECONDS
    spawn_time: float | None = None
    respawn_at: float | None = None


@dataclass
class SupervisorConfig:
    '''
    Per-scraper config for :func:`run_supervisor` and its helpers.

    :param scraper_label: Short identifier that appears in the
        Prometheus ``scraper`` label and in log messages.
        Canonical values are ``'video'``, ``'rss'`` and
        ``'channel'``.
    :param num_processes_env_var: Name of the environment variable
        that controls process count for this scraper. The
        supervisor sets this to ``'1'`` in every child so that
        children don't recursively spawn their own supervisors.
    :param num_processes: Number of child processes the supervisor
        should spawn.
    :param concurrency: Per-child async-task concurrency. Only
        used for metric publication (the supervisor does not run
        async tasks itself).
    :param proxies: Proxy URL list as read from settings. May be
        ``None`` or empty — the supervisor rejects this case
        because multi-process mode is pointless without proxies
        to split.
    :param metrics_port: Prometheus port the supervisor binds.
        Children do not bind their own ports; they write metrics
        files to ``multiproc_dir`` and the supervisor aggregates.
    :param log_file: The supervisor's log file path. When
        non-empty and not a stream target like ``/dev/stdout``,
        children's ``LOG_FILE`` env var is rewritten to
        ``<root>.<worker_instance><ext>`` so each worker writes
        its own file instead of fighting over a shared one.
    :param log_file_env_var: Name of the scraper-specific log-
        file environment variable (for example ``VIDEO_LOG_FILE``).
        When set, :func:`spawn_children` writes the suffixed path
        to **both** ``LOG_FILE`` and this var so the child's
        pydantic settings resolves the scraper-specific alias
        (which is higher priority) to the per-worker file. Set to
        ``None`` if the scraper doesn't use a scraper-specific
        log-file alias.
    :param metrics_port_env_var: Unused. Retained for backwards
        compatibility. Children no longer receive a per-worker
        ``METRICS_PORT``; they inherit ``PROMETHEUS_MULTIPROC_DIR``
        from the supervisor and write gauge files there instead.
    :param shutdown_grace_seconds: Seconds to wait after
        forwarding SIGTERM/SIGINT to children before escalating
        to SIGKILL. Defaults to 30.
    '''

    scraper_label: str
    num_processes_env_var: str
    num_processes: int
    concurrency: int
    proxies: list[str]
    metrics_port: int
    log_file: str | None
    log_file_env_var: str | None = None
    concurrency_env_var: str | None = None
    child_concurrencies: list[int] | None = None
    metrics_port_env_var: str | None = None
    split_proxy_pool: bool = False
    api_key_id: str | None = None
    api_key_secret: str | None = None
    exchange_url: str = 'https://scrape.exchange'
    shutdown_grace_seconds: int = 60
    multiproc_dir: Path = field(
        default_factory=lambda: Path(
            '/run/scrape/_unset_/metrics',
        ),
    )

    def __post_init__(self) -> None:
        if '_unset_' in str(self.multiproc_dir):
            self.multiproc_dir = Path(
                f'/run/scrape/{self.scraper_label}/metrics',
            )


def publish_config_metrics(
    role: str, scraper_label: str,
    num_processes: int, concurrency: int,
) -> None:
    '''
    Publish :data:`METRIC_NUM_PROCESSES` and
    :data:`METRIC_CONCURRENCY` for this process. Call from the
    supervisor right after it binds its own metrics port, and from
    each worker's startup path right after *its* metrics port is
    bound.

    :param role: ``'supervisor'`` or ``'worker'``.
    :param scraper_label: Matches ``SupervisorConfig.scraper_label``.
    :param num_processes: Value to publish. The supervisor
        reports the total child count; a leaf worker reports 1.
    :param concurrency: Value to publish. Both the supervisor and
        the worker report the same per-child concurrency.
    '''

    worker_id: str = get_worker_id()
    METRIC_NUM_PROCESSES.labels(
        platform='youtube',
        role=role,
        scraper=scraper_label,
        worker_id=worker_id,
    ).set(num_processes)
    METRIC_CONCURRENCY.labels(
        platform='youtube',
        role=role,
        scraper=scraper_label,
        worker_id=worker_id,
    ).set(concurrency)


def split_proxies(
    proxies: list[str], n: int,
) -> list[list[str]]:
    '''
    Split *proxies* into *n* disjoint chunks of as-equal-as-
    possible size, preserving input order. Empty chunks are
    returned when ``n > len(proxies)`` so the caller can decide
    whether to skip spawning children for them.
    '''

    if n <= 0:
        raise ValueError(
            f'num_processes must be >= 1, got {n}',
        )
    chunks: list[list[str]] = [[] for _ in range(n)]
    for i, proxy in enumerate(proxies):
        chunks[i % n].append(proxy)
    return chunks


def chunks_are_disjoint_cover(
    chunks: list[list[str]], proxies: list[str],
) -> bool:
    '''
    Verify *chunks* is a disjoint cover of *proxies*. Logs the
    specific failure mode on the module logger and returns
    ``False`` on any violation.

    This is still load-bearing when the shared-file rate limiter
    is disabled (the per-process limiter can't reconcile
    overlapping chunks across children). With the shared-file
    backend it's a sanity net: overlap still works correctly
    but is almost certainly a configuration mistake.
    '''

    assigned: list[str] = [p for chunk in chunks for p in chunk]
    if len(assigned) != len(proxies):
        _LOGGER.error(
            'Supervisor proxy split dropped or added proxies',
            extra={
                'input_count': len(proxies),
                'assigned_count': len(assigned),
            },
        )
        return False
    if len(set(assigned)) != len(assigned):
        _LOGGER.error(
            'Supervisor proxy split produced overlapping chunks',
            extra={'assigned_count': len(assigned)},
        )
        return False
    if set(assigned) != set(proxies):
        _LOGGER.error(
            'Supervisor proxy split differs from input set',
        )
        return False
    return True


def proxy_pool_for_children(
    proxies: list[str], n: int,
) -> list[list[str]]:
    '''
    Return the proxy pool each child should receive.

    Unlike :func:`split_proxies`, this intentionally gives every child the
    full pool. The shared rate limiter owns per-proxy cadence across child
    processes, so each child can safely choose any currently token-rich
    outbound IP.
    '''

    if n <= 0:
        raise ValueError(
            f'num_processes must be >= 1, got {n}',
        )
    return [list(proxies) for _ in range(n)]


def distribute_total_concurrency(total: int, n: int) -> list[int]:
    '''Split a total concurrency limit across child processes.'''
    if total <= 0:
        raise ValueError(
            f'total concurrency must be >= 1, got {total}',
        )
    if n <= 0:
        raise ValueError(
            f'num_processes must be >= 1, got {n}',
        )
    active: int = min(total, n)
    base: int = total // active
    remainder: int = total % active
    return [
        base + (1 if index < remainder else 0)
        for index in range(active)
    ]


def random_proxy_subset(
    proxies: list[str],
    limit: int,
) -> list[str]:
    '''Choose a random proxy subset when capacity is below pool size.'''
    if limit <= 0:
        raise ValueError(
            f'proxy subset limit must be >= 1, got {limit}',
        )
    if limit >= len(proxies):
        return list(proxies)
    return random.sample(proxies, limit)


def _spawn_one_slot(slot: _ChildSlot) -> None:
    '''Launch the subprocess for *slot* and record its handle and
    spawn timestamp. Used both at startup and on respawn so the
    Popen invocation lives in exactly one place.'''

    slot.process = subprocess.Popen(
        slot.spawn_argv, env=slot.spawn_env,
    )
    slot.spawn_time = time.monotonic()
    slot.respawn_at = None


def spawn_children(
    config: SupervisorConfig, chunks: list[list[str]],
    jwt_header: str | None = None,
    child_concurrencies: list[int] | None = None,
) -> list[_ChildSlot]:
    '''Spawn one child subprocess per chunk in *chunks*, returning
    one ``_ChildSlot`` per child. The slot captures the spawn
    env so the supervise loop can re-launch the same child later
    without re-deriving its config.'''

    script_path: str = os.path.abspath(sys.argv[0])
    slots: list[_ChildSlot] = []
    suffixable: bool = bool(
        config.log_file
        and config.log_file not in _NON_FILE_LOG_TARGETS
    )
    for index, chunk in enumerate(chunks):
        if index > 0:
            time.sleep(_SPAWN_STAGGER_SECONDS)
        worker_instance: int = index + 1
        child_env: dict[str, str] = os.environ.copy()
        child_env[config.num_processes_env_var] = '1'
        child_env['PROXIES'] = ','.join(chunk)
        child_env['WORKER_ID'] = str(worker_instance)
        if child_concurrencies is not None:
            child_concurrency: int = child_concurrencies[index]
            if config.concurrency_env_var:
                child_env[config.concurrency_env_var] = str(
                    child_concurrency,
                )
            child_env['CONCURRENCY'] = str(child_concurrency)
        if jwt_header is not None:
            child_env[EXCHANGE_JWT_ENV_VAR] = jwt_header
        child_log_file: str | None = None
        if suffixable:
            assert config.log_file is not None
            root, ext = os.path.splitext(config.log_file)
            child_log_file = f'{root}-{worker_instance}{ext}'
            child_env['LOG_FILE'] = child_log_file
            if config.log_file_env_var:
                child_env[config.log_file_env_var] = child_log_file
        _LOGGER.info(
            'Spawning scraper child',
            extra={
                'scraper': config.scraper_label,
                'worker_instance': worker_instance,
                'proxies_count': len(chunk),
                'concurrency': (
                    child_concurrencies[index]
                    if child_concurrencies is not None else (
                        config.concurrency
                    )
                ),
                'log_file': child_log_file,
            },
        )
        slot: _ChildSlot = _ChildSlot(
            instance=worker_instance,
            spawn_env=child_env,
            spawn_argv=[sys.executable, script_path],
        )
        _spawn_one_slot(slot)
        slots.append(slot)
    return slots


def _should_escalate(
    shutdown_state: dict[str, float | None] | None,
) -> bool:
    '''
    Return ``True`` when the shutdown grace period has expired
    and children should be sent SIGKILL.
    '''

    if shutdown_state is None:
        return False
    deadline: float | None = shutdown_state.get('deadline')
    return deadline is not None and time.monotonic() >= deadline


def _kill_children(
    scraper_label: str,
    pending: list[subprocess.Popen],
) -> None:
    '''Send SIGKILL to every still-running child in *pending*.'''

    _LOGGER.warning(
        'Supervisor grace period expired; sending SIGKILL '
        'to remaining children',
        extra={
            'scraper': scraper_label,
            'remaining_children': len(pending),
        },
    )
    for child in pending:
        if child.poll() is None:
            try:
                child.kill()
            except ProcessLookupError:
                pass


_POLL_INTERVAL_SECONDS: float = 0.5


def _is_shutting_down(
    shutdown_state: dict[str, float | None] | None,
) -> bool:
    '''True once a shutdown signal has been received (or
    ``shutdown_state`` was constructed with a deadline already
    set).'''

    if shutdown_state is None:
        return False
    return shutdown_state.get('deadline') is not None


def _record_crash(
    scraper_label: str, slot: _ChildSlot, rc: int,
    ran_seconds: float, now: float,
) -> None:
    '''Apply the per-slot crash bookkeeping: compute next
    backoff, schedule respawn, log, bump metric.'''

    pid: int | None = (
        slot.process.pid if slot.process else None
    )
    if pid is not None:
        multiprocess.mark_process_dead(pid)
    prev_backoff: float = slot.backoff
    slot.backoff = _compute_next_backoff(
        prev_backoff, ran_seconds,
    )
    slot.respawn_at = now + slot.backoff
    _LOGGER.warning(
        'Child crashed; will respawn',
        extra={
            'scraper': scraper_label,
            'instance': slot.instance,
            'pid': pid,
            'returncode': rc,
            'ran_seconds': ran_seconds,
            'backoff_seconds': slot.backoff,
            'respawn_at': slot.respawn_at,
        },
    )
    if (slot.backoff == _BACKOFF_INITIAL_SECONDS
            and prev_backoff != _BACKOFF_INITIAL_SECONDS):
        _LOGGER.info(
            'Worker stable; backoff reset',
            extra={
                'scraper': scraper_label,
                'instance': slot.instance,
                'prev_backoff': prev_backoff,
                'ran_seconds': ran_seconds,
            },
        )
    METRIC_SUPERVISOR_RESPAWNS.labels(
        scraper=scraper_label,
        instance=str(slot.instance),
    ).inc()
    slot.process = None


def _retire_slot(
    scraper_label: str, slot: _ChildSlot, rc: int,
    shutting_down: bool,
) -> None:
    '''Mark a slot retired (no respawn). Called for clean exits
    and for any exit observed during shutdown.'''

    pid: int | None = (
        slot.process.pid if slot.process else None
    )
    if pid is not None:
        multiprocess.mark_process_dead(pid)
    if shutting_down:
        _LOGGER.info(
            'Child exited during shutdown; not respawning',
            extra={
                'scraper': scraper_label,
                'instance': slot.instance,
                'pid': pid,
                'returncode': rc,
            },
        )
    else:
        _LOGGER.info(
            'Child exited cleanly; not respawning',
            extra={
                'scraper': scraper_label,
                'instance': slot.instance,
                'pid': pid,
            },
        )
    slot.process = None
    slot.respawn_at = None


def _log_respawn(scraper_label: str, slot: _ChildSlot) -> None:
    '''Emit the INFO "Respawning child" line after a successful
    ``_spawn_one_slot`` call so ``new_pid`` is available.'''

    proxies_val: str = slot.spawn_env.get('PROXIES', '')
    proxies_count: int = (
        len(proxies_val.split(',')) if proxies_val else 0
    )
    _LOGGER.info(
        'Respawning child',
        extra={
            'scraper': scraper_label,
            'instance': slot.instance,
            'new_pid': (
                slot.process.pid if slot.process else None
            ),
            'proxies_count': proxies_count,
        },
    )


def supervise_children(
    scraper_label: str, slots: list[_ChildSlot],
    shutdown_state: dict[str, float | None] | None = None,
) -> int:
    '''Supervise *slots* until every slot is retired.

    Per-tick (default ``_POLL_INTERVAL_SECONDS``):

    1. For each slot with ``process is not None``: poll. On exit,
       classify the rc and (a) schedule respawn with backoff, or
       (b) retire the slot.
    2. For each slot with ``process is None and
       respawn_at is not None``: when ``now >= respawn_at`` and
       the supervisor isn't shutting down, call
       ``_spawn_one_slot(slot)``.
    3. When ``_should_escalate(shutdown_state)`` becomes true,
       SIGKILL all still-running children.
    4. Return ``0`` once no slot has a running process and none
       has a pending respawn.

    Returns ``0`` in all normal cases — failures self-heal via
    respawn, and clean exits are absorbed silently.'''

    escalated: bool = False
    was_shutting_down: bool = False
    while True:
        now: float = time.monotonic()
        shutting_down: bool = _is_shutting_down(shutdown_state)

        if shutting_down and not was_shutting_down:
            pending_respawns: int = sum(
                1 for s in slots
                if s.respawn_at is not None
                and s.process is None
            )
            _LOGGER.info(
                'Shutdown received; no further respawns',
                extra={
                    'scraper': scraper_label,
                    'pending_respawns': pending_respawns,
                },
            )
            was_shutting_down = True

        if not escalated and _should_escalate(shutdown_state):
            escalated = True
            running: list[subprocess.Popen] = [
                s.process for s in slots
                if s.process is not None
                and s.process.poll() is None
            ]
            if running:
                _kill_children(scraper_label, running)

        for slot in slots:
            if slot.process is None:
                continue
            rc: int | None = slot.process.poll()
            if rc is None:
                continue
            ran_seconds: float = (
                now - slot.spawn_time
                if slot.spawn_time is not None else 0.0
            )
            if shutting_down or rc == 0:
                _retire_slot(
                    scraper_label, slot, rc, shutting_down,
                )
                continue
            _record_crash(
                scraper_label, slot, rc,
                ran_seconds=ran_seconds, now=now,
            )

        if not shutting_down:
            for slot in slots:
                if (slot.process is None
                        and slot.respawn_at is not None
                        and now >= slot.respawn_at):
                    _spawn_one_slot(slot)
                    _log_respawn(scraper_label, slot)

        running_count: int = sum(
            1 for s in slots if s.process is not None
        )
        pending_respawn_count: int = sum(
            1 for s in slots
            if s.process is None and s.respawn_at is not None
            and not shutting_down
        )
        if running_count == 0 and pending_respawn_count == 0:
            return 0

        time.sleep(_POLL_INTERVAL_SECONDS)


def install_signal_forwarders(
    slots: list[_ChildSlot],
    shutdown_state: dict[str, float | None] | None = None,
    grace_seconds: int = 30,
) -> None:
    '''Install SIGINT/SIGTERM handlers that forward the received
    signal to every still-running child across the slot list.
    Slot processes are looked up at signal-delivery time so
    children spawned by a respawn after install also receive
    forwarded signals.

    When *shutdown_state* is provided, the first signal sets
    ``shutdown_state['deadline']`` to
    ``time.monotonic() + grace_seconds``.'''

    def _forward_signal(signum: int, _frame: object) -> None:
        running: list[subprocess.Popen] = [
            s.process for s in slots
            if s.process is not None
            and s.process.poll() is None
        ]
        _LOGGER.info(
            'Supervisor forwarding signal to children',
            extra={
                'signum': signum,
                'children_count': len(running),
            },
        )
        if (shutdown_state is not None
                and shutdown_state.get('deadline') is None):
            shutdown_state['deadline'] = (
                time.monotonic() + grace_seconds
            )
        for child in running:
            try:
                child.send_signal(signum)
            except ProcessLookupError:
                pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _forward_signal)


def run_supervisor(config: SupervisorConfig) -> int:
    '''
    Full supervisor orchestration. Validates and parses the proxy
    pool, asserts :func:`chunks_are_disjoint_cover`, binds the
    Prometheus HTTP server on ``config.metrics_port``, publishes
    the config gauges, spawns one child per chunk via
    :func:`spawn_children`, installs signal forwarders, and blocks
    on :func:`supervise_children`, which respawns crashed children
    until a shutdown signal is received. Returns the exit code.

    The caller is responsible for calling ``configure_logging``
    before invoking this function — the supervisor only handles
    the process-management layer.
    '''

    if not config.proxies:
        _LOGGER.error(
            'Scraper num_processes > 1 requires PROXIES to be set',
            extra={'scraper': config.scraper_label},
        )
        return 1

    proxies: list[str] = list(config.proxies)
    if not proxies:
        _LOGGER.error(
            'Scraper PROXIES is empty after parsing',
            extra={'scraper': config.scraper_label},
        )
        return 1

    n: int = min(config.num_processes, len(proxies))
    if config.split_proxy_pool:
        chunks: list[list[str]] = split_proxies(proxies, n)
        if not chunks_are_disjoint_cover(chunks, proxies):
            return 1
    else:
        # Every child receives the full proxy pool.  The YouTube rate
        # limiter is shared across worker processes, so overlapping proxy
        # visibility is safe and lets each child choose any currently
        # token-rich outbound IP.
        chunks = proxy_pool_for_children(proxies, n)

    child_concurrencies: list[int] | None = config.child_concurrencies
    if child_concurrencies is not None:
        child_concurrencies = child_concurrencies[:len(chunks)]
        chunks = chunks[:len(child_concurrencies)]
        n = len(chunks)

    # Fetch the JWT once so children don't each hit the token
    # endpoint independently at startup.  Retry with 1s → 2s →
    # 4s → 8s delays (4 attempts total) before giving up.
    jwt_header: str | None = None
    if config.api_key_id and config.api_key_secret:
        delay: float = 1.0
        max_delay: float = 8.0
        while True:
            try:
                jwt_header = asyncio.run(
                    ExchangeClient.get_jwt_token(
                        config.api_key_id,
                        config.api_key_secret,
                        config.exchange_url,
                    )
                )
                _LOGGER.info(
                    'Supervisor acquired JWT for children',
                    extra={'scraper': config.scraper_label},
                )
                break
            except Exception as exc:
                if delay > max_delay:
                    _LOGGER.critical(
                        'Supervisor failed to acquire JWT '
                        'after retries; exiting',
                        exc=exc,
                        extra={'scraper': config.scraper_label},
                    )
                    return 1
                _LOGGER.warning(
                    'Supervisor JWT attempt failed, retrying',
                    exc=exc,
                    extra={
                        'scraper': config.scraper_label,
                        'retry_in_seconds': delay,
                    },
                )
                time.sleep(delay)
                delay *= 2

    start_aggregating_metrics_server(
        config.metrics_port, config.multiproc_dir,
    )
    _LOGGER.info(
        'Supervisor metrics server started',
        extra={
            'scraper': config.scraper_label,
            'metrics_port': config.metrics_port,
        },
    )
    publish_config_metrics(
        role='supervisor',
        scraper_label=config.scraper_label,
        num_processes=n,
        concurrency=config.concurrency,
    )

    slots: list[_ChildSlot] = spawn_children(
        config, chunks, jwt_header=jwt_header,
        child_concurrencies=child_concurrencies,
    )
    shutdown_state: dict[str, float | None] = {
        'deadline': None,
    }
    install_signal_forwarders(
        slots, shutdown_state,
        grace_seconds=config.shutdown_grace_seconds,
    )

    try:
        return supervise_children(
            config.scraper_label, slots, shutdown_state,
        )
    finally:
        for slot in slots:
            if (slot.process is not None
                    and slot.process.poll() is None):
                slot.process.kill()
                slot.process.wait()
