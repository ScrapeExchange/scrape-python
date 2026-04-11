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

import logging
import os
import signal
import subprocess
import sys

from dataclasses import dataclass

from prometheus_client import Gauge, start_http_server

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
    ['role', 'scraper'],
)
METRIC_CONCURRENCY: Gauge = Gauge(
    'scraper_concurrency',
    'Number of concurrent async tasks per worker process as seen '
    'from the current process.',
    ['role', 'scraper'],
)


# Log-file targets that can't be meaningfully suffixed with a
# ``.<worker_instance>`` marker. When the configured log file is
# one of these, :func:`spawn_children` leaves the child log-file
# env vars untouched and every child inherits the parent's stream.
_NON_FILE_LOG_TARGETS: frozenset[str] = frozenset({
    '/dev/stdout', '/dev/stderr', '-',
})


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
    :param proxies: Raw comma-separated proxy URL string as read
        from settings. May be ``None`` or empty — the supervisor
        rejects this case because multi-process mode is pointless
        without proxies to split.
    :param metrics_port: Base Prometheus port. The supervisor
        binds this port itself; children bind
        ``metrics_port + worker_instance`` where ``worker_instance``
        starts at 1.
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
    '''

    scraper_label: str
    num_processes_env_var: str
    num_processes: int
    concurrency: int
    proxies: str | None
    metrics_port: int
    log_file: str | None
    log_file_env_var: str | None = None


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

    METRIC_NUM_PROCESSES.labels(
        role=role, scraper=scraper_label,
    ).set(num_processes)
    METRIC_CONCURRENCY.labels(
        role=role, scraper=scraper_label,
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
            'Supervisor proxy split dropped or added proxies '
            '(input_count=%d assigned_count=%d)',
            len(proxies), len(assigned),
        )
        return False
    if len(set(assigned)) != len(assigned):
        _LOGGER.error(
            'Supervisor proxy split produced overlapping chunks '
            '(assigned_count=%d)',
            len(assigned),
        )
        return False
    if set(assigned) != set(proxies):
        _LOGGER.error(
            'Supervisor proxy split differs from input set',
        )
        return False
    return True


def spawn_children(
    config: SupervisorConfig, chunks: list[list[str]],
) -> list[subprocess.Popen]:
    '''
    Spawn one child subprocess per chunk in *chunks*.

    Each child inherits the parent environment with four
    overrides:

    * ``<config.num_processes_env_var>=1`` — prevents recursion
      into another supervisor.
    * ``PROXIES=<chunk>`` — scopes the child's rate limiter and
      per-proxy metrics to its own slice.
    * ``METRICS_PORT=<base + worker_instance>`` where
      ``worker_instance = index + 1`` — the base port is reserved
      for the supervisor.
    * ``LOG_FILE=<root>-<worker_instance><ext>`` (only when
      *config.log_file* is non-empty) — each child writes to its
      own file so they don't tear up a shared log.
    '''

    script_path: str = os.path.abspath(sys.argv[0])
    children: list[subprocess.Popen] = []
    suffixable: bool = bool(
        config.log_file
        and config.log_file not in _NON_FILE_LOG_TARGETS
    )
    for index, chunk in enumerate(chunks):
        worker_instance: int = index + 1
        child_env: dict[str, str] = os.environ.copy()
        child_env[config.num_processes_env_var] = '1'
        child_env['PROXIES'] = ','.join(chunk)
        child_env['METRICS_PORT'] = str(
            config.metrics_port + worker_instance
        )
        child_log_file: str | None = None
        if suffixable:
            assert config.log_file is not None
            root, ext = os.path.splitext(config.log_file)
            child_log_file = f'{root}-{worker_instance}{ext}'
            child_env['LOG_FILE'] = child_log_file
            if config.log_file_env_var:
                child_env[config.log_file_env_var] = child_log_file
        _LOGGER.info(
            'Spawning %s scraper child '
            '(worker_instance=%d proxies_count=%d '
            'metrics_port=%s log_file=%s)',
            config.scraper_label, worker_instance, len(chunk),
            child_env['METRICS_PORT'], child_log_file,
        )
        children.append(subprocess.Popen(
            [sys.executable, script_path], env=child_env,
        ))
    return children


def terminate_children(
    children: list[subprocess.Popen],
) -> None:
    '''Send SIGTERM to every still-running child.'''

    for child in children:
        if child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass


def wait_for_children(
    scraper_label: str, children: list[subprocess.Popen],
) -> int:
    '''
    Block until every child has exited. Returns ``0`` when every
    child exits cleanly, otherwise the non-zero exit code of the
    first failing child — at which point the surviving siblings
    are sent SIGTERM so the supervisor doesn't keep partial work
    running.
    '''

    exit_code: int = 0
    pending: list[subprocess.Popen] = list(children)
    while pending:
        for child in list(pending):
            try:
                rc: int | None = child.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                continue
            pending.remove(child)
            _LOGGER.info(
                '%s scraper child exited (pid=%d returncode=%s)',
                scraper_label, child.pid, rc,
            )
            if rc != 0 and exit_code == 0:
                exit_code = rc or 1
                _LOGGER.error(
                    'Child failed; terminating siblings '
                    '(pid=%d returncode=%s)',
                    child.pid, rc,
                )
                terminate_children(pending)
    return exit_code


def install_signal_forwarders(
    children: list[subprocess.Popen],
) -> None:
    '''
    Install SIGINT and SIGTERM handlers that forward the received
    signal to every still-running child.
    '''

    def _forward_signal(signum: int, _frame: object) -> None:
        _LOGGER.info(
            'Supervisor forwarding signal to children '
            '(signum=%d children=%d)',
            signum, len(children),
        )
        for child in children:
            if child.poll() is None:
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
    on :func:`wait_for_children`. Returns the exit code.

    The caller is responsible for calling ``configure_logging``
    before invoking this function — the supervisor only handles
    the process-management layer.
    '''

    if not config.proxies:
        _LOGGER.error(
            '%s scraper num_processes > 1 requires PROXIES to '
            'be set',
            config.scraper_label,
        )
        return 1

    proxies: list[str] = [
        p.strip() for p in config.proxies.split(',') if p.strip()
    ]
    if not proxies:
        _LOGGER.error(
            '%s scraper PROXIES is empty after parsing',
            config.scraper_label,
        )
        return 1

    n: int = min(config.num_processes, len(proxies))
    chunks: list[list[str]] = split_proxies(proxies, n)
    if not chunks_are_disjoint_cover(chunks, proxies):
        return 1

    start_http_server(config.metrics_port)
    _LOGGER.info(
        '%s supervisor metrics server started '
        '(metrics_port=%d)',
        config.scraper_label, config.metrics_port,
    )
    publish_config_metrics(
        role='supervisor',
        scraper_label=config.scraper_label,
        num_processes=n,
        concurrency=config.concurrency,
    )

    children: list[subprocess.Popen] = spawn_children(
        config, chunks,
    )
    install_signal_forwarders(children)

    try:
        return wait_for_children(config.scraper_label, children)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
