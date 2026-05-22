'''
Wrapper around ``prometheus_client.start_http_server`` that bumps
the listen backlog before starting the server.

prom_client's ``ThreadingWSGIServer`` inherits
``socketserver.TCPServer.request_queue_size = 5``; under fleet load
(many workers + Prometheus scraping in parallel + the worker's
GIL-bound asyncio loop occasionally not yielding to the prom_client
thread fast enough) SYNs queue past 5 and get dropped silently.
The result is targets that appear DOWN in Prometheus even though
their workers are healthy.

Setting ``ThreadingWSGIServer.request_queue_size`` before
``start_http_server`` makes the server's ``listen()`` syscall use
the larger value (capped by ``/proc/sys/net/core/somaxconn``).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import shutil
from pathlib import Path

from prometheus_client import CollectorRegistry
from prometheus_client import multiprocess
from prometheus_client import (
    start_http_server as _start_http_server,
)
from prometheus_client.exposition import ThreadingWSGIServer


def start_metrics_server(port: int, backlog: int = 128) -> None:
    '''Start the prom_client HTTP metrics server with a larger
    listen backlog so Prometheus scrapes survive concurrent /
    bursty arrival.

    The default of 128 is well within the typical
    ``/proc/sys/net/core/somaxconn`` ceiling (4096) and handles
    dozens of simultaneous scrape attempts without dropping SYNs.
    '''

    ThreadingWSGIServer.request_queue_size = backlog
    _start_http_server(port)


def start_aggregating_metrics_server(
    port: int,
    multiproc_dir: Path,
    backlog: int = 128,
) -> None:
    '''Start a Prometheus HTTP server that aggregates metrics from
    every worker process via mmap'd files in ``multiproc_dir``.

    The supervisor calls this exactly once, before forking any
    child. ``multiproc_dir`` is wiped (clears ghost pid files from
    a prior run) and recreated; ``PROMETHEUS_MULTIPROC_DIR`` is
    set in ``os.environ`` so children inherit it on fork. The HTTP
    server is bound to ``port`` using a fresh
    :class:`CollectorRegistry` whose only collector is
    :class:`multiprocess.MultiProcessCollector`. The ``backlog``
    value bumps the listen backlog identically to
    :func:`start_metrics_server`.
    '''

    shutil.rmtree(multiproc_dir, ignore_errors=True)
    multiproc_dir.mkdir(parents=True, exist_ok=True)
    os.environ['PROMETHEUS_MULTIPROC_DIR'] = str(multiproc_dir)

    registry: CollectorRegistry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)

    ThreadingWSGIServer.request_queue_size = backlog
    _start_http_server(port, registry=registry)
