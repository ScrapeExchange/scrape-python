'''
Process-level worker identity for Prometheus metric labels.

Each scraper worker process gets a unique ``WORKER_ID``
environment variable from the supervisor (see
:func:`scrape_exchange.scraper_supervisor.spawn_children`).
Standalone or supervisor processes default to ``'0'``.

Every Prometheus metric in the scraper fleet includes a
``worker_id`` label sourced from :func:`get_worker_id` so
that workers on the same host — whose ``instance`` labels
collapse in the Prometheus scrape config — produce distinct
time series.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os

_WORKER_ID: str = os.environ.get('WORKER_ID', '0')


def get_worker_id() -> str:
    '''Return the worker ID for the current process.'''
    return _WORKER_ID
