'''
Instagram-specific Prometheus metrics.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from prometheus_client import Gauge, Histogram


SESSION_POOL_SIZE: Gauge = Gauge(
    'instagram_session_pool_size',
    'Number of Instagram browser sessions in each state',
    labelnames=('platform', 'scraper', 'state', 'worker_id'),
)

SESSION_ACQUIRE_ACTIVE: Gauge = Gauge(
    'instagram_session_acquire_active',
    'Instagram sessions currently held by a worker',
    labelnames=('platform', 'scraper', 'proxy_ip', 'worker_id'),
)

SESSION_ACQUIRE_WAIT_SECONDS: Histogram = Histogram(
    'instagram_session_acquire_wait_seconds',
    'Time a worker queued before getting an Instagram session',
    labelnames=('platform', 'scraper', 'worker_id'),
    buckets=(
        0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600,
    ),
)
