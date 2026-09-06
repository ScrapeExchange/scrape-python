'''Bounded-cardinality browser metrics alongside shared scraper metrics.'''

from prometheus_client import Gauge, Histogram

SESSION_POOL_SIZE: Gauge = Gauge(
    'twitch_session_pool_size', 'Ready anonymous browser sessions',
    ('worker_id',),
)
SESSION_ACTIVE: Gauge = Gauge(
    'twitch_session_active', 'Sessions currently scraping a profile',
    ('worker_id',),
)
SESSION_WAIT: Histogram = Histogram(
    'twitch_session_wait_seconds', 'Wait for an available browser session',
    ('worker_id',),
)
