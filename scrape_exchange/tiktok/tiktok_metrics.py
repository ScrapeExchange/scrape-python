'''
Foundation-specific Prometheus metrics for the TikTok scraper
package. The platform-agnostic counters in
``scrape_exchange.scraper_metrics`` cover throughput, failures,
and queues; this module owns the metrics that only make sense for
the TikTok stack (Playwright session pool + ms_token churn +
per-endpoint TikTokApi calls).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from prometheus_client import Counter, Gauge, Histogram


SESSION_POOL_SIZE: Gauge = Gauge(
    'tiktok_session_pool_size',
    'Number of TikTok sessions in each state',
    # state: ready|bootstrapping|failed
    labelnames=('platform', 'scraper', 'state', 'worker_id'),
)

SESSION_ACQUIRE_ACTIVE: Gauge = Gauge(
    'tiktok_session_acquire_active',
    'Sessions currently held by a worker (mirror of '
    'extract_info_active for YouTube)',
    labelnames=('platform', 'scraper', 'proxy_ip', 'worker_id'),
)

SESSION_ACQUIRE_WAIT_SECONDS: Histogram = Histogram(
    'tiktok_session_acquire_wait_seconds',
    'Time a worker queued before getting a session',
    labelnames=('platform', 'scraper', 'worker_id'),
    buckets=(
        0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600,
    ),
)

MS_TOKEN_REFRESH_TOTAL: Counter = Counter(
    'tiktok_ms_token_refresh_total',
    'Total ms_token refresh attempts',
    # outcome: success|failure
    labelnames=('platform', 'scraper', 'outcome', 'worker_id'),
)

MS_TOKEN_AGE_SECONDS: Gauge = Gauge(
    'tiktok_ms_token_age_seconds',
    'Age of the ms_token currently bound to each session',
    labelnames=('platform', 'scraper', 'proxy_ip', 'worker_id'),
)

API_CALL_TOTAL: Counter = Counter(
    'tiktok_api_call_total',
    'TikTokApi calls by endpoint and outcome',
    # endpoint: user_info|user_videos|video_info|hashtag_info|hashtag_videos
    # outcome: success|rate_limit|unavailable|transient|auth|signing|other
    labelnames=('platform', 'scraper', 'endpoint', 'outcome', 'worker_id'),
)

API_CALL_DURATION_SECONDS: Histogram = Histogram(
    'tiktok_api_call_duration_seconds',
    'TikTokApi call latency',
    labelnames=('platform', 'scraper', 'endpoint', 'worker_id'),
    buckets=(
        0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120,
    ),
)
