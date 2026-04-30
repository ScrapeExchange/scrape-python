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
    labelnames=('scraper', 'state'),  # state: ready|bootstrapping|failed
)

SESSION_ACQUIRE_ACTIVE: Gauge = Gauge(
    'tiktok_session_acquire_active',
    'Sessions currently held by a worker (mirror of '
    'extract_info_active for YouTube)',
    labelnames=('scraper', 'proxy_ip'),
)

SESSION_ACQUIRE_WAIT_SECONDS: Histogram = Histogram(
    'tiktok_session_acquire_wait_seconds',
    'Time a worker queued before getting a session',
    labelnames=('scraper',),
    buckets=(
        0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300,
    ),
)

MS_TOKEN_REFRESH_TOTAL: Counter = Counter(
    'tiktok_ms_token_refresh_total',
    'Total ms_token refresh attempts',
    labelnames=('scraper', 'outcome'),  # outcome: success|failure
)

MS_TOKEN_AGE_SECONDS: Gauge = Gauge(
    'tiktok_ms_token_age_seconds',
    'Age of the ms_token currently bound to each session',
    labelnames=('scraper', 'proxy_ip'),
)

API_CALL_TOTAL: Counter = Counter(
    'tiktok_api_call_total',
    'TikTokApi calls by endpoint and outcome',
    labelnames=('scraper', 'endpoint', 'outcome'),
    # endpoint: user_info|user_videos|video_info|hashtag_info|hashtag_videos
    # outcome: success|rate_limit|unavailable|transient|auth|signing|other
)

API_CALL_DURATION_SECONDS: Histogram = Histogram(
    'tiktok_api_call_duration_seconds',
    'TikTokApi call latency',
    labelnames=('scraper', 'endpoint'),
    buckets=(
        0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120,
    ),
)
