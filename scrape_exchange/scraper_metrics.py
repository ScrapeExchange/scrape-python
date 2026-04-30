'''
Shared Prometheus metric declarations for the YouTube scraping tools.

All three tools (video, channel, RSS) register counters and gauges
with the same metric names. Declaring them here — once — avoids
``ValueError: Duplicated timeseries in CollectorRegistry`` when more
than one tool module is imported in the same Python process (e.g.
during test runs).

Tools import the metric objects from here rather than constructing
their own. The underlying :class:`~prometheus_client.Counter` /
:class:`~prometheus_client.Gauge` /
:class:`~prometheus_client.Histogram` instances are the same for all
importers within a process.

Label superset rules
--------------------
Each metric's label list must cover the union of every label any tool
will pass at call time. Labels unused by a particular tool should be
passed as ``'none'``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# scrapes_completed_total
#   Incremented once per successfully scraped entity (video, channel,
#   rss_feed). Label superset covers all three tools.
#   Labels not applicable to a particular tool should be passed as 'none'.
# ---------------------------------------------------------------------------
METRIC_SCRAPES_COMPLETED: Counter = Counter(
    'scrapes_completed_total',
    'Number of entities successfully scraped, labelled by scraper, '
    'entity type, api (ytdlp/html/rss/innertube), proxy IP, '
    'and proxy network.',
    [
        'platform', 'scraper', 'entity', 'api',
        'worker_id', 'proxy_ip', 'proxy_network',
    ],
)

# ---------------------------------------------------------------------------
# scrape_failures_total
#   Incremented on each failed scrape attempt. Label superset covers all
#   three tools.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_FAILURES: Counter = Counter(
    'scrape_failures_total',
    'Number of failed scrape attempts, labelled by scraper, entity '
    'type, api, proxy, and failure reason.',
    [
        'platform', 'scraper', 'entity', 'api',
        'reason', 'worker_id', 'proxy_ip', 'proxy_network',
    ],
)

# ---------------------------------------------------------------------------
# uploads_enqueued_total
#   Incremented each time an entity is successfully enqueued for
#   background upload.
# ---------------------------------------------------------------------------
METRIC_UPLOADS_ENQUEUED: Counter = Counter(
    'uploads_enqueued_total',
    'Number of entities successfully enqueued for background upload. '
    'Actual delivery is tracked by uploads_completed_total.',
    ['platform', 'scraper', 'entity', 'mode', 'worker_id'],
)

# ---------------------------------------------------------------------------
# uploads_skipped_total
#   Incremented when an upload step is skipped (entity already uploaded,
#   file superseded, has sufficient formats, etc.).
# ---------------------------------------------------------------------------
METRIC_UPLOADS_SKIPPED: Counter = Counter(
    'uploads_skipped_total',
    'Number of upload attempts skipped, labelled by the skip reason '
    '(already_uploaded, has_formats, superseded, …).',
    ['platform', 'scraper', 'entity', 'reason', 'worker_id'],
)

# ---------------------------------------------------------------------------
# scrape_queue_size
#   Current number of items pending in the scrape queue. Use tier='none'
#   for scrapers that do not partition their queue by tier.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_QUEUE_SIZE: Gauge = Gauge(
    'scrape_queue_size',
    'Number of items pending processing in the scrape queue.',
    ['platform', 'scraper', 'entity', 'tier', 'worker_id'],
)

# ---------------------------------------------------------------------------
# worker_sleep_seconds
#   Seconds the worker is sleeping before its next processing round.
#   Set to 0 when active.
# ---------------------------------------------------------------------------
METRIC_WORKER_SLEEP_SECONDS: Gauge = Gauge(
    'worker_sleep_seconds',
    'Seconds the worker will sleep before processing the next batch.',
    ['platform', 'scraper', 'worker_id'],
)

# ---------------------------------------------------------------------------
# scrape_duration_seconds
#   Per-scrape latency histogram. The 'api' label distinguishes yt-dlp,
#   HTML, InnerTube, and RSS fetches.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_DURATION: Histogram = Histogram(
    'scrape_duration_seconds',
    'Duration of a single scrape call, labelled by api type and '
    'outcome (success/failure).',
    [
        'platform', 'scraper', 'entity', 'api',
        'outcome', 'worker_id',
    ],
    buckets=(
        0.5, 1.0, 2.5, 5.0, 10.0,
        30.0, 60.0, 120.0, 300.0,
    ),
)

# ---------------------------------------------------------------------------
# uploads_failed_total
#   Bulk-upload records reported as permanently failed by the API. The
#   source files are left in base_dir for a later retry.
# ---------------------------------------------------------------------------
METRIC_UPLOADS_FAILED: Counter = Counter(
    'uploads_failed_total',
    'Bulk-upload records reported as failed. Source files are left '
    'in base_dir for the next iteration.',
    ['platform', 'scraper', 'entity', 'mode', 'worker_id'],
)

# ---------------------------------------------------------------------------
# uploads_missing_result_total
#   Records submitted in a bulk batch that did not appear in the job
#   results (possible API timeout or partial response).
# ---------------------------------------------------------------------------
METRIC_UPLOADS_MISSING_RESULT: Counter = Counter(
    'uploads_missing_result_total',
    'Bulk-upload records submitted but absent from the job results.',
    ['platform', 'scraper', 'entity', 'mode', 'worker_id'],
)

# ---------------------------------------------------------------------------
# upload_batches_total
#   Bulk-upload batches dispatched, labelled by outcome.
# ---------------------------------------------------------------------------
METRIC_UPLOAD_BATCHES: Counter = Counter(
    'upload_batches_total',
    'Bulk-upload batches dispatched by the scheduled upload sweep.',
    ['platform', 'scraper', 'entity', 'mode', 'worker_id', 'outcome'],
)

# ---------------------------------------------------------------------------
# watcher_files_detected_total / watcher_files_skipped_total /
# watcher_batches_total
#   File-system watcher events for the upload-only watcher path.
# ---------------------------------------------------------------------------
METRIC_WATCHER_FILES_DETECTED: Counter = Counter(
    'watcher_files_detected_total',
    'Files detected by the upload-only file watcher.',
    ['platform', 'scraper', 'entity', 'worker_id'],
)

METRIC_WATCHER_FILES_SKIPPED: Counter = Counter(
    'watcher_files_skipped_total',
    'Files skipped by the watcher (already uploaded or superseded).',
    ['platform', 'scraper', 'entity', 'worker_id'],
)

METRIC_WATCHER_BATCHES: Counter = Counter(
    'watcher_batches_total',
    'Number of change batches yielded by the file watcher.',
    ['platform', 'scraper', 'entity', 'worker_id'],
)
