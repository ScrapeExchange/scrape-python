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
    'and proxy file. The proxy_network label was dropped to reduce '
    'series cardinality; recover the per-network view by joining '
    'proxy_file (which is 1:1 with network in production).',
    [
        'platform', 'scraper', 'entity', 'api',
        'worker_id', 'proxy_ip', 'proxy_file',
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
    'type, api, proxy, failure reason, and proxy file. The '
    'proxy_network label was dropped to reduce series cardinality.',
    [
        'platform', 'scraper', 'entity', 'api',
        'reason', 'worker_id', 'proxy_ip',
        'proxy_file',
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
# uploaded_video_ids_lookups_total / uploaded_video_ids_adds_total
#   Redis-backed uploaded-video-ID SET activity.
# ---------------------------------------------------------------------------
METRIC_UPLOADED_LOOKUPS: Counter = Counter(
    'uploaded_video_ids_lookups_total',
    'Lookups against the Redis uploaded-video-ids set.',
    ['outcome'],
)

METRIC_UPLOADED_ADDS: Counter = Counter(
    'uploaded_video_ids_adds_total',
    'Adds to the Redis uploaded-video-ids set.',
    ['outcome'],
)

# ---------------------------------------------------------------------------
# scrape_queue_size
#   Current number of items pending in the scrape queue. Use state='none'
#   for scrapers that do not partition their queue by tier/state.
#
#   ``worker_id`` is present so per-process and shared-state callers can
#   coexist without double-counting:
#
#   - Per-process callers (e.g. yt_video_scrape: asyncio.Queue per worker)
#     pass ``worker_id=get_worker_id()``. Each worker writes its own series;
#     ``sum by (entity, state)`` gives the correct fleet total.
#
#   - Shared-state callers (e.g. yt_video_upload: directory listing,
#     yt_rss_scrape: Redis-backed tier sizes) pass ``worker_id=''``. All
#     workers collapse to one series and ``livemostrecent`` returns the
#     correct shared value without N× inflation.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_QUEUE_SIZE: Gauge = Gauge(
    'scrape_queue_size',
    'Number of items pending processing in the scrape queue.',
    ['platform', 'scraper', 'entity', 'state', 'worker_id'],
    multiprocess_mode='livemostrecent',
)

# ---------------------------------------------------------------------------
# scrape_queue_enqueue_total
#   Incremented when a producer successfully adds an item to a scrape queue.
#   ``source`` is intentionally low-cardinality: examples include rss,
#   tiktok_creator, import, cli.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_QUEUE_ENQUEUES: Counter = Counter(
    'scrape_queue_enqueue_total',
    'Number of scrape queue items enqueued.',
    ['platform', 'scraper', 'entity', 'source'],
)

# ---------------------------------------------------------------------------
# scrape_retry_total
#   Incremented when a scrape schedules or consumes a retry after a
#   transient/rate-limit/auth-class failure.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_RETRIES: Counter = Counter(
    'scrape_retry_total',
    'Number of scrape retries by platform, entity, scraper, api, and reason.',
    ['platform', 'scraper', 'entity', 'api', 'reason'],
)

# ---------------------------------------------------------------------------
# tiktok_short_url_resolutions_total
#   Outcomes of resolving a vm/vt.tiktok.com short link to a creator handle.
# ---------------------------------------------------------------------------
METRIC_TIKTOK_SHORT_URL_RESOLUTIONS: Counter = Counter(
    'tiktok_short_url_resolutions_total',
    'TikTok creator short-URL resolution outcomes.',
    ['platform', 'scraper', 'entity', 'outcome'],
)

# ---------------------------------------------------------------------------
# scrape_records_written_total
#   Incremented after a scraped record is successfully persisted to disk.
# ---------------------------------------------------------------------------
METRIC_SCRAPE_RECORDS_WRITTEN: Counter = Counter(
    'scrape_records_written_total',
    'Number of scraped records successfully written to disk.',
    ['platform', 'scraper', 'entity'],
)

# ---------------------------------------------------------------------------
# files_pending_upload
#   Files found on disk that may still need to be uploaded.
#   Shared by channel and video upload tools; entity label distinguishes
#   them. livemostrecent ensures only the latest value per process is
#   used across a multi-process fleet.
# ---------------------------------------------------------------------------
METRIC_FILES_PENDING_UPLOAD: Gauge = Gauge(
    'files_pending_upload',
    'Number of files found that may need to be uploaded',
    ['platform', 'scraper', 'entity', 'worker_id'],
    multiprocess_mode='livemostrecent',
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
    multiprocess_mode='livemostrecent',
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
        0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        10.0, 30.0, 60.0, 120.0, 300.0,
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
# channel_priority_writes_total
#   Incremented each time the RSS scraper writes a channel-stat record
#   to YOUTUBE_CHANNEL_PRIORITY_DIRECTORY instead of POSTing it via
#   enqueue_upload.
# ---------------------------------------------------------------------------
METRIC_CHANNEL_PRIORITY_WRITES: Counter = Counter(
    'channel_priority_writes_total',
    'Channel-stat records written to '
    'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY by the RSS '
    'scraper. Replaces the in-process enqueue_upload path '
    'that previously POSTed channel updates directly.',
    ['platform', 'scraper', 'worker_id'],
)

# ---------------------------------------------------------------------------
# channel_priority_uploads_total
#   Incremented each time the channel-upload service POSTs a
#   channel-priority record to scrape.exchange.
# ---------------------------------------------------------------------------
METRIC_CHANNEL_PRIORITY_UPLOADS: Counter = Counter(
    'channel_priority_uploads_total',
    'Channel-priority records POSTed to scrape.exchange '
    'by the channel-upload service, broken down by '
    'outcome.',
    ['platform', 'scraper', 'result', 'worker_id'],
    # result is one of: success, retried, failed_permanently
)

# ---------------------------------------------------------------------------
# channel_priority_queue_age_seconds
#   Age of the oldest file in YOUTUBE_CHANNEL_PRIORITY_DIRECTORY.
#   A monotonically growing value indicates the consumer is falling
#   behind the producer.
# ---------------------------------------------------------------------------
METRIC_CHANNEL_PRIORITY_QUEUE_AGE: Gauge = Gauge(
    'channel_priority_queue_age_seconds',
    'Age of the oldest file currently in '
    'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY. Sampled per '
    'channel-upload sweep. A monotonically growing '
    'value indicates the consumer is falling behind the '
    'producer.',
    ['platform', 'scraper', 'worker_id'],
    multiprocess_mode='max',
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

# ---------------------------------------------------------------------------
# supervisor_respawns_total
#   Incremented once per respawn scheduled by the scraper supervisor
#   when a child process exits with a non-zero return code. The
#   ``instance`` label is the worker_instance (1..N) so dashboards
#   can flag a single slot that's flapping versus a fleet-wide event.
# ---------------------------------------------------------------------------
METRIC_SUPERVISOR_RESPAWNS: Counter = Counter(
    'supervisor_respawns_total',
    'Number of times the supervisor has respawned a crashed child.',
    ['scraper', 'instance'],
)

# ---------------------------------------------------------------------------
# watchdog_terminations_total
#   Incremented by the liveness watchdog immediately before it calls
#   os._exit(1) on a wedged worker. The ``signal`` label is ``loop`` (the
#   async heartbeat went stale -> frozen event loop) or ``work`` (no
#   forward worker progress -> all tasks wedged). Under multiprocess mode
#   the increment survives the process exit and is summed across dead
#   PIDs, so it is a durable "why did the slot restart" breakdown
#   alongside the supervisor's respawn counter.
# ---------------------------------------------------------------------------
METRIC_WATCHDOG_TERMINATIONS: Counter = Counter(
    'watchdog_terminations_total',
    'Number of times the liveness watchdog has terminated a worker.',
    ['signal'],
)

# ---------------------------------------------------------------------------
# rss_circuit_transitions_total
#   Incremented on every RSS circuit-breaker state transition. Labels
#   capture the from/to state pair so dashboards can distinguish
#   closed->open (trip), open->closed (recovery), and
#   closed-regular->closed-impaired (degradation) events.
# ---------------------------------------------------------------------------
METRIC_RSS_CIRCUIT_TRANSITIONS: Counter = Counter(
    'rss_circuit_transitions',
    'RSS circuit-breaker state transitions, labelled by from/to '
    'state names: closed-regular, open-regular, '
    'closed-impaired, open-impaired.',
    ['platform', 'from_state', 'to_state'],
)

# ---------------------------------------------------------------------------
# rss_circuit_state
#   Current RSS circuit-breaker state encoded as a one-hot gauge.
#   One label combination is set to 1 at a time; the others are 0.
#   Use sum() to detect multi-host disagreement.
# ---------------------------------------------------------------------------
METRIC_RSS_CIRCUIT_STATE: Gauge = Gauge(
    'rss_circuit_state',
    'Current RSS circuit-breaker state. One label combination '
    'is set to 1 at a time; the others are 0. Use sum() to '
    'detect multi-host disagreement.',
    ['platform', 'state'],
    multiprocess_mode='livemostrecent',
)

# ---------------------------------------------------------------------------
# rss_circuit_current_open_seconds
#   Current cooldown duration S for the RSS circuit breaker.
#   Reflects doubling in impaired mode and reset to the initial
#   value on recovery to regular mode.
# ---------------------------------------------------------------------------
METRIC_RSS_CIRCUIT_OPEN_SECONDS: Gauge = Gauge(
    'rss_circuit_current_open_seconds',
    'Current S (cooldown duration in seconds) for the RSS '
    'circuit breaker. Reflects doubling in impaired mode '
    'and reset to the initial value on recovery to regular '
    'mode.',
    ['platform'],
    multiprocess_mode='livemostrecent',
)

# ---------------------------------------------------------------------------
# rss_circuit_wait_seconds
#   Seconds an RSS worker call to breaker.acquire() blocked
#   because the circuit was open.
# ---------------------------------------------------------------------------
METRIC_RSS_CIRCUIT_WAIT_SECONDS: Histogram = Histogram(
    'rss_circuit_wait_seconds',
    'Seconds an RSS worker call to breaker.acquire() blocked '
    'because the circuit was open.',
    ['platform'],
    buckets=(0, 1, 5, 15, 60, 300, 900, 3600, 7200),
)
