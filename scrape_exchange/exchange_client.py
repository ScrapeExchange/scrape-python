'''
Scrape.Exchange client used for uploading content

:author: boinko@scrape.exchange
:copyright: 2026
:license: GPLv3
'''


import asyncio
import base64
import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging

from typing import Any, Callable, Self, TYPE_CHECKING

from httpx import AsyncClient, Response
from prometheus_client import Counter, Gauge, Histogram

from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeCallType,
    ScrapeExchangeRateLimiter,
    DEFAULT_429_PENALTY_SECONDS,
)
from scrape_exchange.worker_id import get_worker_id

if TYPE_CHECKING:
    from pathlib import Path
    from scrape_exchange.file_management import AssetFileManagement

TOKEN_ENDPOINT: str = '/api/v1/account/token'

SCRAPE_EXCHANGE_URL: str = 'https://scrape.exchange'

_LOGGER: logging.Logger = logging.getLogger(__name__)


# HTTP latency histogram for every request to the Scrape.Exchange API.
# Labelled by HTTP method and a coarse status class ('2xx'/'3xx'/'4xx'/
# '5xx'/'error' where 'error' means the request raised an exception
# before any response arrived).
METRIC_REQUEST_DURATION: Histogram = Histogram(
    'exchange_api_request_duration_seconds',
    'Duration of HTTP requests to scrape.exchange, by method and '
    'HTTP status class.',
    ['platform', 'scraper', 'method', 'status_class', 'worker_id'],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# Background upload queue metrics. Populated by the fire-and-forget
# ``enqueue_upload`` path so the dashboard can alert on a stuck queue
# or on sustained API failures.
METRIC_UPLOAD_QUEUE_DEPTH: Gauge = Gauge(
    'upload_queue_depth',
    'Number of background upload jobs currently waiting in the '
    'ExchangeClient fire-and-forget upload queue.',
    ['platform', 'scraper', 'worker_id'],
)
METRIC_UPLOAD_QUEUE_DROPPED: Counter = Counter(
    'upload_queue_dropped_total',
    'Background upload jobs dropped because the queue was full. The '
    'underlying asset file is left in the base directory and will be '
    'retried on the next scraper iteration.',
    ['platform', 'scraper', 'reason', 'worker_id'],
)
METRIC_BACKGROUND_UPLOADS: Counter = Counter(
    'uploads_completed_total',
    'Background upload jobs processed, labelled by outcome '
    '(created/updated/failure) and entity type '
    '(video/channel). "created" = HTTP 201 (new data), '
    '"updated" = HTTP 200 (existing data refreshed).',
    ['platform', 'scraper', 'entity', 'mode', 'status', 'worker_id'],
)
METRIC_429_RECEIVED: Counter = Counter(
    'api_429_received_total',
    'Number of HTTP 429 responses received from the '
    'Scrape.Exchange API, by method.',
    ['platform', 'scraper', 'method', 'worker_id'],
)


def _classify_rejected_response(
    status_code: int,
) -> tuple[str, Callable[..., None]]:
    '''
    Map a non-2xx HTTP status from scrape.exchange to the
    ``METRIC_BACKGROUND_UPLOADS`` ``status`` label and the right
    logger level for the "Background upload rejected by API"
    message.

    - ``404``: ``not_found`` / INFO. The resource doesn't exist
      on scrape.exchange — routine for GET lookups, not a
      failure.
    - ``5xx``: ``failure`` / ERROR. Server-side outage that the
      retry loop in :meth:`ExchangeClient.post` couldn't mask;
      operators should see it.
    - other (``400``/``401``/``403``/``422``/...): ``failure`` /
      WARNING. Client-side mismatch, typically a code or schema
      bug, not a sustained server problem.
    '''
    if status_code == 404:
        return 'not_found', _LOGGER.info
    if 500 <= status_code < 600:
        return 'failure', _LOGGER.error
    return 'failure', _LOGGER.warning


def _status_class(status_code: int | None) -> str:
    '''
    Return a coarse status-class label for the request-duration
    histogram. Returns ``'error'`` when no HTTP response was
    obtained (transport failure / timeout). Returns
    ``'not_found'`` for HTTP 404 specifically, since "the
    resource doesn't exist on scrape.exchange" is a routine
    answer for GET lookups (e.g. checking whether a channel is
    already known) and shouldn't be aggregated as a failure on
    dashboards alongside other 4xx codes. Everything else is
    bucketed by the leading digit (``'1xx'``..``'5xx'``).
    '''

    if status_code is None:
        return 'error'
    if status_code == 404:
        return 'not_found'
    return f'{status_code // 100}xx'


def _parse_retry_after(
    response: Response, default: float,
) -> float:
    '''
    Extract the ``Retry-After`` header value (seconds) from an
    HTTP 429 response.  Falls back to *default* when the header
    is absent or unparseable.
    '''
    header: str | None = response.headers.get('retry-after')
    if header is None:
        return default
    try:
        value: float = float(header)
        return max(1.0, value)
    except (ValueError, TypeError):
        return default


@dataclass
class _UploadJob:
    '''
    Work item enqueued by :meth:`ExchangeClient.enqueue_upload`.

    When ``file_manager`` and ``filename`` are both provided, the
    background worker calls ``file_manager.mark_uploaded(filename)``
    on HTTP 201, which atomically moves the on-disk asset from the
    base directory into the uploaded directory. This is what encodes
    "request completed successfully" for the rest of the scraper.

    When ``move_source_dir`` is also set, the worker calls
    ``file_manager.mark_uploaded_from(filename, move_source_dir)``
    instead — used by the priority-directory pipeline so the
    source file moves from an external staging directory into
    ``uploaded_dir`` rather than from ``base_dir``.
    '''

    url: str
    json: dict[str, Any]
    file_manager: 'AssetFileManagement | None' = None
    filename: str | None = None
    move_source_dir: 'Path | None' = None
    entity: str = 'unknown'
    log_extra: dict[str, Any] = field(default_factory=dict)


class ExchangeClient(AsyncClient):
    TOKEN_API: str = '/token'
    ME_API: str = '/me'
    POST_REGISTER_API: str = '/api/v1/account/register'
    POST_TOKEN_API: str = '/api/v1/account/token'
    GET_NEWEST_SCHEMA_API: str = '/api/v1/schema/newest'
    GET_ALL_SCHEMAS_API: str = '/api/v1/schema'
    GET_SCHEMA_COUNTERS_API: str = '/api/v1/schema/counters'
    GET_DATA_PARAM: str = '/api/v1/data/param'
    POST_SCHEMA_API: str = '/api/v1/schema'
    POST_DATA_API: str = '/api/v1/data'
    GET_CONTENT_API: str = '/api/v1/data/content'
    GET_FILTER_API: str = '/api/v1/filter'
    GET_STATS_API: str = '/api/v1/stats'
    GET_METRICS_API: str = '/api/v1/metrics'
    GET_STATUS_API: str = '/api/v1/status'

    # Background fire-and-forget upload queue tuning. These are class
    # attributes so tests and tools can override them before
    # instantiating the client.
    UPLOAD_QUEUE_MAX: int = 1024
    UPLOAD_WORKER_COUNT: int = 32

    def __init__(self, exchange_url: str = SCRAPE_EXCHANGE_URL) -> None:
        '''
        Initializes the ExchangeClient with API credentials and URL.

        :param api_key_id: The API key ID for authentication.
        :param api_key_secret: The API key secret for authentication.
        :param api_url: The base URL of the Scrape.Exchange API.
        :returns: (none)
        :raises: (non)
        '''

        self.exchange_url: str = exchange_url
        self.jwt_header: str | None = None
        self.headers: dict[str, str] = {}

        # Background upload machinery is lazily started on first
        # ``enqueue_upload`` call so tests and callers that never need
        # fire-and-forget uploads don't pay for idle worker tasks.
        self._upload_queue: asyncio.Queue[_UploadJob | None] | None = None
        self._upload_tasks: list[asyncio.Task] = []
        self._upload_shutdown: bool = False

        # No default Content-Type. httpx auto-sets the correct
        # value per body kind: ``application/json`` for ``json=``
        # callers, ``multipart/form-data; boundary=...`` for the
        # bulk-upload path that passes ``data=`` + ``files=``. A
        # client-level default would win the header-merge step in
        # :class:`httpx.Client._merge_headers` and stop httpx from
        # emitting the multipart boundary, leaving the server to
        # parse multipart bytes as JSON and reject every field as
        # missing.
        super().__init__(
            trust_env=False,
        )

    @property
    def authenticated_username(self) -> str | None:
        '''Return the username (``sub`` claim) embedded in the
        cached JWT, or ``None`` if no JWT is set or the token is
        unparseable. Used by callers that need to scope queries
        to records this client uploaded — the JWT issued by
        ``POST /api/v1/account/token`` carries the account
        username in ``sub``, so no extra HTTP call is needed.'''
        header: str | None = self.jwt_header
        if not header:
            return None
        token: str = header.removeprefix('Bearer ').strip()
        parts: list[str] = token.split('.')
        if len(parts) != 3:
            return None
        try:
            payload_b64: str = (
                parts[1] + '=' * (-len(parts[1]) % 4)
            )
            payload: dict = json.loads(
                base64.urlsafe_b64decode(payload_b64),
            )
        except (ValueError, TypeError):
            return None
        sub: Any = payload.get('sub')
        return sub if isinstance(sub, str) else None

    @staticmethod
    async def setup(
        api_key_id: str, api_key_secret: str,
        exchange_url: str = SCRAPE_EXCHANGE_URL,
        retries: int = 3, delay: float = 1.0,
    ) -> Self:
        '''
        Factory to create an instance of ExchangeClient and
        the JWT token for later use.

        When the ``EXCHANGE_JWT`` environment variable is set
        (written by the supervisor), the pre-fetched JWT is
        used directly and the token endpoint is not contacted.
        This avoids N redundant auth calls when N child
        processes start simultaneously.

        :param api_key_id: The API key ID for authentication.
        :param api_key_secret: The API key secret.
        :param exchange_url: The base URL of the API.
        :param retries: Number of retries on failure.
        :param delay: Initial delay in seconds between retries
                      (doubles each retry).
        :returns: An initialized instance of ExchangeClient.
        :raises: Exception if the JWT cannot be obtained after
                 retries.
        '''

        self = ExchangeClient(exchange_url)

        # If the supervisor already fetched a JWT, use it.
        env_jwt: str | None = os.environ.get('EXCHANGE_JWT')
        if env_jwt:
            _LOGGER.info(
                'Using pre-fetched JWT from supervisor',
            )
            self.jwt_header = env_jwt
            self.headers['Authorization'] = self.jwt_header
            return self

        attempt: int = 0
        while True:
            try:
                self.jwt_header = (
                    await ExchangeClient.get_jwt_token(
                        api_key_id, api_key_secret,
                        self.exchange_url,
                    )
                )
                break
            except Exception as exc:
                if attempt < retries:
                    sleep_for: float = delay * (2 ** attempt)
                    _LOGGER.debug(
                        'Retrying ExchangeClient setup',
                        exc=exc,
                        extra={
                            'exchange_url': (
                                self.exchange_url
                            ),
                            'exc_type': (
                                type(exc).__name__
                            ),
                            'retries_left': (
                                retries - attempt
                            ),
                        },
                    )
                    await asyncio.sleep(sleep_for)
                    attempt += 1
                    continue
                _LOGGER.warning(
                    'Failed to set up ExchangeClient '
                    'after attempts',
                    exc=exc,
                    extra={
                        'exchange_url': (
                            self.exchange_url
                        ),
                        'attempts': retries + 1,
                        'exc_type': (
                            type(exc).__name__
                        ),
                    },
                )
                raise
        self.headers['Authorization'] = self.jwt_header
        return self

    @staticmethod
    async def get_jwt_token(api_key_id: str, api_key_secret: str,
                            api_url: str = SCRAPE_EXCHANGE_URL) -> str:
        '''
        Retrieves a JWT token from the Scrape.Exchange API using the provided
        API credentials.

        :returns: The JWT token as a string.
        :raises: RuntimeError if the token retrieval fails.
        '''

        async with AsyncClient(
            trust_env=False, timeout=30.0,
        ) as client:
            response: Response = await client.post(
                f'{api_url}{TOKEN_ENDPOINT}',
                data={
                    'api_key_id': api_key_id,
                    'api_key_secret': api_key_secret,
                },
            )
            _LOGGER.debug(
                'Token endpoint response',
                extra={
                    'status_code': response.status_code,
                    'response_text': response.text,
                },
            )
            if response.status_code != 200:
                _LOGGER.warning(
                    'Failed to retrieve JWT token',
                    extra={'response_text': response.text},
                )
                raise RuntimeError(
                    f'Failed to retrieve JWT token: {response.text}'
                )
            token_data: dict[str, str] = response.json()

            jwt_header: str = ExchangeClient.get_auth_header(
                token_data['access_token']
            )

            return jwt_header

    async def get(
        self, url: str, retries: int = 3,
        delay: float = 1.0, **kwargs,
    ) -> Response:
        '''
        Overrides the AsyncClient get method to include the JWT
        token in the headers, rate-limit via
        :class:`ScrapeExchangeRateLimiter`, and retry on HTTP 429.

        :param url: The URL to send the GET request to.
        :returns: The response from the GET request.
        :raises: Exception if the GET request fails after the
                 specified number of retries.
        '''

        limiter: ScrapeExchangeRateLimiter | None = (
            ScrapeExchangeRateLimiter._instance
        )
        if limiter is not None:
            await limiter.acquire(ScrapeExchangeCallType.GET)

        _LOGGER.debug('HTTP GET', extra={'url': url})
        start: datetime = datetime.now(UTC)
        try:
            result: Response = await super().get(
                url, **kwargs,
            )
        except Exception as exc:
            duration: float = (
                (datetime.now(UTC) - start).total_seconds()
            )
            METRIC_REQUEST_DURATION.labels(
                platform='scrape_exchange',
                scraper='exchange_client',
                method='get',
                status_class='error',
                worker_id=get_worker_id(),
            ).observe(duration)
            if retries > 0:
                await asyncio.sleep(delay)
                _LOGGER.debug(
                    'Retrying GET',
                    exc=exc,
                    extra={
                        'url': url,
                        'exc_type': type(exc).__name__,
                        'retries_left': retries,
                    },
                )
                return await self.get(
                    url, retries=retries - 1,
                    delay=delay * 2, **kwargs,
                )
            _LOGGER.warning(
                'GET request failed after retries',
                exc=exc,
                extra={
                    'url': url,
                    'exc_type': type(exc).__name__,
                },
            )
            raise exc

        duration = (
            (datetime.now(UTC) - start).total_seconds()
        )
        METRIC_REQUEST_DURATION.labels(
            platform='scrape_exchange',
            scraper='exchange_client',
            method='get',
            status_class=_status_class(result.status_code),
            worker_id=get_worker_id(),
        ).observe(duration)

        if result.status_code == 429:
            METRIC_429_RECEIVED.labels(
                platform='scrape_exchange',
                scraper='exchange_client',
                method='get',
                worker_id=get_worker_id(),
            ).inc()
            penalty: float = _parse_retry_after(
                result, DEFAULT_429_PENALTY_SECONDS,
            )
            _LOGGER.warning(
                'HTTP 429 from Scrape.Exchange on GET',
                extra={
                    'url': url,
                    'penalty': penalty,
                    'retries_left': retries,
                },
            )
            if limiter is not None:
                await limiter.penalise(
                    ScrapeExchangeCallType.GET,
                    None, penalty,
                )
            if retries > 0:
                await asyncio.sleep(penalty)
                return await self.get(
                    url, retries=retries - 1,
                    delay=delay, **kwargs,
                )
            return result

        _LOGGER.debug(
            'HTTP GET completed',
            extra={
                'url': url,
                'status_code': result.status_code,
                'duration': duration,
            },
        )
        return result

    async def post(
        self, url: str, retries: int = 3,
        delay: float = 1.0, **kwargs,
    ) -> Response:
        '''
        Overrides the AsyncClient post method to include retry
        logic with exponential backoff, rate-limit via
        :class:`ScrapeExchangeRateLimiter`, and retry on HTTP 429.

        :param url: The URL to send the POST request to.
        :param retries: Number of retries on failure.
        :param delay: Initial delay in seconds between retries
                      (doubles each retry).
        :returns: The response from the POST request.
        :raises: Exception if the POST request fails after the
                 specified number of retries.
        '''

        limiter: ScrapeExchangeRateLimiter | None = (
            ScrapeExchangeRateLimiter._instance
        )
        if limiter is not None:
            await limiter.acquire(ScrapeExchangeCallType.POST)

        _LOGGER.debug('HTTP POST', extra={'url': url})
        start: datetime = datetime.now(UTC)
        try:
            result: Response = await super().post(
                url, **kwargs,
            )
        except Exception as exc:
            duration: float = (
                (datetime.now(UTC) - start).total_seconds()
            )
            METRIC_REQUEST_DURATION.labels(
                platform='scrape_exchange',
                scraper='exchange_client',
                method='post',
                status_class='error',
                worker_id=get_worker_id(),
            ).observe(duration)
            if retries > 0:
                await asyncio.sleep(delay)
                _LOGGER.debug(
                    'Retrying POST',
                    exc=exc,
                    extra={
                        'url': url,
                        'exc_type': type(exc).__name__,
                        'retries_left': retries,
                    },
                )
                return await self.post(
                    url, retries=retries - 1,
                    delay=delay * 2, **kwargs,
                )
            _LOGGER.warning(
                'POST request failed after retries',
                exc=exc,
                extra={
                    'url': url,
                    'exc_type': type(exc).__name__,
                },
            )
            raise exc

        duration = (
            (datetime.now(UTC) - start).total_seconds()
        )
        METRIC_REQUEST_DURATION.labels(
            platform='scrape_exchange',
            scraper='exchange_client',
            method='post',
            status_class=_status_class(result.status_code),
            worker_id=get_worker_id(),
        ).observe(duration)

        if result.status_code == 429:
            METRIC_429_RECEIVED.labels(
                platform='scrape_exchange',
                scraper='exchange_client',
                method='post',
                worker_id=get_worker_id(),
            ).inc()
            penalty: float = _parse_retry_after(
                result, DEFAULT_429_PENALTY_SECONDS,
            )
            _LOGGER.warning(
                'HTTP 429 from Scrape.Exchange on POST',
                extra={
                    'url': url,
                    'penalty': penalty,
                    'retries_left': retries,
                },
            )
            if limiter is not None:
                await limiter.penalise(
                    ScrapeExchangeCallType.POST,
                    None, penalty,
                )
            if retries > 0:
                await asyncio.sleep(penalty)
                return await self.post(
                    url, retries=retries - 1,
                    delay=delay, **kwargs,
                )
            return result

        _LOGGER.debug(
            'HTTP POST completed',
            extra={
                'url': url,
                'status_code': result.status_code,
                'duration': duration,
            },
        )
        return result

    @staticmethod
    def get_auth_header(token: str) -> str:
        return f'Bearer {token}'

    # ------------------------------------------------------------------
    # Background fire-and-forget upload queue
    # ------------------------------------------------------------------

    def enqueue_upload(
        self,
        url: str,
        *,
        json: dict[str, Any],
        file_manager: 'AssetFileManagement | None' = None,
        filename: str | None = None,
        move_source_dir: 'Path | None' = None,
        entity: str = 'unknown',
        log_extra: dict[str, Any] | None = None,
    ) -> bool:
        '''
        Fire-and-forget upload to the Scrape.Exchange data API.

        Returns immediately. A background worker performs the actual
        POST (reusing :meth:`post`'s retry logic) and, on HTTP 201,
        calls ``file_manager.mark_uploaded(filename)`` to atomically
        move the asset from ``base_dir`` to ``uploaded_dir``. This file
        move is what represents "upload completed" for the rest of the
        scraper: files left in ``base_dir`` are retried on the next
        iteration.

        The caller never blocks and never sees a failure: if the
        upload queue is full (i.e. the API is down and retries are
        backing up), the enqueue is dropped, counted via the
        ``upload_queue_dropped_total`` counter, and
        the file stays in ``base_dir`` for a later retry.

        :param url: Full URL of the upload endpoint.
        :param json: JSON body to POST.
        :param file_manager: Optional asset file manager that owns the
            file whose successful upload should be marked.
        :param filename: Bare filename (relative to ``base_dir``) to
            mark as uploaded on success.
        :param log_extra: Additional structured fields to attach to
            background-worker log messages.
        :returns: ``True`` if the job was enqueued, ``False`` if it
            was dropped (queue full or client shutting down).
        '''

        if self._upload_shutdown:
            return False

        self._ensure_upload_tasks()

        job: _UploadJob = _UploadJob(
            url=url,
            json=json,
            file_manager=file_manager,
            filename=filename,
            move_source_dir=move_source_dir,
            entity=entity,
            log_extra=dict(log_extra or {}),
        )
        try:
            self._upload_queue.put_nowait(job)
        except asyncio.QueueFull:
            METRIC_UPLOAD_QUEUE_DROPPED.labels(
                platform='scrape_exchange',
                scraper='exchange_client',
                reason='queue_full',
                worker_id=get_worker_id(),
            ).inc()
            _LOGGER.warning(
                'Upload queue full; dropping enqueue request',
                extra={
                    'filename': filename,
                    'queue_max': self.UPLOAD_QUEUE_MAX,
                    **(log_extra or {}),
                },
            )
            return False

        METRIC_UPLOAD_QUEUE_DEPTH.labels(
            platform='scrape_exchange',
            scraper='exchange_client',
            worker_id=get_worker_id(),
        ).set(self._upload_queue.qsize())
        return True

    def _ensure_upload_tasks(self) -> None:
        '''
        Lazily create the background upload queue and worker tasks
        on the first ``enqueue_upload`` call. Requires a running
        event loop, which all scraper tool call sites satisfy.
        '''

        if self._upload_queue is not None:
            return
        self._upload_queue = asyncio.Queue(maxsize=self.UPLOAD_QUEUE_MAX)
        for i in range(self.UPLOAD_WORKER_COUNT):
            task: asyncio.Task = asyncio.create_task(
                self._upload_worker_loop(),
                name=f'ExchangeClient.upload_worker-{i}',
            )
            self._upload_tasks.append(task)

    async def _upload_worker_loop(self) -> None:
        '''
        Drain jobs from the upload queue until the task is cancelled
        (which happens in :meth:`drain_uploads`). Exceptions in
        individual jobs are logged and swallowed so one bad payload
        cannot kill the worker.
        '''

        assert self._upload_queue is not None
        while True:
            # Cancellation propagates out of ``queue.get()`` during
            # drain; let it bubble up so asyncio marks the task as
            # properly cancelled. ``drain_uploads`` waits on a gather
            # with ``return_exceptions=True`` so the raise is benign.
            job: _UploadJob = await self._upload_queue.get()
            try:
                await self._perform_background_upload(job)
            except Exception as exc:
                _LOGGER.error(
                    'Unexpected error in upload worker',
                    exc=exc,
                    extra={
                        'upload_filename': job.filename,
                        **job.log_extra,
                    },
                )
                METRIC_BACKGROUND_UPLOADS.labels(
                    platform='youtube',
                    scraper='exchange_client',
                    entity=job.entity,
                    mode='background',
                    status='failure',
                    worker_id=get_worker_id(),
                ).inc()
            finally:
                self._upload_queue.task_done()
                METRIC_UPLOAD_QUEUE_DEPTH.labels(
                    platform='scrape_exchange',
                    scraper='exchange_client',
                    worker_id=get_worker_id(),
                ).set(self._upload_queue.qsize())

    @staticmethod
    async def _mark_job_uploaded(job: _UploadJob) -> None:
        '''
        Dispatch to the right ``mark_uploaded*`` flavour for *job*.
        When ``job.move_source_dir`` is set the source file lives
        outside ``base_dir`` (the priority-directory pipeline) and
        we move it into ``uploaded_dir`` from there; otherwise the
        regular base-dir path applies.
        '''
        if job.move_source_dir is not None:
            await job.file_manager.mark_uploaded_from(
                job.filename, job.move_source_dir,
            )
        else:
            await job.file_manager.mark_uploaded(job.filename)

    async def _perform_background_upload(
        self, job: _UploadJob,
    ) -> None:
        '''
        Execute a single upload job: POST the payload (reusing
        :meth:`post`'s retry loop), and on HTTP 200/201 mark
        the file as uploaded via
        :meth:`AssetFileManagement.mark_uploaded`.

        The API returns 201 for newly created data and 200 for
        existing data that was updated. Both are success and
        are tracked separately via the ``outcome`` label
        (``created`` vs ``updated``).

        If the POST ultimately returns 429 (i.e. retries inside
        :meth:`post` were also exhausted), the job is re-enqueued
        so it will be retried after the rate-limiter penalty has
        drained and other queued jobs have been processed.
        '''

        try:
            resp: Response = await self.post(
                job.url, json=job.json,
            )
        except Exception as exc:
            METRIC_BACKGROUND_UPLOADS.labels(
                platform='youtube',
                scraper='exchange_client',
                entity=job.entity,
                mode='background',
                status='failure',
                worker_id=get_worker_id(),
            ).inc()
            _LOGGER.warning(
                'Background upload POST failed after retries',
                exc=exc,
                extra={
                    'filename': job.filename,
                    **job.log_extra,
                },
            )
            return

        if resp.status_code in (200, 201):
            outcome: str = (
                'created' if resp.status_code == 201
                else 'updated'
            )
            if (job.file_manager is not None
                    and job.filename is not None):
                try:
                    await self._mark_job_uploaded(job)
                except OSError as exc:
                    METRIC_BACKGROUND_UPLOADS.labels(
                        platform='youtube',
                        scraper='exchange_client',
                        entity=job.entity,
                        mode='background',
                        status='failure',
                        worker_id=get_worker_id(),
                    ).inc()
                    # ERROR (not WARNING): the API accepted the
                    # record but local bookkeeping broke. The file
                    # will be re-uploaded next sweep and the API
                    # will dedupe, but local disk is misbehaving
                    # and an operator should investigate.
                    _LOGGER.error(
                        'Upload succeeded but '
                        'mark_uploaded failed',
                        exc=exc,
                        extra={
                            'filename': job.filename,
                            **job.log_extra,
                        },
                    )
                    return
            METRIC_BACKGROUND_UPLOADS.labels(
                platform='youtube',
                scraper='exchange_client',
                entity=job.entity,
                mode='background',
                status=outcome,
                worker_id=get_worker_id(),
            ).inc()
            _LOGGER.debug(
                'Background upload succeeded',
                extra={
                    'filename': job.filename,
                    'outcome': outcome,
                    **job.log_extra,
                },
            )
            return

        if resp.status_code == 429:
            # post() already penalised the rate limiter and
            # retried; if we still got 429 the server is under
            # sustained pressure.  Re-enqueue so the job is
            # retried after the penalty has elapsed and other
            # queued work has drained.
            _LOGGER.warning(
                'Background upload got 429 after retries, '
                're-enqueueing',
                extra={
                    'filename': job.filename,
                    **job.log_extra,
                },
            )
            try:
                self._upload_queue.put_nowait(job)
            except asyncio.QueueFull:
                METRIC_UPLOAD_QUEUE_DROPPED.labels(
                    platform='scrape_exchange',
                    scraper='exchange_client',
                    reason='queue_full',
                    worker_id=get_worker_id(),
                ).inc()
                _LOGGER.warning(
                    'Re-enqueue after 429 failed: '
                    'queue full',
                    extra={
                        'filename': job.filename,
                        **job.log_extra,
                    },
                )
            return

        status, log_fn = _classify_rejected_response(
            resp.status_code,
        )
        METRIC_BACKGROUND_UPLOADS.labels(
            platform='youtube',
            scraper='exchange_client',
            entity=job.entity,
            mode='background',
            status=status,
            worker_id=get_worker_id(),
        ).inc()
        log_fn(
            'Background upload rejected by API',
            extra={
                'filename': job.filename,
                'status_code': resp.status_code,
                'response_text': resp.text,
                **job.log_extra,
            },
        )

    async def drain_uploads(self, timeout: float = 10.0) -> None:
        '''
        Stop accepting new background uploads and wait for in-flight
        and queued jobs to finish, up to *timeout* seconds. Call this
        from a tool's shutdown path (e.g. a SIGTERM handler) before
        tearing down the event loop.

        After the timeout any remaining worker tasks are cancelled;
        unfinished jobs' asset files are left in ``base_dir`` and
        will be retried on the next scraper run.
        '''

        self._upload_shutdown = True
        if self._upload_queue is None or not self._upload_tasks:
            return

        remaining: int = self._upload_queue.qsize()
        _LOGGER.info(
            'Draining background upload queue',
            extra={
                'queued': remaining,
                'tasks': len(self._upload_tasks),
                'timeout': timeout,
            },
        )

        # Wait for every currently-queued item (plus in-flight jobs)
        # to be processed. ``_upload_shutdown`` blocks any new
        # enqueues, so ``join()`` is guaranteed to make progress.
        try:
            await asyncio.wait_for(
                self._upload_queue.join(), timeout=timeout
            )
        except asyncio.TimeoutError:
            dropped: int = self._upload_queue.qsize()
            _LOGGER.warning(
                'Drain timeout exceeded; cancelling workers',
                extra={
                    'timeout': timeout,
                    'queued_on_timeout': dropped,
                },
            )

        # Whether we drained cleanly or timed out, cancel the worker
        # tasks so they exit out of their blocking ``queue.get()``.
        for task in self._upload_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(
            *self._upload_tasks, return_exceptions=True
        )
        self._upload_tasks.clear()
        self._upload_queue = None
        METRIC_UPLOAD_QUEUE_DEPTH.labels(
            platform='scrape_exchange',
            scraper='exchange_client',
            worker_id=get_worker_id(),
        ).set(0)

    async def aclose(self) -> None:
        '''
        Close the underlying HTTPX client, draining the background
        upload queue first so in-flight jobs get a chance to finish
        before sockets are torn down.
        '''

        if self._upload_queue is not None and not self._upload_shutdown:
            await self.drain_uploads()
        await super().aclose()
