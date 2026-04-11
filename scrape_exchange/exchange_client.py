'''
Scrape.Exchange client used for uploading content

:author: boinko@scrape.exchange
:copyright: 2026
:license: GPLv3
'''


import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging

from typing import Any, Self, TYPE_CHECKING

from httpx import AsyncClient, Response
from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from scrape_exchange.file_management import AssetFileManagement

TOKEN_ENDPOINT: str = '/api/account/v1/token'

SCRAPE_EXCHANGE_URL: str = 'https://scrape.exchange'

_LOGGER: logging.Logger = logging.getLogger(__name__)


# HTTP latency histogram for every request to the Scrape.Exchange API.
# Labelled by HTTP method and a coarse status class ('2xx'/'3xx'/'4xx'/
# '5xx'/'error' where 'error' means the request raised an exception
# before any response arrived).
METRIC_REQUEST_DURATION: Histogram = Histogram(
    'exchange_client_request_duration_seconds',
    'Duration of HTTP requests to scrape.exchange, by method and '
    'HTTP status class.',
    ['method', 'status_class'],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# Background upload queue metrics. Populated by the fire-and-forget
# ``enqueue_upload`` path so the dashboard can alert on a stuck queue
# or on sustained API failures.
METRIC_UPLOAD_QUEUE_DEPTH: Gauge = Gauge(
    'exchange_client_upload_queue_depth',
    'Number of background upload jobs currently waiting in the '
    'ExchangeClient fire-and-forget upload queue.',
)
METRIC_UPLOAD_QUEUE_DROPPED: Counter = Counter(
    'exchange_client_upload_queue_dropped_total',
    'Background upload jobs dropped because the queue was full. The '
    'underlying asset file is left in the base directory and will be '
    'retried on the next scraper iteration.',
)
METRIC_BACKGROUND_UPLOADS: Counter = Counter(
    'exchange_client_background_uploads_total',
    'Background upload jobs processed, labelled by outcome '
    '(success/failure).',
    ['outcome'],
)


def _status_class(status_code: int | None) -> str:
    '''Return a coarse ``'Nxx'`` status-class label or ``'error'``.'''

    if status_code is None:
        return 'error'
    return f'{status_code // 100}xx'


@dataclass
class _UploadJob:
    '''
    Work item enqueued by :meth:`ExchangeClient.enqueue_upload`.

    When ``file_manager`` and ``filename`` are both provided, the
    background worker calls ``file_manager.mark_uploaded(filename)``
    on HTTP 201, which atomically moves the on-disk asset from the
    base directory into the uploaded directory. This is what encodes
    "request completed successfully" for the rest of the scraper.
    '''

    url: str
    json: dict[str, Any]
    file_manager: 'AssetFileManagement | None' = None
    filename: str | None = None
    log_extra: dict[str, Any] = field(default_factory=dict)


class ExchangeClient(AsyncClient):
    TOKEN_API: str = '/token'
    ME_API: str = '/me'
    POST_REGISTER_API: str = '/api/account/v1/register'
    POST_TOKEN_API: str = '/api/account/v1/token'
    GET_NEWEST_SCHEMA_API: str = '/api/schema/v1/newest'
    GET_ALL_SCHEMAS_API: str = '/api/schema/v1'
    GET_SCHEMA_COUNTERS_API: str = '/api/schema/v1/counters'
    GET_DATA_PARAM: str = '/api/data/v1/param'
    POST_SCHEMA_API: str = '/api/schema/v1'
    POST_DATA_API: str = '/api/data/v1'
    GET_CONTENT_API: str = '/api/data/v1/content'
    GET_FILTER_API: str = '/api/filter/v1'
    GET_STATS_API: str = '/api/stats/v1'
    GET_METRICS_API: str = '/api/metrics/v1'
    GET_STATUS_API: str = '/api/status/v1'

    # Background fire-and-forget upload queue tuning. These are class
    # attributes so tests and tools can override them before
    # instantiating the client.
    UPLOAD_QUEUE_MAX: int = 1024
    UPLOAD_WORKER_COUNT: int = 8

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
        self._upload_workers: list[asyncio.Task] = []
        self._upload_shutdown: bool = False

        super().__init__(
            headers={
                'Content-Type': 'application/json',
            },
            trust_env=False,
        )

    @staticmethod
    async def setup(api_key_id: str, api_key_secret: str,
                    exchange_url: str = SCRAPE_EXCHANGE_URL,
                    retries: int = 3, delay: float = 1.0) -> Self:
        '''
        Factory to create an instance of ExchangeClient and the JWT token for
        later us.

        :param api_key_id: The API key ID for authentication.
        :param api_key_secret: The API key secret for authentication.
        :param api_url: The base URL of the Scrape.Exchange API.
        :param retries: Number of retries on failure.
        :param delay: Initial delay in seconds between retries (doubles each
        retry).
        :returns: An initialized instance of ExchangeClient.
        :raises: (none)
        '''

        self = ExchangeClient(exchange_url)
        attempt: int = 0
        while True:
            try:
                self.jwt_header = await ExchangeClient.get_jwt_token(
                    api_key_id, api_key_secret, self.exchange_url
                )
                break
            except Exception as exc:
                if attempt < retries:
                    sleep_for: float = delay * (2 ** attempt)
                    _LOGGER.debug(
                        'Retrying ExchangeClient setup',
                        exc=exc,
                        extra={
                            'exchange_url': self.exchange_url,
                            'exc_type': type(exc).__name__,
                            'retries_left': retries - attempt,
                        },
                    )
                    await asyncio.sleep(sleep_for)
                    attempt += 1
                    continue
                _LOGGER.warning(
                    'Failed to set up ExchangeClient after attempts',
                    exc=exc,
                    extra={
                        'exchange_url': self.exchange_url,
                        'attempts': retries + 1,
                        'exc_type': type(exc).__name__,
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

        async with AsyncClient(trust_env=False) as client:
            response: Response = await client.post(
                f'{api_url}{TOKEN_ENDPOINT}',
                data={
                    'api_key_id': api_key_id,
                    'api_key_secret': api_key_secret
                }
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

    async def get(self, url: str, retries: int = 3, delay: float = 1.0,
                  **kwargs) -> Response:
        '''
        Overrides the AsyncClient get method to include the JWT token in the
        headers.

        :param url: The URL to send the GET request to.
        :returns: The response from the GET request.
        :raises: Exception if the GET request fails after the specified
        number of retries.
        '''

        _LOGGER.debug('HTTP GET', extra={'url': url})
        start: datetime = datetime.now(UTC)
        try:
            result: Response = await super().get(url, **kwargs)
        except Exception as exc:
            duration: float = (datetime.now(UTC) - start).total_seconds()
            METRIC_REQUEST_DURATION.labels(
                method='get', status_class='error'
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
                    url, retries=retries - 1, delay=delay*2, **kwargs
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

        duration = (datetime.now(UTC) - start).total_seconds()
        METRIC_REQUEST_DURATION.labels(
            method='get', status_class=_status_class(result.status_code)
        ).observe(duration)
        _LOGGER.debug(
            'HTTP GET completed',
            extra={
                'url': url,
                'status_code': result.status_code,
                'duration': duration,
            },
        )
        return result

    async def post(self, url: str, retries: int = 3, delay: float = 1.0,
                   **kwargs) -> Response:
        '''
        Overrides the AsyncClient post method to include retry logic with
        exponential backoff.

        :param url: The URL to send the POST request to.
        :param retries: Number of retries on failure.
        :param delay: Initial delay in seconds between retries (doubles each
        retry).
        :returns: The response from the POST request.
        :raises: Exception if the POST request fails after the specified
        number of retries.
        '''

        _LOGGER.debug('HTTP POST', extra={'url': url})
        start: datetime = datetime.now(UTC)
        try:
            result: Response = await super().post(url, **kwargs)
        except Exception as exc:
            duration: float = (datetime.now(UTC) - start).total_seconds()
            METRIC_REQUEST_DURATION.labels(
                method='post', status_class='error'
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
                    url, retries=retries - 1, delay=delay*2, **kwargs
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

        duration = (datetime.now(UTC) - start).total_seconds()
        METRIC_REQUEST_DURATION.labels(
            method='post', status_class=_status_class(result.status_code)
        ).observe(duration)
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
        ``exchange_client_upload_queue_dropped_total`` counter, and
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

        self._ensure_upload_workers()

        job: _UploadJob = _UploadJob(
            url=url,
            json=json,
            file_manager=file_manager,
            filename=filename,
            log_extra=dict(log_extra or {}),
        )
        try:
            self._upload_queue.put_nowait(job)
        except asyncio.QueueFull:
            METRIC_UPLOAD_QUEUE_DROPPED.inc()
            _LOGGER.warning(
                'Upload queue full; dropping enqueue request',
                extra={
                    'filename': filename,
                    'queue_max': self.UPLOAD_QUEUE_MAX,
                    **(log_extra or {}),
                },
            )
            return False

        METRIC_UPLOAD_QUEUE_DEPTH.set(self._upload_queue.qsize())
        return True

    def _ensure_upload_workers(self) -> None:
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
            self._upload_workers.append(task)

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
                    outcome='failure'
                ).inc()
            finally:
                self._upload_queue.task_done()
                METRIC_UPLOAD_QUEUE_DEPTH.set(
                    self._upload_queue.qsize()
                )

    async def _perform_background_upload(self, job: _UploadJob) -> None:
        '''
        Execute a single upload job: POST the payload (reusing
        :meth:`post`'s retry loop), and on HTTP 201 mark the file as
        uploaded via :meth:`AssetFileManagement.mark_uploaded`.
        '''

        try:
            resp: Response = await self.post(job.url, json=job.json)
        except Exception as exc:
            METRIC_BACKGROUND_UPLOADS.labels(outcome='failure').inc()
            _LOGGER.warning(
                'Background upload POST failed after retries',
                exc=exc,
                extra={'filename': job.filename, **job.log_extra},
            )
            return

        if resp.status_code == 201:
            if job.file_manager is not None and job.filename is not None:
                try:
                    await job.file_manager.mark_uploaded(job.filename)
                except OSError as exc:
                    METRIC_BACKGROUND_UPLOADS.labels(
                        outcome='failure'
                    ).inc()
                    _LOGGER.warning(
                        'Upload succeeded but mark_uploaded failed',
                        exc=exc,
                        extra={
                            'filename': job.filename,
                            **job.log_extra,
                        },
                    )
                    return
            METRIC_BACKGROUND_UPLOADS.labels(outcome='success').inc()
            _LOGGER.debug(
                'Background upload succeeded',
                extra={'filename': job.filename, **job.log_extra},
            )
            return

        METRIC_BACKGROUND_UPLOADS.labels(outcome='failure').inc()
        _LOGGER.warning(
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
        if self._upload_queue is None or not self._upload_workers:
            return

        remaining: int = self._upload_queue.qsize()
        _LOGGER.info(
            'Draining background upload queue',
            extra={
                'queued': remaining,
                'workers': len(self._upload_workers),
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
        for task in self._upload_workers:
            if not task.done():
                task.cancel()
        await asyncio.gather(
            *self._upload_workers, return_exceptions=True
        )
        self._upload_workers.clear()
        self._upload_queue = None
        METRIC_UPLOAD_QUEUE_DEPTH.set(0)

    async def aclose(self) -> None:
        '''
        Close the underlying HTTPX client, draining the background
        upload queue first so in-flight jobs get a chance to finish
        before sockets are torn down.
        '''

        if self._upload_queue is not None and not self._upload_shutdown:
            await self.drain_uploads()
        await super().aclose()
