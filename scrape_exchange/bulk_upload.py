'''
Shared client-side bulk-upload helpers.

The Scrape Exchange ``POST /api/v1/bulk`` endpoint accepts a single
multipart file containing many records and returns a ``job_id`` for
async processing. Per-record outcomes are exposed at ``GET
/api/v1/bulk/{job_id}/results``; terminal-status notification is
streamed over the ``WS /api/v1/bulk/progress/{job_id}`` WebSocket.

This module factors out the entity-agnostic plumbing — POST, WS
streaming, results fetch, file reconciliation — so that the YouTube
channel and video scrapers (and any future bulk-upload caller) can
share the same code path. Per-tool concerns kept on the caller
side: settings storage, Prometheus metrics, and the entity-specific
record builder that turns a source file into ``(content_id, line)``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
from dataclasses import dataclass
from uuid import uuid4

import orjson
import websockets
from httpx import Response, Timeout

from .exchange_client import ExchangeClient
from .file_management import AssetFileManagement


_LOGGER: logging.Logger = logging.getLogger(__name__)


BULK_API_PATH: str = '/api/v1/bulk'
TERMINAL_BULK_STATUSES: frozenset[str] = frozenset({
    'completed', 'failed',
})


@dataclass
class BulkBatchOutcome:
    '''
    Result summary for a dispatched bulk batch. ``status`` describes
    the lifecycle outcome; ``success``/``failed``/``missing`` are
    per-record counts after reconciliation. Callers turn these into
    Prometheus metrics with their own label scheme.

    ``status`` values:

    * ``'completed'`` — POST 201, terminal status reached, results
      fetched and applied. The counts reflect the actual outcome.
    * ``'post_error'`` — the POST itself raised. No job exists
      server-side from this attempt; counts are zero.
    * ``'post_rejected'`` — POST returned non-201 (validation,
      schema, disk, etc.). Counts zero.
    * ``'no_job_id'`` — POST 201 but the response did not contain
      ``job_id``. Counts zero.
    * ``'progress_failed'`` — terminal status was not observed
      (timeout, error close, etc.). Counts zero.

    On any non-``completed`` status, source files are left in
    ``base_dir`` for the next iteration to retry.
    '''
    status: str
    job_id: str | None
    success: int
    failed: int
    missing: int


def bulk_progress_ws_url(exchange_url: str, job_id: str) -> str:
    '''
    Translate the HTTP exchange URL into the WebSocket URL for the
    bulk progress endpoint. ``http://`` becomes ``ws://`` and
    ``https://`` becomes ``wss://``; anything else is left
    unchanged so test fixtures with custom schemes still work.
    '''
    if exchange_url.startswith('https://'):
        ws_base: str = 'wss://' + exchange_url[len('https://'):]
    elif exchange_url.startswith('http://'):
        ws_base = 'ws://' + exchange_url[len('http://'):]
    else:
        ws_base = exchange_url
    return f'{ws_base}{BULK_API_PATH}/progress/{job_id}'


async def _handle_ws_close_error(
    job_id: str,
    exc: 'websockets.exceptions.ConnectionClosedError',
    exchange_url: str,
    client: ExchangeClient,
    deadline: float,
) -> bool:
    '''
    Decide what to do when the bulk-progress WebSocket closed
    with an error frame. Logs at the right level, and on the
    expected service-restart codes (1001/1012) falls back to
    polling the status endpoint so the caller can still apply
    per-record results instead of dropping the whole batch.
    Returns ``True`` if a terminal status was eventually
    observed, ``False`` otherwise.
    '''
    _log_ws_closed_with_error(job_id, exc)
    rcvd = exc.rcvd
    code: int | None = rcvd.code if rcvd else None
    if code in (1001, 1012):
        return await _poll_status_until_terminal(
            job_id, exchange_url, client, deadline,
        )
    return False


async def _poll_status_until_terminal(
    job_id: str,
    exchange_url: str,
    client: ExchangeClient,
    deadline: float,
) -> bool:
    '''
    Poll ``GET /api/v1/bulk?job_id=...`` until the job reaches a
    terminal status (``completed``/``failed``) or *deadline* — a
    monotonic wall clock from
    ``asyncio.get_running_loop().time()`` — is reached.

    Used as a fallback after a WebSocket service-restart close so
    the scraper still observes the bulk job's outcome instead of
    leaving every interrupted batch for retry. Returns ``True`` if
    a terminal status was seen, ``False`` on timeout, transport
    error, or non-200 response.
    '''
    status_url: str = f'{exchange_url}{BULK_API_PATH}'
    poll_interval: float = 2.0
    while True:
        remaining: float = (
            deadline - asyncio.get_running_loop().time()
        )
        if remaining <= 0:
            logging.warning(
                'Bulk progress poll timed out',
                extra={'job_id': job_id},
            )
            return False
        try:
            resp: Response = await client.get(
                status_url, params={'job_id': job_id},
            )
        except Exception as exc:
            logging.warning(
                'Bulk progress status fetch failed',
                exc=exc,
                extra={'job_id': job_id},
            )
            return False
        if resp.status_code != 200:
            logging.warning(
                'Bulk progress status non-200',
                extra={
                    'job_id': job_id,
                    'status_code': resp.status_code,
                    'response_text': resp.text,
                },
            )
            return False
        status: str = resp.json().get('status', '')
        if status in TERMINAL_BULK_STATUSES:
            logging.info(
                'Bulk job reached terminal status (poll fallback)',
                extra={'job_id': job_id, 'status': status},
            )
            return True
        sleep_for: float = min(poll_interval, remaining)
        await asyncio.sleep(sleep_for)


def _log_ws_closed_with_error(
    job_id: str,
    exc: 'websockets.exceptions.ConnectionClosedError',
) -> None:
    '''
    Log a WebSocket close at the right level for its close code.

    1012 (service_restart) and 1001 (going_away) are expected
    during gunicorn worker rotation / API redeploys; the scraper
    recovers by leaving files for retry on the next iteration, so
    these get INFO. Anything else is a real anomaly (network
    failure, server bug) and stays at WARNING.
    '''
    rcvd = exc.rcvd
    code: int | None = rcvd.code if rcvd else None
    reason: str | None = rcvd.reason if rcvd else None
    extra: dict[str, object] = {
        'job_id': job_id,
        'close_code': code,
        'close_reason': reason,
    }
    if code in (1001, 1012):
        logging.info(
            'Bulk progress WebSocket closed for server restart',
            extra=extra,
        )
    else:
        logging.warning(
            'Bulk progress WebSocket closed with error',
            exc=exc,
            extra=extra,
        )


async def stream_bulk_job_progress(
    job_id: str,
    exchange_url: str,
    client: ExchangeClient,
    timeout_seconds: float,
) -> bool:
    '''
    Subscribe to the bulk-upload progress WebSocket for *job_id*
    and wait for a terminal status (``completed`` or ``failed``)
    or until *timeout_seconds* elapses.

    Returns ``True`` on terminal status (including a clean
    server-initiated close, which the server uses to signal "job
    already complete"), and ``False`` on missing auth, an error
    close code (4401/4403/4404), any other connection failure,
    or timeout.

    Auth is forwarded via the ``Authorization`` header copied off
    *client*.
    '''
    auth_header: str | None = client.headers.get('Authorization')
    if not auth_header:
        logging.warning(
            'No Authorization header on ExchangeClient; cannot '
            'open bulk-progress WebSocket',
            extra={'job_id': job_id},
        )
        return False

    ws_url: str = bulk_progress_ws_url(exchange_url, job_id)
    deadline: float = (
        asyncio.get_running_loop().time() + timeout_seconds
    )

    logging.debug(
        'Connecting to bulk progress WebSocket',
        extra={
            'job_id': job_id,
            'ws_url': ws_url,
            'timeout_seconds': timeout_seconds,
        },
    )
    try:
        async with websockets.connect(
            ws_url,
            additional_headers=[('Authorization', auth_header)],
        ) as ws:
            logging.debug(
                'Bulk progress WebSocket connected',
                extra={'job_id': job_id},
            )
            while True:
                remaining: float = (
                    deadline - asyncio.get_running_loop().time()
                )
                if remaining <= 0:
                    logging.warning(
                        'Bulk progress WebSocket timed out',
                        extra={
                            'job_id': job_id,
                            'timeout_seconds': timeout_seconds,
                        },
                    )
                    return False
                try:
                    raw: str = await asyncio.wait_for(
                        ws.recv(), timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    logging.warning(
                        'Bulk progress WebSocket recv timed out',
                        extra={
                            'job_id': job_id,
                            'timeout_seconds': timeout_seconds,
                        },
                    )
                    return False

                try:
                    message: dict = orjson.loads(raw)
                except Exception as exc:
                    logging.warning(
                        'Bulk progress WebSocket sent non-JSON '
                        'message, ignoring',
                        exc=exc,
                        extra={'job_id': job_id},
                    )
                    continue

                status: str = message.get('status', '')
                logging.debug(
                    'Bulk progress WebSocket message',
                    extra={
                        'job_id': job_id,
                        'status': status,
                        'worker': message.get('worker'),
                        'message': message.get('message'),
                        'timestamp': message.get('timestamp'),
                    },
                )
                if status in TERMINAL_BULK_STATUSES:
                    logging.info(
                        'Bulk job reached terminal status',
                        extra={
                            'job_id': job_id, 'status': status,
                        },
                    )
                    return True
    except websockets.exceptions.ConnectionClosedOK:
        # Server closed cleanly (code 1000): "job already complete"
        # after replaying history. Treat as terminal so the caller
        # fetches results.
        logging.info(
            'Bulk progress WebSocket closed cleanly',
            extra={'job_id': job_id},
        )
        return True
    except websockets.exceptions.ConnectionClosedError as exc:
        return await _handle_ws_close_error(
            job_id, exc, exchange_url, client, deadline,
        )
    except Exception as exc:
        logging.warning(
            'Bulk progress WebSocket failed',
            exc=exc,
            extra={'job_id': job_id},
        )
        return False


async def fetch_bulk_results(
    job_id: str,
    exchange_url: str,
    client: ExchangeClient,
) -> list[dict]:
    '''
    Fetch per-record results for *job_id*. Returns an empty list
    on any error so the caller can leave source files in base_dir
    for the next iteration.
    '''
    results_url: str = (
        f'{exchange_url}{BULK_API_PATH}/{job_id}/results'
    )
    logging.debug(
        'Fetching bulk job results',
        extra={'job_id': job_id, 'results_url': results_url},
    )
    try:
        resp: Response = await client.get(results_url)
    except Exception as exc:
        logging.warning(
            'Bulk job results fetch failed',
            exc=exc,
            extra={'job_id': job_id},
        )
        return []
    if resp.status_code != 200:
        logging.warning(
            'Bulk job results response non-200',
            extra={
                'job_id': job_id,
                'status_code': resp.status_code,
                'response_text': resp.text,
            },
        )
        return []
    body: dict = resp.json()
    results: list[dict] = body.get('results', [])
    logging.debug(
        'Fetched bulk job results',
        extra={'job_id': job_id, 'results_count': len(results)},
    )
    return results


async def apply_bulk_results(
    batch_records: list[tuple[str, str]],
    results: list[dict],
    fm: AssetFileManagement,
    batch_id: str,
    job_id: str,
) -> tuple[int, int, int]:
    '''
    Reconcile per-record results against the source files we sent.
    Successful records are moved to ``uploaded_dir`` via
    :meth:`AssetFileManagement.mark_uploaded`. Failed and missing
    records are left in ``base_dir`` for the next iteration.

    *batch_records* is the ordered list of ``(content_id,
    filename)`` tuples in the same order they were appended to the
    submitted ``.jsonl``. The order is what enables the
    ``record_index`` fallback when a result entry lacks
    ``platform_content_id``.

    Returns ``(success, failed, missing)`` — counts the caller
    can turn into Prometheus metrics under whatever label scheme
    they prefer.
    '''
    by_id: dict[str, str] = dict(batch_records)
    by_index: dict[int, str] = {
        idx: fname for idx, (_, fname) in enumerate(batch_records)
    }
    logging.debug(
        'Reconciling bulk results',
        extra={
            'batch_id': batch_id,
            'job_id': job_id,
            'results_count': len(results),
            'records_sent': len(batch_records),
        },
    )

    seen: set[str] = set()
    success_count: int = 0
    failure_count: int = 0
    for entry in results:
        status: str = entry.get('status', '')
        cid: str | None = entry.get('platform_content_id')
        record_index: int | None = entry.get('record_index')
        filename: str | None = None
        if cid:
            filename = by_id.get(cid)
        if filename is None and record_index is not None:
            filename = by_index.get(record_index)
        if filename is None:
            logging.warning(
                'Bulk result has no matchable identifier',
                extra={'job_id': job_id, 'entry': entry},
            )
            continue
        seen.add(filename)
        if status == 'success':
            logging.debug(
                'Bulk record succeeded, marking uploaded',
                extra={
                    'filename': filename,
                    'job_id': job_id,
                    'platform_content_id': cid,
                },
            )
            try:
                await fm.mark_uploaded(filename)
                success_count += 1
            except OSError as exc:
                logging.warning(
                    'Bulk record succeeded but mark_uploaded '
                    'failed',
                    exc=exc,
                    extra={
                        'filename': filename,
                        'job_id': job_id,
                    },
                )
        else:
            failure_count += 1
            logging.info(
                'Bulk record failed, leaving file for retry',
                extra={
                    'filename': filename,
                    'job_id': job_id,
                    'reason': entry.get('reason'),
                },
            )

    missing_count: int = len(batch_records) - len(seen)
    logging.info(
        'Bulk batch reconciled',
        extra={
            'batch_id': batch_id,
            'job_id': job_id,
            'success': success_count,
            'failed': failure_count,
            'missing': missing_count,
            'total_sent': len(batch_records),
        },
    )
    return success_count, failure_count, missing_count


async def upload_bulk_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    *,
    schema_owner: str,
    schema_version: str,
    platform: str,
    entity: str,
    exchange_url: str,
    client: ExchangeClient,
    fm: AssetFileManagement,
    progress_timeout_seconds: float,
    filename_prefix: str,
) -> BulkBatchOutcome:
    '''
    Dispatch one prepared batch through the full bulk pipeline:
    POST → progress WebSocket → results fetch → mark_uploaded.

    *batch_buf* is the raw ``.jsonl`` bytes (one record per line).
    *batch_records* is the parallel ``(content_id, filename)``
    list in submission order; the helper does not look at
    *batch_buf*'s contents itself, so the caller is responsible
    for keeping the two in step.

    Returns a :class:`BulkBatchOutcome` describing the lifecycle
    outcome and per-record counts. The caller uses these to drive
    Prometheus metrics. Source files for non-success records are
    left in ``base_dir`` for retry on the next iteration.
    '''
    if not batch_records:
        return BulkBatchOutcome(
            status='completed', job_id=None,
            success=0, failed=0, missing=0,
        )

    batch_id: str = uuid4().hex[:8]
    upload_filename: str = f'{filename_prefix}-{batch_id}.jsonl'

    logging.info(
        'Uploading bulk batch',
        extra={
            'batch_id': batch_id,
            'platform': platform,
            'entity': entity,
            'records': len(batch_records),
            'bytes': len(batch_buf),
        },
    )

    bulk_url: str = f'{exchange_url}{BULK_API_PATH}'
    logging.debug(
        'POSTing bulk batch',
        extra={
            'batch_id': batch_id,
            'bulk_url': bulk_url,
            'upload_filename': upload_filename,
            'schema_owner': schema_owner,
            'schema_version': schema_version,
            'platform': platform,
            'entity': entity,
        },
    )
    # Bulk batches can be hundreds of MB (BULK_MAX_BATCH_BYTES is
    # commonly 200 MB in production). httpx's default 5 s write
    # timeout fires long before that finishes uploading on a
    # non-LAN link. Match the server's client_body_timeout (3600s
    # in scrape-api.conf) so the client and the server agree on
    # how long an upload may take -- only real network failure
    # ends the request, not a timeout mismatch.
    try:
        post_resp: Response = await client.post(
            bulk_url,
            data={
                'username': schema_owner,
                'platform': platform,
                'entity': entity,
                'version': schema_version,
            },
            files={
                'file': (
                    upload_filename, batch_buf,
                    'application/x-ndjson',
                ),
            },
            timeout=Timeout(
                connect=30.0, write=3600.0, read=3600.0, pool=60.0,
            ),
        )
    except Exception as exc:
        logging.warning(
            'Bulk batch POST failed',
            exc=exc,
            extra={
                'batch_id': batch_id,
                'records': len(batch_records),
            },
        )
        return BulkBatchOutcome(
            status='post_error', job_id=None,
            success=0, failed=0, missing=0,
        )

    if post_resp.status_code != 201:
        logging.warning(
            'Bulk batch upload rejected',
            extra={
                'batch_id': batch_id,
                'status_code': post_resp.status_code,
                'response_text': post_resp.text,
            },
        )
        return BulkBatchOutcome(
            status='post_rejected', job_id=None,
            success=0, failed=0, missing=0,
        )

    job_id: str = post_resp.json().get('job_id', '')
    logging.debug(
        'Bulk batch POST accepted',
        extra={
            'batch_id': batch_id,
            'status_code': post_resp.status_code,
            'job_id': job_id,
        },
    )
    if not job_id:
        logging.warning(
            'Bulk batch response missing job_id',
            extra={'batch_id': batch_id},
        )
        return BulkBatchOutcome(
            status='no_job_id', job_id=None,
            success=0, failed=0, missing=0,
        )

    if not await stream_bulk_job_progress(
        job_id, exchange_url, client, progress_timeout_seconds,
    ):
        logging.warning(
            'Bulk batch did not reach terminal status, leaving '
            'source files for retry',
            extra={
                'batch_id': batch_id, 'job_id': job_id,
            },
        )
        return BulkBatchOutcome(
            status='progress_failed', job_id=job_id,
            success=0, failed=0, missing=0,
        )

    results: list[dict] = await fetch_bulk_results(
        job_id, exchange_url, client,
    )
    success: int
    failed: int
    missing: int
    success, failed, missing = await apply_bulk_results(
        batch_records, results, fm, batch_id, job_id,
    )
    return BulkBatchOutcome(
        status='completed', job_id=job_id,
        success=success, failed=failed, missing=missing,
    )
