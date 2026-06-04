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
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Callable
from uuid import uuid4

import aiofiles.os
import orjson
import websockets
from httpx import Response, Timeout

from .exchange_client import ExchangeClient
from .file_management import AssetFileManagement, atomic_write_bytes
from .scraper_metrics import METRIC_UPLOADS_SKIPPED
from .worker_id import get_worker_id

if TYPE_CHECKING:
    from .youtube.exchange_channels_set import (
        RedisExchangeChannelsSet,
    )


_LOGGER: logging.Logger = logging.getLogger(__name__)


def record_bulk_filter_skip(
    *,
    platform: str,
    scraper: str,
    entity: str,
    reason: str,
) -> None:
    '''
    Bump ``uploads_skipped_total`` for one record dropped by the
    bulk-sweep filter (the per-file pipeline that runs before
    each batch is built). Tagging by *reason* lets dashboards
    attribute the gap between files-on-disk and files-uploaded
    to a specific cause — e.g. ``superseded``, ``read_failed``,
    ``no_handle``, ``missing_video_id``, ``schema_invalid``,
    ``unrecognised_filename`` — so an operator can tell whether
    the backlog is recoverable, permanent, or just stale.

    Generic across scrapers: callers pass *platform*, *scraper*,
    *entity* explicitly so the same counter covers video, channel,
    and any future entity types.
    '''
    METRIC_UPLOADS_SKIPPED.labels(
        platform=platform,
        scraper=scraper,
        entity=entity,
        reason=reason,
        worker_id=get_worker_id(),
    ).inc()


BULK_API_PATH: str = '/api/v1/bulk'
TERMINAL_BULK_STATUSES: frozenset[str] = frozenset({
    'completed', 'failed',
})
# Subdirectory of ``base_dir`` where in-flight bulk uploads
# persist their state. Each accepted ``POST /api/v1/bulk``
# writes ``<base_dir>/.bulk/<job_id>.json`` so that a crashed
# scraper can ask the API on next startup whether each job
# eventually finished, fetch the results, and reconcile the
# source files instead of re-uploading the whole batch.
BULK_STATE_DIR_NAME: str = '.bulk'
# Wall-clock budget for the resume helper to wait for one
# pending job to reach terminal status before giving up and
# leaving the state file for the next startup attempt.
_RESUME_POLL_TIMEOUT_SECONDS: float = 300.0
# Cap on a single bulk-progress WebSocket ``recv`` wait. The server may
# send no progress frame for a long stretch on a big job; without a cap
# the recv would block past the liveness watchdog's work timeout and the
# upload tool would be killed mid-job. Loop back every interval to touch
# the watchdog; the overall job deadline still bounds the total wait.
# Must be < WATCHDOG_WORK_TIMEOUT_SECONDS.
_WS_RECV_TOUCH_INTERVAL: float = 30.0
_STALE_BULK_STATE_AGE: timedelta = timedelta(hours=24)
_SLOT_WAIT_SLEEP_SECONDS: float = 5.0
_SLOT_RESERVATIONS: dict[Path, int] = {}
_SLOT_CONDITION: asyncio.Condition = asyncio.Condition()


@dataclass
class BulkUploadState:
    '''
    Persistent record of an accepted ``POST /api/v1/bulk`` that
    has not yet been reconciled. Written to
    ``<base_dir>/.bulk/<job_id>.json`` immediately after the
    server returns ``job_id`` and removed once
    :func:`apply_bulk_results` has run, so a crashed scraper can
    resume mid-flight bulk jobs without re-uploading.
    '''
    job_id: str
    batch_id: str
    schema_owner: str
    schema_version: str
    platform: str
    entity: str
    upload_filename: str
    batch_records: list[tuple[str, str]] = field(
        default_factory=list,
    )
    created_at: str = ''

    def to_dict(self) -> dict:
        '''Serialise to a JSON-friendly dict.'''
        return {
            'job_id': self.job_id,
            'batch_id': self.batch_id,
            'schema_owner': self.schema_owner,
            'schema_version': self.schema_version,
            'platform': self.platform,
            'entity': self.entity,
            'upload_filename': self.upload_filename,
            'batch_records': [
                list(r) for r in self.batch_records
            ],
            'created_at': self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'BulkUploadState':
        '''Inverse of :meth:`to_dict`.'''
        return cls(
            job_id=data['job_id'],
            batch_id=data['batch_id'],
            schema_owner=data['schema_owner'],
            schema_version=data['schema_version'],
            platform=data['platform'],
            entity=data['entity'],
            upload_filename=data['upload_filename'],
            batch_records=[
                (r[0], r[1]) for r in data.get('batch_records', [])
            ],
            created_at=data.get('created_at', ''),
        )


def _bulk_state_dir(fm: AssetFileManagement) -> Path:
    return fm.base_dir / BULK_STATE_DIR_NAME


def _bulk_state_path(
    fm: AssetFileManagement, job_id: str,
) -> Path:
    return _bulk_state_dir(fm) / f'{job_id}.json'


def _touch_watchdog_work() -> None:
    '''Keep the liveness watchdog fresh during intentional waits.'''
    try:
        from scrape_exchange.watchdog import Watchdog
        Watchdog.get().touch_work()
    except Exception:
        pass


def _bulk_state_created_at(state: BulkUploadState) -> datetime | None:
    if not state.created_at:
        return None
    try:
        created_at: datetime = datetime.fromisoformat(
            state.created_at,
        )
    except ValueError:
        return None
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=UTC)
    return created_at.astimezone(UTC)


def _bulk_state_is_stale(
    state: BulkUploadState,
    *,
    now: datetime | None = None,
) -> bool:
    created_at: datetime | None = _bulk_state_created_at(state)
    if created_at is None:
        return False
    if now is None:
        now = datetime.now(UTC)
    return now - created_at > _STALE_BULK_STATE_AGE


async def write_bulk_state(
    fm: AssetFileManagement, state: BulkUploadState,
) -> None:
    '''
    Atomically persist *state* to ``<base_dir>/.bulk/<job_id>.json``.
    Best-effort: any OSError is logged at warning and swallowed —
    a missing state file just means the resume path won't be able
    to recover this particular job, which is the same outcome as
    today's "no persistence at all".
    '''
    try:
        state_dir: Path = _bulk_state_dir(fm)
        state_dir.mkdir(parents=True, exist_ok=True)
        path: Path = state_dir / f'{state.job_id}.json'
        payload: bytes = orjson.dumps(
            state.to_dict(), option=orjson.OPT_INDENT_2,
        )
        await atomic_write_bytes(path, payload)
    except OSError as exc:
        _LOGGER.warning(
            'Failed to write bulk-upload state file',
            exc=exc,
            extra={'job_id': state.job_id},
        )


async def delete_bulk_state(
    fm: AssetFileManagement, job_id: str,
) -> None:
    '''
    Best-effort delete of a bulk-upload state file. ``FileNotFoundError``
    is treated as a success (the file may have been cleaned up by
    a parallel worker that handled the same job).
    '''
    path: Path = _bulk_state_path(fm, job_id)
    try:
        await aiofiles.os.remove(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        _LOGGER.warning(
            'Failed to delete bulk-upload state file',
            exc=exc,
            extra={'job_id': job_id, 'path': str(path)},
        )


def list_bulk_states(
    fm: AssetFileManagement,
) -> list[BulkUploadState]:
    '''
    Load every persisted bulk-upload state from
    ``<base_dir>/.bulk``. Corrupt or unreadable files are
    deleted (so a malformed entry can't wedge resume on every
    startup) and skipped. Returns an empty list when the
    directory does not exist.
    '''
    state_dir: Path = _bulk_state_dir(fm)
    if not state_dir.is_dir():
        return []
    states: list[BulkUploadState] = []
    for entry in state_dir.iterdir():
        if not entry.is_file() or entry.suffix != '.json':
            continue
        try:
            data: dict = orjson.loads(entry.read_bytes())
            states.append(BulkUploadState.from_dict(data))
        except (
            OSError,
            orjson.JSONDecodeError,
            KeyError,
            TypeError,
        ) as exc:
            _LOGGER.warning(
                'Discarding unreadable bulk state file',
                exc=exc,
                extra={'path': str(entry)},
            )
            try:
                entry.unlink()
            except OSError:
                pass
    return states


async def resume_pending_bulk_uploads(
    fm: AssetFileManagement,
    client: ExchangeClient,
    exchange_url: str,
    poll_timeout_seconds: float = _RESUME_POLL_TIMEOUT_SECONDS,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> None:
    '''
    Read ``<base_dir>/.bulk/`` on startup and reconcile every
    persisted bulk job against the API:

    * The API returns 404 → the job was never queued (or has
      been cleaned up by retention). Delete the state file; the
      source files stay in ``base_dir`` to be re-uploaded by
      the next sweep.
    * The API returns a non-terminal status → poll until the
      job hits ``completed``/``failed`` (bounded by
      *poll_timeout_seconds*). On terminal: fetch results,
      apply them via :func:`apply_bulk_results`, delete the
      state file. On timeout: leave the state file for the next
      startup attempt.
    * The API returns a terminal status → fetch + apply +
      delete immediately.
    * Any transport / non-200 / non-404 response → log and
      leave the state file alone for retry next startup.

    Call this from each scraper's startup *after* the
    ``ExchangeClient`` is connected and *before* the live
    scrape / upload loop begins.
    '''
    states: list[BulkUploadState] = list_bulk_states(fm)
    if not states:
        return
    _LOGGER.info(
        'Resuming pending bulk uploads from state files',
        extra={'count': len(states)},
    )
    for state in states:
        _touch_watchdog_work()
        if _bulk_state_is_stale(state):
            _LOGGER.info(
                'Discarding stale bulk-upload state file',
                extra={
                    'job_id': state.job_id,
                    'created_at': state.created_at,
                    'stale_after_hours': (
                        _STALE_BULK_STATE_AGE.total_seconds() / 3600
                    ),
                },
            )
            await delete_bulk_state(fm, state.job_id)
            continue
        try:
            await _resume_one_bulk_state(
                fm, client, exchange_url, state,
                poll_timeout_seconds,
                exchange_set=exchange_set,
                id_from_filename=id_from_filename,
            )
        except Exception as exc:
            _LOGGER.warning(
                'Bulk-upload resume failed for job, '
                'leaving state for retry',
                exc=exc,
                extra={'job_id': state.job_id},
            )


async def wait_for_bulk_upload_slot(
    fm: AssetFileManagement,
    client: ExchangeClient,
    exchange_url: str,
    *,
    max_active_jobs: int,
    poll_timeout_seconds: float = _RESUME_POLL_TIMEOUT_SECONDS,
    sleep_seconds: float = _SLOT_WAIT_SLEEP_SECONDS,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> None:
    '''
    Block until this file manager has capacity to start another
    accepted bulk job.

    The durable ``.bulk`` state directory is the source of truth:
    a job counts as active after ``POST /api/v1/bulk`` returns a
    ``job_id`` and stops counting only after results are reconciled
    and the state file is deleted. This keeps callers from starting
    unbounded replacement jobs when a progress WebSocket fails while
    the server-side job is still running.
    '''
    max_active: int = max(max_active_jobs, 1)
    while True:
        state_dir: Path = _bulk_state_dir(fm).resolve()
        pending_count: int = (
            len(list_bulk_states(fm))
            + _SLOT_RESERVATIONS.get(state_dir, 0)
        )
        if pending_count < max_active:
            return

        _touch_watchdog_work()
        _LOGGER.info(
            'Waiting for bulk-upload slot',
            extra={
                'pending_jobs': pending_count,
                'max_active_jobs': max_active,
            },
        )
        await resume_pending_bulk_uploads(
            fm, client, exchange_url,
            poll_timeout_seconds=poll_timeout_seconds,
            exchange_set=exchange_set,
            id_from_filename=id_from_filename,
        )
        pending_count = (
            len(list_bulk_states(fm))
            + _SLOT_RESERVATIONS.get(state_dir, 0)
        )
        if pending_count < max_active:
            return
        _touch_watchdog_work()
        await asyncio.sleep(sleep_seconds)


@asynccontextmanager
async def reserve_bulk_upload_slot(
    fm: AssetFileManagement,
    client: ExchangeClient,
    exchange_url: str,
    *,
    max_active_jobs: int,
    poll_timeout_seconds: float = _RESUME_POLL_TIMEOUT_SECONDS,
    sleep_seconds: float = _SLOT_WAIT_SLEEP_SECONDS,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> AsyncIterator[None]:
    '''
    Reserve one bulk-upload slot for a caller that is about to
    POST a batch.

    ``wait_for_bulk_upload_slot`` handles durable accepted jobs;
    this context manager also accounts for local tasks that have
    passed the gate but have not yet received and persisted a
    ``job_id``. The reservation is held for the caller's whole
    batch lifecycle, so progress failures hand capacity back only
    if the durable state file continues to represent the still
    pending server-side job.
    '''
    max_active: int = max(max_active_jobs, 1)
    state_dir: Path = _bulk_state_dir(fm).resolve()
    reserved: bool = False
    try:
        while True:
            async with _SLOT_CONDITION:
                pending_count: int = (
                    len(list_bulk_states(fm))
                    + _SLOT_RESERVATIONS.get(state_dir, 0)
                )
                if pending_count < max_active:
                    _SLOT_RESERVATIONS[state_dir] = (
                        _SLOT_RESERVATIONS.get(state_dir, 0) + 1
                    )
                    reserved = True
                    break

            await wait_for_bulk_upload_slot(
                fm, client, exchange_url,
                max_active_jobs=max_active,
                poll_timeout_seconds=poll_timeout_seconds,
                sleep_seconds=sleep_seconds,
                exchange_set=exchange_set,
                id_from_filename=id_from_filename,
            )

        yield
    finally:
        if reserved:
            async with _SLOT_CONDITION:
                remaining: int = _SLOT_RESERVATIONS.get(
                    state_dir, 0,
                ) - 1
                if remaining > 0:
                    _SLOT_RESERVATIONS[state_dir] = remaining
                else:
                    _SLOT_RESERVATIONS.pop(state_dir, None)
                _SLOT_CONDITION.notify_all()


async def _resume_one_bulk_state(
    fm: AssetFileManagement,
    client: ExchangeClient,
    exchange_url: str,
    state: BulkUploadState,
    poll_timeout_seconds: float,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> None:
    '''
    Drive one persisted ``BulkUploadState`` through to a
    terminal verdict. See :func:`resume_pending_bulk_uploads`
    for the matrix of API responses and outcomes.
    '''
    status_url: str = f'{exchange_url}{BULK_API_PATH}'
    try:
        resp: Response = await client.get(
            status_url, params={'job_id': state.job_id},
        )
    except Exception as exc:
        _LOGGER.warning(
            'Bulk-upload status fetch failed during resume',
            exc=exc,
            extra={'job_id': state.job_id},
        )
        return
    if resp.status_code == 404:
        _LOGGER.info(
            'Bulk job not found on API, removing stale state',
            extra={'job_id': state.job_id},
        )
        await delete_bulk_state(fm, state.job_id)
        return
    if resp.status_code != 200:
        _LOGGER.warning(
            'Unexpected status from bulk-status endpoint',
            extra={
                'job_id': state.job_id,
                'status_code': resp.status_code,
            },
        )
        return
    body: dict = resp.json()
    job_status: str = body.get('status', '')
    if job_status not in TERMINAL_BULK_STATUSES:
        deadline: float = (
            asyncio.get_running_loop().time() + poll_timeout_seconds
        )
        if not await _poll_status_until_terminal(
            state.job_id, exchange_url, client, deadline,
        ):
            _LOGGER.info(
                'Bulk job still pending after resume poll, '
                'leaving state for next startup',
                extra={
                    'job_id': state.job_id,
                    'status': job_status,
                },
            )
            return
    results: 'BulkResults | None' = await fetch_bulk_results(
        state.job_id, exchange_url, client,
    )
    await apply_bulk_results(
        state.batch_records, results, fm,
        state.batch_id, state.job_id,
        exchange_set=exchange_set,
        id_from_filename=id_from_filename,
    )
    await delete_bulk_state(fm, state.job_id)
    _LOGGER.info(
        'Resumed bulk job reconciled',
        extra={
            'job_id': state.job_id,
            'batch_id': state.batch_id,
            'records': len(state.batch_records),
        },
    )


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
    success_ids: set[str] = field(default_factory=set)


@dataclass
class BulkResults:
    '''Parsed ``GET /api/v1/bulk/{job_id}/results`` payload.

    ``failures`` holds only the failed record outcomes (ADR-0008);
    successes and duplicates are implied by ``total - failed`` and
    are not listed.
    '''
    total: int
    succeeded: int
    failed: int
    duplicate: int
    failures: list[dict]


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
        _touch_watchdog_work()
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
        _touch_watchdog_work()
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
                _touch_watchdog_work()
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
                        ws.recv(),
                        timeout=min(
                            remaining, _WS_RECV_TOUCH_INTERVAL,
                        ),
                    )
                except asyncio.TimeoutError:
                    # No frame within this interval. The overall job
                    # deadline is enforced at the top of the loop; loop
                    # back to re-touch the watchdog and keep waiting so a
                    # quiet-but-healthy job is not mistaken for a hang.
                    continue

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
) -> 'BulkResults | None':
    '''
    Fetch aggregate counts + per-record failures for *job_id*.

    Returns ``None`` on any fetch error, a non-200 response, or when
    any of the four count fields (``total/succeeded/failed/
    duplicate``) is absent — an un-upgraded server that does not yet
    publish counts (ADR-0008 follow-up). ``None`` signals the caller
    to leave source files in ``base_dir`` for the next iteration.
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
        return None
    if resp.status_code != 200:
        logging.warning(
            'Bulk job results response non-200',
            extra={
                'job_id': job_id,
                'status_code': resp.status_code,
                'response_text': resp.text,
            },
        )
        return None
    body: dict = resp.json()
    required: tuple[str, ...] = (
        'total', 'succeeded', 'failed', 'duplicate',
    )
    if any(name not in body for name in required):
        logging.warning(
            'Bulk results missing one or more count fields; '
            'server not upgraded, leaving batch for retry',
            extra={'job_id': job_id, 'present_keys': sorted(body)},
        )
        return None
    results: BulkResults = BulkResults(
        total=int(body['total']),
        succeeded=int(body['succeeded']),
        failed=int(body['failed']),
        duplicate=int(body['duplicate']),
        failures=body.get('results', []),
    )
    logging.debug(
        'Fetched bulk job results',
        extra={
            'job_id': job_id,
            'total': results.total,
            'failed': results.failed,
            'failures_count': len(results.failures),
        },
    )
    return results


def _match_failures(
    failures: list[dict],
    by_id: dict[str, str],
    by_index: dict[int, str],
    job_id: str,
) -> tuple[set[str], bool]:
    '''Map each failure entry to a submitted filename by
    ``platform_content_id`` then ``record_index``. Returns the set of
    failed filenames and whether any entry was unmatchable (which
    blocks salvage, since an unidentifiable failure cannot be left for
    retry).'''
    failed_files: set[str] = set()
    unmatched: bool = False
    for entry in failures:
        cid: str | None = entry.get('platform_content_id')
        record_index: int | None = entry.get('record_index')
        filename: str | None = None
        if cid:
            filename = by_id.get(cid)
        if filename is None and record_index is not None:
            filename = by_index.get(record_index)
        if filename is None:
            logging.warning(
                'Bulk failure has no matchable identifier',
                extra={'job_id': job_id, 'entry': entry},
            )
            unmatched = True
            continue
        failed_files.add(filename)
    return failed_files, unmatched


async def apply_bulk_results(
    batch_records: list[tuple[str, str]],
    results: 'BulkResults | None',
    fm: AssetFileManagement,
    batch_id: str,
    job_id: str,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> tuple[int, int, int, set[str]]:
    '''
    Reconcile a failures-only results payload against the files we
    sent (ADR-0008).

    *results.failures* lists only failed records; successes and
    duplicates are implied. Each failure is matched to a source file
    by ``platform_content_id`` then ``record_index`` (the latter
    relies on *batch_records* being in the order they were appended
    to the submitted ``.jsonl``) and left in ``base_dir`` for retry.
    Every other submitted record is marked uploaded via
    :meth:`AssetFileManagement.mark_uploaded` — but only when the
    server's counts fully reconcile with what we sent:

    * ``results.total == len(batch_records)``;
    * ``succeeded + failed + duplicate == total`` (internal
      consistency);
    * ``len(results.failures) == results.failed`` and every failure
      matched a distinct submitted file.

    The completeness checks are the data-loss guard: a server that
    reports ``failed > 0`` with an empty/short failures list
    (mirroring lag/drop) or a sub-total ``total`` (job-level failure
    before all records were processed) would otherwise let an
    unprocessed or failed file be marked uploaded and deleted. When
    any check fails — or *results* is ``None`` (fetch error /
    un-upgraded server) — the whole batch is left for retry.

    When *exchange_set* and *id_from_filename* are supplied, the
    channel_ids derived from successful filenames are SADDed into the
    set in a single trailing batch.

    Returns ``(success, failed, missing, success_ids)`` where
    ``missing`` is the count of records left unreconciled (0 on the
    salvage path, ``len(batch_records)`` when the batch could not be
    reconciled).
    '''
    submitted: int = len(batch_records)
    by_id: dict[str, str] = dict(batch_records)
    by_index: dict[int, str] = {
        idx: fname for idx, (_, fname) in enumerate(batch_records)
    }

    if results is None:
        logging.warning(
            'Bulk results unavailable; leaving batch for retry',
            extra={'batch_id': batch_id, 'job_id': job_id,
                   'submitted': submitted},
        )
        return 0, 0, submitted, set()

    failed_files: set[str]
    unmatched: bool
    failed_files, unmatched = _match_failures(
        results.failures, by_id, by_index, job_id,
    )

    # Salvage requires the failures list to be COMPLETE, not just the
    # total to match: the data-loss guard depends on every failed
    # record appearing in the failures list.
    counts_consistent: bool = (
        results.succeeded + results.failed + results.duplicate
        == results.total
    )
    failures_complete: bool = (
        not unmatched
        and len(results.failures) == results.failed
        and len(failed_files) == results.failed
    )
    if (
        results.total != submitted
        or not counts_consistent
        or not failures_complete
    ):
        logging.warning(
            'Bulk batch counts do not reconcile; leaving for retry',
            extra={
                'batch_id': batch_id, 'job_id': job_id,
                'submitted': submitted, 'total': results.total,
                'succeeded': results.succeeded,
                'failed': results.failed,
                'duplicate': results.duplicate,
                'failure_entries': len(results.failures),
                'matched_failures': len(failed_files),
                'unmatched': unmatched,
            },
        )
        return 0, 0, submitted, set()

    success_count: int
    success_ids: set[str]
    success_count, success_ids = await _mark_salvaged(
        batch_records, failed_files, fm, job_id,
        exchange_set, id_from_filename,
    )
    failure_count: int = len(failed_files)

    logging.info(
        'Bulk batch reconciled',
        extra={
            'batch_id': batch_id,
            'job_id': job_id,
            'success': success_count,
            'failed': failure_count,
            'missing': 0,
            'total_sent': submitted,
        },
    )
    return success_count, failure_count, 0, success_ids


async def _mark_salvaged(
    batch_records: list[tuple[str, str]],
    failed_files: set[str],
    fm: AssetFileManagement,
    job_id: str,
    exchange_set: 'RedisExchangeChannelsSet | None',
    id_from_filename: Callable[[str], str] | None,
) -> tuple[int, set[str]]:
    '''Mark every submitted record not in *failed_files* uploaded
    (success or duplicate). Returns ``(success_count,
    success_ids)``. A ``mark_uploaded`` OSError leaves that one file
    for retry without aborting the batch.'''
    success_count: int = 0
    ids_to_add: set[str] = set()
    success_ids: set[str] = set()
    for content_id, filename in batch_records:
        if filename in failed_files:
            logging.info(
                'Bulk record failed, leaving file for retry',
                extra={'filename': filename, 'job_id': job_id},
            )
            continue
        try:
            await fm.mark_uploaded(filename)
        except OSError as exc:
            logging.warning(
                'Bulk record succeeded but mark_uploaded failed',
                exc=exc,
                extra={'filename': filename, 'job_id': job_id},
            )
            continue
        success_count += 1
        if content_id:
            success_ids.add(content_id)
        if exchange_set is not None and id_from_filename is not None:
            ids_to_add.add(id_from_filename(filename))

    if exchange_set is not None and ids_to_add:
        await exchange_set.add_many(ids_to_add)
    return success_count, success_ids


async def post_bulk_batch(
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
    filename_prefix: str,
) -> tuple[str, str, BulkBatchOutcome | None]:
    '''
    POST one prepared batch to the bulk endpoint, persist the
    crash-recovery state file, and return the job and batch ids.

    Returns ``(job_id, batch_id, None)`` when the server accepted
    the batch and the caller should proceed to
    :func:`finalize_bulk_batch`. Returns
    ``("", "", outcome)`` (with a terminal ``BulkBatchOutcome``)
    when the POST itself failed, the response was rejected, or no
    job_id was returned — the caller short-circuits and reports
    that outcome.

    *batch_buf* is held only for the duration of this call. After
    it returns the caller MUST drop its own reference (rebind to
    ``b''`` or equivalent), otherwise the batch bytes will stay
    in memory through the entire progress wait inside
    :func:`finalize_bulk_batch`.
    '''
    if not batch_records:
        return '', '', BulkBatchOutcome(
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
        return '', '', BulkBatchOutcome(
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
        return '', '', BulkBatchOutcome(
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
        return '', '', BulkBatchOutcome(
            status='no_job_id', job_id=None,
            success=0, failed=0, missing=0,
        )

    # Persist state immediately after the API accepts the upload.
    # If the scraper crashes between here and ``apply_bulk_results``
    # below, the next startup's ``resume_pending_bulk_uploads``
    # picks up the state file and reconciles the job from the API's
    # results endpoint instead of re-uploading the whole batch.
    await write_bulk_state(
        fm,
        BulkUploadState(
            job_id=job_id,
            batch_id=batch_id,
            schema_owner=schema_owner,
            schema_version=schema_version,
            platform=platform,
            entity=entity,
            upload_filename=upload_filename,
            batch_records=list(batch_records),
            created_at=datetime.now(UTC).isoformat(),
        ),
    )
    return job_id, batch_id, None


async def finalize_bulk_batch(
    job_id: str,
    batch_id: str,
    batch_records: list[tuple[str, str]],
    *,
    exchange_url: str,
    client: ExchangeClient,
    fm: AssetFileManagement,
    progress_timeout_seconds: float,
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> BulkBatchOutcome:
    '''
    Wait for the job's terminal status, fetch its per-record
    results, apply them to the on-disk source files, then delete
    the crash-recovery state file.

    Holds only *batch_records* and the small JSON results blob in
    memory — never the original batch bytes — so it is safe to
    block here for the full ``progress_timeout_seconds`` window
    while the operator's bytes have already been released.
    '''
    if not await stream_bulk_job_progress(
        job_id, exchange_url, client, progress_timeout_seconds,
    ):
        logging.warning(
            'Bulk batch did not reach terminal status, leaving '
            'source files for retry',
            extra={'job_id': job_id},
        )
        # Keep the state file: a future startup will resume the
        # job via the status endpoint and reconcile its results
        # rather than re-uploading the whole batch.
        return BulkBatchOutcome(
            status='progress_failed', job_id=job_id,
            success=0, failed=0, missing=0,
        )

    results: 'BulkResults | None' = await fetch_bulk_results(
        job_id, exchange_url, client,
    )
    success: int
    failed: int
    missing: int
    success_ids: set[str]
    success, failed, missing, success_ids = await apply_bulk_results(
        batch_records, results, fm, batch_id, job_id,
        exchange_set=exchange_set,
        id_from_filename=id_from_filename,
    )
    # Reconciliation done — drop the state file so resume on the
    # next startup doesn't re-process a job we've already handled.
    await delete_bulk_state(fm, job_id)
    return BulkBatchOutcome(
        status='completed', job_id=job_id,
        success=success, failed=failed, missing=missing,
        success_ids=success_ids,
    )


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
    exchange_set: 'RedisExchangeChannelsSet | None' = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> BulkBatchOutcome:
    '''
    Backwards-compatible wrapper: POST → progress → results →
    mark_uploaded as one call.

    New callers should invoke :func:`post_bulk_batch` and
    :func:`finalize_bulk_batch` separately and drop their
    ``batch_buf`` reference in between — that releases the batch
    bytes during the progress wait. This wrapper exists only so
    that the older one-call shape keeps working for tests and
    legacy callers; it cannot release ``batch_buf`` on the
    caller's behalf.
    '''
    job_id: str
    batch_id: str
    err: BulkBatchOutcome | None
    job_id, batch_id, err = await post_bulk_batch(
        batch_buf, batch_records,
        schema_owner=schema_owner,
        schema_version=schema_version,
        platform=platform,
        entity=entity,
        exchange_url=exchange_url,
        client=client,
        fm=fm,
        filename_prefix=filename_prefix,
    )
    # Release the wrapper's own reference — callers may still
    # hold theirs, in which case they should switch to the
    # two-phase API.
    batch_buf = b''  # noqa: F841
    if err is not None:
        return err
    return await finalize_bulk_batch(
        job_id, batch_id, batch_records,
        exchange_url=exchange_url,
        client=client,
        fm=fm,
        progress_timeout_seconds=progress_timeout_seconds,
        exchange_set=exchange_set,
        id_from_filename=id_from_filename,
    )
