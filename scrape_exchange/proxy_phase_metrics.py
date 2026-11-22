'''Per-phase connection-establishment metrics for proxied
HTTPS requests, shared across the RSS scraper and InnerTube
sessions (httpx2 + httpcore2 trace extension) and the YouTube
HTML scrape path (httpx_curl_cffi).

Both paths report into the same three histograms — keyed by
``call_type`` plus ``proxy_file`` / ``proxy_network`` — so a
single dashboard can compare TCP / CONNECT / TLS phase
timings across both clients on the same axes.

Phase model:

* ``proxy_tcp_connect_seconds``    — TCP handshake to the proxy.
* ``proxy_http_connect_seconds``   — HTTP CONNECT tunnel exchange
  (the proxy dialing the origin on our behalf). Only emitted for
  ``call_type='rss'``; curl_cffi reports a cumulative APPCONNECT
  time that bundles the CONNECT exchange and TLS handshake, so
  the HTML path cannot split them.
* ``proxy_tls_handshake_seconds``  — TLS handshake to YouTube.
  For ``call_type='html'`` this measurement includes the HTTP
  CONNECT exchange (since curl bundles them); for
  ``call_type='rss'`` it is the TLS handshake alone.

Both paths only sample on fresh connection establishment.
Keep-alive reuse skips the underlying trace / curl-info events,
so reused connections record nothing — by design.

Usage:

* RSS: build a per-fetch async trace callback with
  :func:`make_rss_phase_trace` and pass it via
  ``extensions={"trace": trace}`` on the ``http.get(...)`` call.
* InnerTube: build a per-fetch sync trace callback with
  :func:`make_innertube_phase_trace`. Wire it in by patching
  the InnerTube adaptor's session ``send`` (see
  :func:`install_innertube_phase_tracing`).
* HTML: list :data:`HTML_CURL_INFOS` as the transport's
  ``curl_infos=[...]`` argument so curl populates the timing
  dict, then call :func:`record_html_phase_metrics` on every
  successful response.

Response transfer metrics (``api_response_bytes``,
``api_response_wait_seconds``, ``api_response_body_seconds``)
are recorded for every InnerTube request (by the same
``session.send`` wrapper) and for every request through
:class:`TransferMetricsCurlTransport`. Throughput per proxy
file is ``rate(api_response_bytes_sum) /
rate(api_response_body_seconds_sum)``.
'''

import os
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

# Stays on httpx (0.23): record_html_phase_metrics reads responses from
# AsyncYouTubeClient, whose httpx_curl_cffi transport is built on it.
import httpx
from curl_cffi import CurlInfo
from httpx_curl_cffi import AsyncCurlTransport
from prometheus_client import Counter, Histogram

from .proxy_loader import proxy_file_label

# Mirror youtube_client._SCRAPER_BY_SCRIPT so all metrics
# carry a consistent ``scraper`` label without each call
# site having to know it. Resolved once at module import.
_SCRAPER_BY_SCRIPT: dict[str, str] = {
    'yt_video_scrape.py': 'video_scraper',
    'yt_channel_scrape.py': 'channel_scraper',
    'yt_rss_scrape.py': 'rss_scraper',
    'yt_discover_channels.py': 'discover',
}
_SCRIPT_NAME: str = (
    os.path.basename(sys.argv[0]) if sys.argv else 'unknown'
)
_SCRAPER_LABEL: str = _SCRAPER_BY_SCRIPT.get(
    _SCRIPT_NAME, 'unknown',
)

# Bucket layout spans sub-100ms (LAN-microsecond VPN
# proxies) through 30s (the 20s curl outlier plus headroom).
_PHASE_BUCKETS: tuple[float, ...] = (
    0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
    10.0, 20.0, 30.0,
)

METRIC_PROXY_TCP_CONNECT_SECONDS: Histogram = Histogram(
    'proxy_tcp_connect_seconds',
    'TCP handshake time to the proxy on YouTube fetches',
    labelnames=(
        'scraper', 'call_type', 'proxy_file',
    ),
    buckets=_PHASE_BUCKETS,
)
METRIC_PROXY_HTTP_CONNECT_SECONDS: Histogram = Histogram(
    'proxy_http_connect_seconds',
    'HTTP CONNECT tunnel establishment time. Only emitted '
    'for call_type=rss or innertube; the curl_cffi '
    'transport reports a cumulative APPCONNECT time that '
    'bundles this phase into proxy_tls_handshake_seconds '
    'for call_type=html.',
    labelnames=(
        'scraper', 'call_type', 'proxy_file',
    ),
    buckets=_PHASE_BUCKETS,
)
METRIC_PROXY_TLS_HANDSHAKE_SECONDS: Histogram = Histogram(
    'proxy_tls_handshake_seconds',
    'TLS handshake time to YouTube via tunnel. For '
    'call_type=html this bundles the HTTP CONNECT exchange '
    'too (curl reports APPCONNECT cumulatively); for '
    'call_type=rss or innertube it is the TLS handshake '
    'alone.',
    labelnames=(
        'scraper', 'call_type', 'proxy_file',
    ),
    buckets=_PHASE_BUCKETS,
)
METRIC_PROXY_REQUESTS: Counter = Counter(
    'proxy_requests_total',
    'Number of requests issued through proxy pool, '
    'labelled by whether the underlying TCP connection was '
    'newly established (reused="false") or pulled from the '
    'keep-alive pool (reused="true"). The ratio gives the '
    'keep-alive reuse rate: a high reused/total fraction '
    'means the TLS handshake cost amortises across many '
    'requests; a low fraction means every fetch pays the '
    'full handshake cost.',
    labelnames=(
        'scraper', 'call_type', 'proxy_file', 'reused',
    ),
)

# --- Response transfer: size, wait and body time ---------------

_TRANSFER_LABELS: tuple[str, ...] = (
    'platform', 'scraper', 'call', 'proxy_file',
)
_BYTES_BUCKETS: tuple[float, ...] = (
    1_000, 4_000, 16_000, 64_000, 256_000,
    1_000_000, 4_000_000, 16_000_000,
)
_TRANSFER_BUCKETS: tuple[float, ...] = (
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 30.0, 60.0,
)

METRIC_API_RESPONSE_BYTES: Histogram = Histogram(
    'api_response_bytes',
    'Response body size as received over the network, before '
    'decompression. Divide rate(api_response_bytes_sum) by '
    'rate(api_response_body_seconds_sum) for throughput.',
    labelnames=_TRANSFER_LABELS,
    buckets=_BYTES_BUCKETS,
)
METRIC_API_RESPONSE_WAIT_SECONDS: Histogram = Histogram(
    'api_response_wait_seconds',
    'Time from sending the request to receiving the response '
    'headers: server processing plus one round trip.',
    labelnames=_TRANSFER_LABELS,
    buckets=_TRANSFER_BUCKETS,
)
METRIC_API_RESPONSE_BODY_SECONDS: Histogram = Histogram(
    'api_response_body_seconds',
    'Time from receiving the response headers to receiving '
    'the last byte of the body.',
    labelnames=_TRANSFER_LABELS,
    buckets=_TRANSFER_BUCKETS,
)


def record_response_transfer(
    *,
    platform: str,
    scraper: str,
    call: str,
    proxy_file: str,
    num_bytes: int,
    wait_seconds: float,
    body_seconds: float,
) -> None:
    '''Observe one response on the three transfer histograms.
    Records nothing when a value is negative, so the bytes and
    body-time sums always cover the same responses.'''

    if num_bytes < 0 or wait_seconds < 0 or body_seconds < 0:
        return
    labels: dict[str, str] = {
        'platform': platform,
        'scraper': scraper,
        'call': call,
        'proxy_file': proxy_file,
    }
    METRIC_API_RESPONSE_BYTES.labels(**labels).observe(num_bytes)
    METRIC_API_RESPONSE_WAIT_SECONDS.labels(**labels).observe(
        wait_seconds,
    )
    METRIC_API_RESPONSE_BODY_SECONDS.labels(**labels).observe(
        body_seconds,
    )


# --- httpcore trace extension (RSS async + InnerTube sync) ---

AsyncTraceCallback = Callable[
    [str, dict[str, Any]], Awaitable[None],
]
SyncTraceCallback = Callable[[str, dict[str, Any]], None]

# Trace events that mark the transfer phases, and the state key
# each one sets. A proxied request also fires these for the
# CONNECT exchange; the real request's events come later and
# overwrite them.
_TRANSFER_EVENTS: tuple[tuple[str, str], ...] = (
    ('send_request_body.complete', 'request_sent_at'),
    ('receive_response_headers.complete', 'headers_at'),
    ('receive_response_body.complete', 'body_done_at'),
)


def _record_transfer_event(
    state: dict[str, float | None], name: str, now: float,
) -> bool:
    '''Store the time of a transfer-phase event in *state*.
    Returns True when *name* was one of them.'''

    for event, key in _TRANSFER_EVENTS:
        if event in name:
            state[key] = now
            return True
    return False


def _record_phase_event(
    state: dict[str, float | None],
    name: str,
    *,
    call_type: str,
    proxy_file: str,
) -> None:
    '''Shared per-event recorder used by both the async (RSS)
    and sync (InnerTube) trace callbacks. Observes the
    matching histogram on each phase-completion event.
    '''
    now: float = time.perf_counter()
    if 'connect_tcp.started' in name:
        state['tcp_started_at'] = now
        return
    if 'connect_tcp.complete' in name:
        t_start: float | None = state['tcp_started_at']
        if t_start is not None:
            METRIC_PROXY_TCP_CONNECT_SECONDS.labels(
                scraper=_SCRAPER_LABEL,
                call_type=call_type,
                proxy_file=proxy_file,
            ).observe(now - t_start)
        state['tcp_completed_at'] = now
        return
    if 'start_tls.started' in name:
        t_tcp_end: float | None = state['tcp_completed_at']
        if t_tcp_end is not None:
            METRIC_PROXY_HTTP_CONNECT_SECONDS.labels(
                scraper=_SCRAPER_LABEL,
                call_type=call_type,
                proxy_file=proxy_file,
            ).observe(now - t_tcp_end)
        state['tls_started_at'] = now
        return
    if 'start_tls.complete' in name:
        t_tls_start: float | None = state['tls_started_at']
        if t_tls_start is not None:
            METRIC_PROXY_TLS_HANDSHAKE_SECONDS.labels(
                scraper=_SCRAPER_LABEL,
                call_type=call_type,
                proxy_file=proxy_file,
            ).observe(now - t_tls_start)
            state['tls_started_at'] = None
        # Mark the application channel as ready. Subsequent
        # send_request_headers.started events from this trace
        # belong to the actual HTTPS request, not the proxy
        # CONNECT exchange that fires before start_tls.
        state['tls_complete'] = True
        return
    if 'send_request_headers.started' in name:
        # Three cases:
        #   - Reused connection: no connect_tcp events fired at
        #     all → tcp_started_at is None. Count as reused.
        #   - Fresh connection past TLS handshake: tls_complete
        #     is True. Count as fresh.
        #   - Fresh connection mid-CONNECT-exchange: HTTPS-
        #     through-HTTP-proxy fires this event for the
        #     CONNECT request itself, before start_tls. Skip
        #     to avoid double-counting fresh requests.
        if state['tcp_started_at'] is None:
            reused: str = 'true'
        elif state.get('tls_complete'):
            reused = 'false'
        else:
            return
        METRIC_PROXY_REQUESTS.labels(
            scraper=_SCRAPER_LABEL,
            call_type=call_type,
            proxy_file=proxy_file,
            reused=reused,
        ).inc()


def make_rss_phase_trace(
    proxy_file: str,
) -> AsyncTraceCallback:
    '''Async trace callback for ``httpx2.AsyncClient`` requests.
    Pass via ``extensions={"trace": trace}`` on the
    ``http.get(...)`` call. Records with ``call_type='rss'``.

    Keep-alive reuse skips ``connect_tcp`` / ``start_tls``
    events, so reused connections record no samples.
    '''

    state: dict[str, float | None] = {
        'tcp_started_at': None,
        'tcp_completed_at': None,
        'tls_started_at': None,
    }

    async def trace(
        name: str, info: dict[str, Any],
    ) -> None:
        _record_phase_event(
            state, name,
            call_type='rss',
            proxy_file=proxy_file,
        )

    return trace


def make_innertube_phase_trace(
    proxy_file: str,
) -> SyncTraceCallback:
    '''Sync trace callback for the third-party
    ``innertube.InnerTube`` library, which uses the
    ``httpx2.Client`` session we give it. Records with
    ``call_type='innertube'``. Same phase semantics as
    :func:`make_rss_phase_trace`.
    '''

    return _make_innertube_trace(_new_trace_state(), proxy_file)


def _new_trace_state() -> dict[str, float | None]:
    return {
        'tcp_started_at': None,
        'tcp_completed_at': None,
        'tls_started_at': None,
    }


def _make_innertube_trace(
    state: dict[str, float | None], proxy_file: str,
) -> SyncTraceCallback:
    '''InnerTube trace callback that records the connection
    phases and keeps the transfer timestamps in *state*.'''

    def trace(name: str, info: dict[str, Any]) -> None:
        if _record_transfer_event(state, name, time.perf_counter()):
            return
        _record_phase_event(
            state, name,
            call_type='innertube',
            proxy_file=proxy_file,
        )

    return trace


def _innertube_call(request: Any) -> str:
    '''The InnerTube endpoint name, from the last segment of the
    request path: ``/youtubei/v1/player`` gives ``player``.'''

    path: str = str(getattr(request.url, 'path', '') or '')
    return path.rstrip('/').rsplit('/', 1)[-1] or 'unknown'


def _record_innertube_transfer(
    request: Any,
    response: Any,
    state: dict[str, float | None],
    proxy_file: str,
) -> None:
    '''Observe the transfer metrics for one InnerTube response.
    No-op when the body was not read (a streamed response) or
    the trace events are missing.'''

    sent: float | None = state.get('request_sent_at')
    headers: float | None = state.get('headers_at')
    done: float | None = state.get('body_done_at')
    if sent is None or headers is None or done is None:
        return
    if not sent <= headers <= done:
        return
    num_bytes: Any = getattr(response, 'num_bytes_downloaded', None)
    if not isinstance(num_bytes, int):
        return
    record_response_transfer(
        platform='youtube',
        scraper=_SCRAPER_LABEL,
        call=_innertube_call(request),
        proxy_file=proxy_file,
        num_bytes=num_bytes,
        wait_seconds=headers - sent,
        body_seconds=done - headers,
    )


def install_innertube_phase_tracing(
    session,
    proxy_file: str,
) -> None:
    '''Patch the InnerTube adaptor's ``httpx2.Client`` session
    so every outgoing request carries a phase-tracing trace
    extension. Idempotent (sets a marker attribute).

    The innertube library does not expose a per-request hook
    for extensions, so the cleanest wire-in is to wrap
    ``session.send``. The wrapped ``send`` injects a freshly-
    built trace callback into each request before delegating
    to the original method, so each request gets its own
    closure with private state — safe under concurrent use.
    After the original ``send`` returns, the response transfer
    metrics are recorded from that state.

    *session* is the InnerTube ``adaptor.session`` attribute
    (i.e. ``httpx2.Client``).
    '''

    if getattr(session, '_phase_tracing_installed', False):
        return
    original_send = session.send

    def patched_send(request, **kwargs):
        state: dict[str, float | None] | None = None
        if 'trace' not in request.extensions:
            state = _new_trace_state()
            request.extensions['trace'] = _make_innertube_trace(
                state, proxy_file,
            )
        response = original_send(request, **kwargs)
        if state is not None:
            _record_innertube_transfer(
                request, response, state, proxy_file,
            )
        return response

    session.send = patched_send  # type: ignore[method-assign]
    session._phase_tracing_installed = True


# --- HTML path: curl_cffi info -----------------------------

HTML_CURL_INFOS: tuple[CurlInfo, ...] = (
    CurlInfo.NAMELOOKUP_TIME,
    CurlInfo.CONNECT_TIME,
    CurlInfo.APPCONNECT_TIME,
)

_NAMELOOKUP_KEY: int = int(CurlInfo.NAMELOOKUP_TIME)
_CONNECT_KEY: int = int(CurlInfo.CONNECT_TIME)
_APPCONNECT_KEY: int = int(CurlInfo.APPCONNECT_TIME)


def record_html_phase_metrics(
    response: httpx.Response,
    proxy_file: str,
) -> None:
    '''Read curl phase timings from *response* and observe
    them on the proxy phase histograms with
    ``call_type='html'``.

    No-op when the response carries no curl info (non-
    ``AsyncCurlTransport`` response or a mock in tests) or
    when the connection was reused from the keep-alive pool
    (curl reports 0 for the connect-related fields on
    reuse).
    '''

    extensions: dict = (
        getattr(response, 'extensions', None) or {}
    )
    infos: dict[int, float] = (
        extensions.get('curl', {}).get('infos', {})
    )
    if not infos:
        return
    namelookup: float = infos.get(_NAMELOOKUP_KEY, 0.0)
    connect: float = infos.get(_CONNECT_KEY, 0.0)
    appconnect: float = infos.get(_APPCONNECT_KEY, 0.0)

    if connect > 0.0 and connect >= namelookup:
        METRIC_PROXY_TCP_CONNECT_SECONDS.labels(
            scraper=_SCRAPER_LABEL,
            call_type='html',
            proxy_file=proxy_file,
        ).observe(connect - namelookup)
    if appconnect > 0.0 and appconnect >= connect:
        METRIC_PROXY_TLS_HANDSHAKE_SECONDS.labels(
            scraper=_SCRAPER_LABEL,
            call_type='html',
            proxy_file=proxy_file,
        ).observe(appconnect - connect)
    # Count every successful response as one request. curl
    # reports CONNECT_TIME == 0 for connections reused from
    # its internal keep-alive pool.
    reused_html: str = 'false' if connect > 0.0 else 'true'
    METRIC_PROXY_REQUESTS.labels(
        scraper=_SCRAPER_LABEL,
        call_type='html',
        proxy_file=proxy_file,
        reused=reused_html,
    ).inc()


# --- curl_cffi transport with response transfer metrics ------

_SIZE_DOWNLOAD_KEY: int = int(CurlInfo.SIZE_DOWNLOAD_T)
_PRETRANSFER_KEY: int = int(CurlInfo.PRETRANSFER_TIME)
_STARTTRANSFER_KEY: int = int(CurlInfo.STARTTRANSFER_TIME)
_TOTAL_KEY: int = int(CurlInfo.TOTAL_TIME)
_TRANSFER_CURL_INFOS: tuple[CurlInfo, ...] = (
    CurlInfo.SIZE_DOWNLOAD_T,
    CurlInfo.PRETRANSFER_TIME,
    CurlInfo.STARTTRANSFER_TIME,
    CurlInfo.TOTAL_TIME,
)


def _curl_transfer_values(
    response: httpx.Response,
) -> tuple[int, float, float] | None:
    '''Return ``(bytes, wait_seconds, body_seconds)`` for a curl
    response whose body has been read, or None without curl info.

    In stream mode curl captures its infos when the headers
    arrive, so the final size and total time are read from the
    curl handle, which still belongs to this response until it
    is closed. Without a handle (stream mode off) the captured
    infos are already final.
    '''

    curl_ext: dict = (
        (getattr(response, 'extensions', None) or {}).get('curl')
        or {}
    )
    handle: Any = getattr(curl_ext.get('response'), 'curl', None)
    if handle is not None:
        values: dict[int, Any] = {
            int(info): handle.getinfo(info)
            for info in _TRANSFER_CURL_INFOS
        }
    else:
        values = {
            int(key): value
            for key, value in (curl_ext.get('infos') or {}).items()
        }
    try:
        num_bytes: int = int(values[_SIZE_DOWNLOAD_KEY])
        pretransfer: float = float(values[_PRETRANSFER_KEY])
        starttransfer: float = float(values[_STARTTRANSFER_KEY])
        total: float = float(values[_TOTAL_KEY])
    except (KeyError, TypeError, ValueError):
        return None
    return num_bytes, starttransfer - pretransfer, total - starttransfer


class _TransferMetricsStream(httpx.AsyncByteStream):
    '''Response body stream that calls *on_complete* once the
    last chunk has been read, before the stream is closed.'''

    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        response: httpx.Response,
        on_complete: Callable[[httpx.Response], None],
    ) -> None:
        self._inner: httpx.AsyncByteStream = inner
        self._response: httpx.Response = response
        self._on_complete: Callable[[httpx.Response], None] = (
            on_complete
        )

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._inner:
            yield chunk
        self._on_complete(self._response)

    async def aclose(self) -> None:
        await self._inner.aclose()


class TransferMetricsCurlTransport(AsyncCurlTransport):
    '''``AsyncCurlTransport`` that records the response transfer
    metrics (bytes, wait time, body time) for every response
    whose body is read in full. The ``proxy_file`` label is
    looked up from *proxy* when each response completes.'''

    def __init__(
        self,
        *,
        platform: str,
        scraper: str,
        call: str,
        proxy: str | None = None,
        curl_infos: list[CurlInfo] | None = None,
        **kwargs: Any,
    ) -> None:
        infos: list[CurlInfo] = list(curl_infos or [])
        for info in _TRANSFER_CURL_INFOS:
            if info not in infos:
                infos.append(info)
        super().__init__(proxy=proxy, curl_infos=infos, **kwargs)
        self._platform: str = platform
        self._scraper: str = scraper
        self._call: str = call
        self._proxy_entry: str = proxy or ''

    async def handle_async_request(
        self, request: httpx.Request,
    ) -> httpx.Response:
        response: httpx.Response = (
            await super().handle_async_request(request)
        )
        response.stream = _TransferMetricsStream(
            response.stream, response, self._record,
        )
        return response

    def _record(self, response: httpx.Response) -> None:
        values: tuple[int, float, float] | None = (
            _curl_transfer_values(response)
        )
        if values is None:
            return
        num_bytes, wait_seconds, body_seconds = values
        record_response_transfer(
            platform=self._platform,
            scraper=self._scraper,
            call=self._call,
            proxy_file=proxy_file_label(self._proxy_entry),
            num_bytes=num_bytes,
            wait_seconds=wait_seconds,
            body_seconds=body_seconds,
        )
