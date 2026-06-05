'''Per-phase connection-establishment metrics for proxied
HTTPS requests, shared across the RSS scraper (vanilla httpx
+ httpcore trace extension) and the YouTube HTML scrape
path (httpx_curl_cffi).

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
'''

import os
import sys
import time
from typing import Any, Awaitable, Callable

import httpx
from curl_cffi import CurlInfo
from prometheus_client import Counter, Histogram

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


# --- httpcore trace extension (RSS async + InnerTube sync) ---

AsyncTraceCallback = Callable[
    [str, dict[str, Any]], Awaitable[None],
]
SyncTraceCallback = Callable[[str, dict[str, Any]], None]


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
    '''Async trace callback for ``httpx.AsyncClient`` requests.
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
    ``httpx.Client`` sync API internally. Records with
    ``call_type='innertube'``. Same phase semantics as
    :func:`make_rss_phase_trace`.
    '''

    state: dict[str, float | None] = {
        'tcp_started_at': None,
        'tcp_completed_at': None,
        'tls_started_at': None,
    }

    def trace(name: str, info: dict[str, Any]) -> None:
        _record_phase_event(
            state, name,
            call_type='innertube',
            proxy_file=proxy_file,
        )

    return trace


def install_innertube_phase_tracing(
    session,
    proxy_file: str,
) -> None:
    '''Patch the InnerTube adaptor's ``httpx.Client`` session
    so every outgoing request carries a phase-tracing trace
    extension. Idempotent (sets a marker attribute).

    The innertube library does not expose a per-request hook
    for extensions, so the cleanest wire-in is to wrap
    ``session.send``. The wrapped ``send`` injects a freshly-
    built trace callback into each request before delegating
    to the original method, so each request gets its own
    closure with private state — safe under concurrent use.

    *session* is the InnerTube ``adaptor.session`` attribute
    (i.e. ``httpx.Client``).
    '''

    if getattr(session, '_phase_tracing_installed', False):
        return
    original_send = session.send

    def patched_send(request, **kwargs):
        if 'trace' not in request.extensions:
            request.extensions['trace'] = (
                make_innertube_phase_trace(
                    proxy_file=proxy_file,
                )
            )
        return original_send(request, **kwargs)

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
