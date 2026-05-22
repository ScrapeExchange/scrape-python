"""
Proxy-list loader.

Reads proxy/egress entries from one or more files, normalizes each
entry to a canonical form, deduplicates, and exposes a module-level
lookup so metric-emission sites can label each entry with the file
it came from.

Entry shapes (Section 1 of the design spec):

  http://user:pass@host:port  - canonical URL
  http://host:port            - URL without auth
  http://host:port:user:pass  - provider 4-colon form (canonicalized)
  local://<ipv4>              - native source-IP egress

Lines that do not start with http://, https://, or local:// have
http:// prepended before parsing, so a bare ``host:port`` or
``host:port:user:pass`` line is accepted as if it were written
with the http:// scheme.
"""

import asyncio
import ipaddress
import logging
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import httpx


_ALLOWED_URL_SCHEMES: Final[tuple[str, ...]] = ('http://', 'https://')
_LOCAL_SCHEME: Final[str] = 'local://'

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


def _parse_entry(raw: str) -> str:
    """Canonicalize a single entry. Raises ValueError on any
    malformed input. Caller is responsible for stripping
    whitespace/comments."""

    if raw.startswith(_LOCAL_SCHEME):
        ip_str: str = raw[len(_LOCAL_SCHEME):]
        try:
            ip: ipaddress.IPv4Address = ipaddress.IPv4Address(ip_str)
        except (ipaddress.AddressValueError, ValueError) as exc:
            raise ValueError(
                f'{_LOCAL_SCHEME} entry must be a valid IPv4 '
                f'address: {raw!r}'
            ) from exc
        if (
            ip.is_unspecified
            or ip.is_multicast
            or ip == ipaddress.IPv4Address('255.255.255.255')
        ):
            raise ValueError(
                f'{_LOCAL_SCHEME} entry must be a usable host '
                f'IPv4: {raw!r}'
            )
        return f'{_LOCAL_SCHEME}{ip}'

    # Auto-prepend http:// when the line has no recognised scheme,
    # so a bare host:port or host:port:user:pass line is accepted.
    if not any(raw.startswith(s) for s in _ALLOWED_URL_SCHEMES):
        raw = f'http://{raw}'

    scheme: str = next(
        s for s in _ALLOWED_URL_SCHEMES if raw.startswith(s)
    )
    body: str = raw[len(scheme):]

    # Reject paths, query strings, and fragments — proxy entries
    # must be bare host:port (with optional auth), not full URLs.
    if any(c in body for c in ('/', '?', '#')):
        raise ValueError(
            f'paths/queries/fragments not allowed in proxy '
            f'entries: {raw!r}'
        )

    # Already-canonical URL with auth (contains '@') is left as-is.
    if '@' in body:
        host_part: str = body.split('@', 1)[1]
        if ':' not in host_part or not host_part.split(':', 1)[1]:
            raise ValueError(f'URL missing port: {raw!r}')
        port: str = host_part.split(':', 1)[1]
        _validate_port(port, raw)
        return raw

    # No '@' means either:
    #   host:port              (URL without auth)
    #   host:port:user:pass    (provider 4-colon form)
    parts: list[str] = body.split(':', 3)
    if len(parts) == 2:
        # URL without auth
        host: str
        host, port = parts
        if not host or not port:
            raise ValueError(f'URL missing host or port: {raw!r}')
        _validate_port(port, raw)
        return raw
    if len(parts) == 4:
        user: str
        password: str
        host, port, user, password = parts
        if not (host and port and user and password):
            raise ValueError(
                f'4-colon form has empty part: {raw!r}'
            )
        _validate_port(port, raw)
        return f'{scheme}{user}:{password}@{host}:{port}'
    raise ValueError(f'cannot parse entry: {raw!r}')


@dataclass(frozen=True)
class ProxyCatalog:
    """Canonical, deduplicated proxy entries plus the file each came
    from. Source maps each canonical entry string to its file label
    (Path.stem of the source file)."""

    entries: list[str] = field(default_factory=list)
    source: dict[str, str] = field(default_factory=dict)


def _parse_file(path: Path) -> list[tuple[int, str]]:
    """Read one file, return [(line_number, raw_entry)] for
    non-blank, non-comment lines. Hard-fails on missing or empty
    file."""

    if not path.exists():
        raise FileNotFoundError(f'proxy file not found: {path}')
    raw: str = path.read_text(encoding='utf-8')
    rows: list[tuple[int, str]] = []
    for n, line in enumerate(raw.splitlines(), start=1):
        stripped: str = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        rows.append((n, stripped))
    if not rows:
        raise ValueError(
            f'proxy file {path} is empty (no usable entries)'
        )
    return rows


def load_proxy_catalog(paths: Sequence[Path]) -> ProxyCatalog:
    """Read each file, parse and normalize each line, dedupe, return
    the catalog. Hard-fails on missing/empty/malformed files. An
    empty paths list returns an empty catalog and logs a warning
    (matching the existing "no proxies configured" state)."""

    if not paths:
        _LOGGER.warning(
            'PROXY_FILES is unset/empty; no proxies configured.'
        )
        return ProxyCatalog()

    # ``~`` paths are common in operator CLI invocations
    # (``--proxy-files ~/proxies/a,~/proxies/b``). Bash only
    # tilde-expands the first comma-separated entry, so the rest
    # arrive here with literal ``~/``. Expand once at the boundary
    # so every downstream operation (existence check, stem dedupe,
    # error messages) works on resolved paths.
    expanded: list[Path] = [Path(p).expanduser() for p in paths]

    seen_stems: dict[str, Path] = {}
    for path in expanded:
        stem: str = path.stem
        if stem in seen_stems:
            raise ValueError(
                f'PROXY_FILES has two files with same stem '
                f'{stem!r}: {seen_stems[stem]} and {path}'
            )
        seen_stems[stem] = path

    entries: list[str] = []
    source: dict[str, str] = {}
    dup_count: int = 0

    for path in expanded:
        label: str = path.stem
        for line_no, raw in _parse_file(path):
            try:
                canonical: str = _parse_entry(raw)
            except ValueError as exc:
                raise ValueError(
                    f'{path}:{line_no}: {raw} ({exc})'
                ) from exc
            if canonical in source:
                _LOGGER.warning(
                    'proxy_loader: %s already loaded from %s; '
                    'skipping duplicate in %s',
                    canonical, source[canonical], label,
                )
                dup_count += 1
                continue
            entries.append(canonical)
            source[canonical] = label

    if dup_count:
        _LOGGER.info(
            'proxy_loader: dropped %d duplicate entries across files',
            dup_count,
        )
    return ProxyCatalog(entries=entries, source=source)


def _validate_port(port: str, raw: str) -> None:
    """Raise ValueError if *port* is not a valid TCP port number."""
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        raise ValueError(
            f'port must be a number between 1 and 65535: {raw!r}'
        )


_ACTIVE_CATALOG: ProxyCatalog = ProxyCatalog()


def set_active_catalog(catalog: ProxyCatalog) -> None:
    """Register the active catalog for module-level lookups. Each
    process should call this exactly once at startup (settings
    initialization does this automatically)."""

    global _ACTIVE_CATALOG
    _ACTIVE_CATALOG = catalog


def proxy_file_label(entry: str) -> str:
    """Return the proxy_file label for an entry, or 'none' if the
    entry isn't in the active catalog."""

    return _ACTIVE_CATALOG.source.get(entry, 'none')


def httpx_client_for_entry(
    entry: str | None, **kwargs: Any,
) -> httpx.AsyncClient:
    """Build an AsyncClient that egresses via the given entry.

    A proxy-URL entry is passed as ``proxies=`` (existing behavior).
    A ``local://<ipv4>`` entry is realized as an
    ``httpx.AsyncHTTPTransport(local_address=ip)``.
    ``None`` returns a vanilla AsyncClient (no proxy, default
    routing) so callers can hand the rate limiter's ``str | None``
    return value directly without an extra null check.

    Extra kwargs are forwarded unchanged to ``httpx.AsyncClient``.
    """

    if entry is None:
        return httpx.AsyncClient(**kwargs)
    if entry.startswith(_LOCAL_SCHEME):
        ip: str = entry.removeprefix(_LOCAL_SCHEME)
        return httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(local_address=ip),
            **kwargs,
        )
    return httpx.AsyncClient(proxies=entry, **kwargs)


from scrape_exchange._lazy_async_pool import _LazyAsyncPool


_POOLED_HTTPX_LIMITS: Final[httpx.Limits] = httpx.Limits(
    max_keepalive_connections=100,
    max_connections=40,
    # Per-fetch idle window after which httpx evicts a
    # keep-alive connection from the pool. The default 5s
    # forced a fresh CONNECT on essentially every fetch
    # under our request cadence — workers wait at least a
    # few seconds at the rate limiter between requests, so
    # 5s was below the typical per-(worker, proxy) gap.
    #
    # 300s sits comfortably inside YouTube's tolerance:
    # empirical probe on 2026-05-12 showed YouTube keeps
    # a single connection alive across 1000 sequential
    # requests with 300s gaps between them. We hold a bit
    # longer than the tested 300s gap because most of our
    # actual gaps are shorter and the cost of being wrong
    # (next request hits ConnectionResetError, httpx
    # transparently retries on a fresh connection) is
    # bounded.
    keepalive_expiry=300.0,
)
_POOLED_HTTPX_DEFAULT_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(
    10.0, connect=5.0,
)


def _make_pooled_httpx_client_for_entry(
    entry: str | None,
) -> httpx.AsyncClient:
    """Pool factory for the pooled httpx client. Configures
    keep-alive sizing and a generous default timeout. Per-fetch
    timeout overrides are still applied at call time via
    ``client.get(url, timeout=...)``."""

    if entry is None:
        return httpx.AsyncClient(
            limits=_POOLED_HTTPX_LIMITS,
            timeout=_POOLED_HTTPX_DEFAULT_TIMEOUT,
        )
    if entry.startswith(_LOCAL_SCHEME):
        ip: str = entry.removeprefix(_LOCAL_SCHEME)
        return httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(local_address=ip),
            limits=_POOLED_HTTPX_LIMITS,
            timeout=_POOLED_HTTPX_DEFAULT_TIMEOUT,
        )
    return httpx.AsyncClient(
        proxies=entry,
        limits=_POOLED_HTTPX_LIMITS,
        timeout=_POOLED_HTTPX_DEFAULT_TIMEOUT,
    )


_HTTPX_POOL: _LazyAsyncPool[
    str | None, httpx.AsyncClient,
] = _LazyAsyncPool(factory=_make_pooled_httpx_client_for_entry)


def pooled_httpx_client_for_entry(
    entry: str | None,
) -> httpx.AsyncClient:
    """Return the long-lived, keep-alive-pooled
    :class:`httpx.AsyncClient` for ``entry`` (the canonical proxy
    URL, ``local://<ipv4>``, or ``None`` for proxyless). The same
    client instance is returned across calls for the same key,
    so subsequent fetches reuse the existing TCP connection
    instead of opening a fresh one.

    The cached client is closed by ``aclose_pooled_httpx_clients()``
    at scraper shutdown. Tests use ``_reset_pool_for_tests()``
    to drop the cache without closing real connections."""

    return _HTTPX_POOL.get(entry)


async def aclose_pooled_httpx_clients() -> None:
    """Close every pooled httpx client and empty the pool. Called
    from the scraper shutdown drain."""

    await _HTTPX_POOL.aclose_all()


# ---------------------------------------------------------------
# Pool warm-up jitter
#
# When N worker processes start within ~1s of each other and each
# fires a request through every proxy, every first request opens
# a fresh CONNECT tunnel. With 16 RSS workers × 8 proxies = 128
# coincident SYNs to 8 destination IPs through one consumer-grade
# WAN router — many get dropped, surfacing as ``timeout_connect``.
# The token-bucket rate limiter does not constrain this because
# it gates request rate, not TCP connection establishment.
#
# ``jitter_pool_warmup(entry)`` sleeps for a random 0–3s the
# first time it is awaited for ``entry`` in the current process,
# and is a no-op on subsequent calls. Callers wrap their first
# use of the pooled httpx client per proxy with it, spreading
# the cold-start CONNECTs across a 3s window so the WAN router
# can establish them in sequence rather than dropping the burst.
# ---------------------------------------------------------------

POOL_WARMUP_MAX_SECONDS: float = 3.0

_POOL_WARMUP_SEEN: set[str | None] = set()
_POOL_WARMUP_LOCK: 'asyncio.Lock | None' = None


def _reset_warmup_for_tests() -> None:
    """Drop the per-process warm-up dedup set. Test-only."""
    _POOL_WARMUP_SEEN.clear()
    global _POOL_WARMUP_LOCK
    _POOL_WARMUP_LOCK = None


async def jitter_pool_warmup(entry: str | None) -> None:
    """Stagger the first CONNECT tunnel establishment to *entry*.

    Sleeps a random duration in ``[0, POOL_WARMUP_MAX_SECONDS)``
    the first time this is called for *entry* in the current
    process; no-ops on every subsequent call. Used to spread
    burst SYNs at worker startup so a consumer-grade WAN router
    or ISP CGN does not drop coincident handshakes to many
    distinct destination IPs.

    Safe to call from any worker; the per-process dedup set is
    guarded by an asyncio.Lock so concurrent first-use across
    coroutines serialises correctly.
    """
    if entry in _POOL_WARMUP_SEEN:
        return
    global _POOL_WARMUP_LOCK
    if _POOL_WARMUP_LOCK is None:
        _POOL_WARMUP_LOCK = asyncio.Lock()
    async with _POOL_WARMUP_LOCK:
        if entry in _POOL_WARMUP_SEEN:
            return
        _POOL_WARMUP_SEEN.add(entry)
        delay: float = random.uniform(0, POOL_WARMUP_MAX_SECONDS)
        if delay > 0:
            await asyncio.sleep(delay)


def _reset_pool_for_tests() -> None:
    """Drop cached pooled clients without calling aclose. Tests
    only."""

    _HTTPX_POOL.reset_for_tests()
