#!/usr/bin/env python3

'''
Benchmark yt-dlp JS runtimes by scraping a small set of YouTube
videos through each runtime back-to-back, then printing a side-by-side
comparison of wall-clock and JS-runtime subprocess spawn counts.

The point: yt-dlp's `js_runtimes` option dispatches to one of a small
set of external runtimes (deno, node, bun, quickjs) for player-JS
work — primarily n-param transforms, which can't be cached because
the input is per-URL. Each runtime is invoked as a fresh subprocess
per JS evaluation, so the dominant cost is fork+exec+startup, not
the JS itself. Switching from a heavyweight runtime (Deno/Node) to a
light one (QuickJS) can materially reduce per-scrape CPU.

This script is intentionally standalone: it builds its own YoutubeDL
instance per runtime rather than going through the production worker
plumbing, so the benchmark stays comparable across production
refactors. It mirrors the option set in
scrape_exchange/youtube/youtube_video.py::_setup_download_client.

Usage:
    python tools/yt_runtime_benchmark.py \\
        --runtimes deno,quickjs \\
        --videos dQw4w9WgXcQ,jNQXAC9IVRw \\
        [--proxy http://user:pass@host:port] \\
        [--po-token-url http://localhost:4416]

Add new runtimes to ``_RUNTIME_PROC_HINTS`` below as yt-dlp adds
support for them.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import asyncio
import sys
import tempfile
import time

from typing import Any

from yt_dlp import YoutubeDL

from scrape_exchange.youtube.youtube_client import CONSENT_COOKIES
from scrape_exchange.youtube.youtube_video import (
    PO_TOKEN_URL,
    YouTubeVideo,
)


# Substrings used to recognise a spawned subprocess as belonging to
# the runtime under test, by inspecting argv[0]. Keep this in sync
# with the keys yt-dlp accepts in `js_runtimes` (see
# yt-dlp/__init__.py: supported_js_runtimes).
_RUNTIME_PROC_HINTS: dict[str, list[str]] = {
    'deno': ['deno'],
    'node': ['node'],
    'bun': ['bun'],
    'quickjs': ['qjs', 'quickjs'],
}


class SubprocessCounter:
    '''
    Context manager that counts subprocess spawns whose executable
    path contains one of the configured hint substrings.

    Implementation: installs a ``sys.addaudithook`` listening for the
    ``subprocess.Popen`` audit event (CPython 3.8+). The audit-hook
    approach catches every subprocess spawn regardless of the API
    used (``subprocess.Popen``, ``subprocess.run``,
    ``asyncio.create_subprocess_exec``, ``os.posix_spawn``, etc.),
    which is critical because yt-dlp drives some runtimes through
    asyncio's transport layer rather than the blocking ``subprocess``
    module — a plain monkey-patch of ``subprocess.Popen`` would miss
    those entirely.

    Audit hooks cannot be uninstalled, so this counter sets an
    ``active`` flag on context entry and clears it on exit; the hook
    itself is no-op outside the active window. That keeps the counter
    composable across back-to-back ``with`` blocks for different
    runtimes within a single process.
    '''

    _hook_installed: bool = False
    _instances: list['SubprocessCounter'] = []

    def __init__(self, hints: list[str]) -> None:
        self.hints: list[str] = hints
        self.count: int = 0
        self.active: bool = False

    @staticmethod
    def _exe_from_audit_args(args: tuple) -> str:
        '''
        Extract a best-effort executable path from a
        ``subprocess.Popen`` audit event's args tuple, which is
        ``(executable, args, cwd, env)``. Falls back to ``args[1][0]``
        when the explicit executable slot is empty (the common case
        when the caller passed a list as the first positional arg).
        '''
        if not args:
            return ''
        exe_val: Any = args[0]
        if exe_val:
            return str(exe_val)
        if len(args) >= 2:
            argv_val: Any = args[1]
            if isinstance(argv_val, (list, tuple)) and argv_val:
                return str(argv_val[0])
        return ''

    @classmethod
    def _dispatch(cls, exe: str) -> None:
        for inst in cls._instances:
            if inst.active and any(h in exe for h in inst.hints):
                inst.count += 1

    @classmethod
    def _ensure_hook(cls) -> None:
        if cls._hook_installed:
            return

        def _hook(event: str, args: tuple) -> None:
            if event == 'subprocess.Popen':
                cls._dispatch(cls._exe_from_audit_args(args))

        sys.addaudithook(_hook)
        cls._hook_installed = True

    def __enter__(self) -> 'SubprocessCounter':
        SubprocessCounter._ensure_hook()
        SubprocessCounter._instances.append(self)
        self.active = True
        return self

    def __exit__(self, *exc: Any) -> None:
        self.active = False
        try:
            SubprocessCounter._instances.remove(self)
        except ValueError:
            pass


def build_client(
    runtime: str, po_token_url: str, proxy: str | None,
    ytdlp_cache_dir: str, runtime_path: str | None,
) -> YoutubeDL:
    '''
    Build a fresh YoutubeDL configured to use the given JS runtime.
    Mirrors the production opts in
    ``scrape_exchange/youtube/youtube_video.py::_setup_download_client``.
    '''

    cookie_fp: Any = tempfile.NamedTemporaryFile(
        mode='w', delete=False, suffix='.txt'
    )
    cookie_fp.write('# Netscape HTTP Cookie File\n')
    for name, value in CONSENT_COOKIES.items():
        cookie_fp.write(
            f'.youtube.com\tTRUE\t/\tFALSE\t0\t{name}\t{value}\n'
        )
    cookie_fp.close()

    runtime_opts: dict = (
        {'path': runtime_path} if runtime_path else {}
    )
    opts: dict = {
        'quiet': True,
        'noprogress': True,
        'no_color': True,
        'format': 'all',
        'proxy': proxy,
        'cookiefile': cookie_fp.name,
        'cachedir': ytdlp_cache_dir,
        'js_runtimes': {runtime: runtime_opts},
        'extractor_args': {
            'youtube': {
                'player-client': 'tv',
                'youtubepot-bgutilhttp:base_url': po_token_url,
            }
        },
    }
    return YoutubeDL(opts)


async def scrape_one(
    client: YoutubeDL, video_id: str, proxy: str | None,
) -> tuple[bool, float]:
    '''
    Scrape one video via :meth:`YouTubeVideo.scrape` using the
    supplied download client. Returns ``(success, wall_clock_seconds)``.
    Failures are logged to stderr but do not abort the benchmark; an
    unsupported runtime should show up as zero successes plus zero
    spawns of the expected binary.
    '''

    start: float = time.monotonic()
    try:
        await YouTubeVideo.scrape(
            video_id,
            channel_name=None,
            channel_thumbnail=None,
            download_client=client,
            proxies=[proxy] if proxy else [],
        )
        return True, time.monotonic() - start
    except Exception as exc:
        print(
            f'  scrape error for {video_id}: {exc}', file=sys.stderr
        )
        return False, time.monotonic() - start


async def benchmark_runtime(
    runtime: str, video_ids: list[str], po_token_url: str,
    proxy: str | None, ytdlp_cache_dir: str,
    runtime_path: str | None,
) -> dict:
    '''
    Run every ``video_id`` through one runtime and return aggregate
    stats. Each runtime gets its own fresh YoutubeDL and its own
    fresh subprocess counter, so prior runtimes can't pollute the
    measurement.
    '''

    print(f'\n=== runtime: {runtime} ===', file=sys.stderr)
    hints: list[str] = _RUNTIME_PROC_HINTS.get(runtime, [runtime])
    client: YoutubeDL = build_client(
        runtime, po_token_url, proxy, ytdlp_cache_dir, runtime_path,
    )
    durations: list[float] = []
    successes: int = 0
    with SubprocessCounter(hints) as counter:
        run_start: float = time.monotonic()
        for vid in video_ids:
            ok: bool
            dur: float
            ok, dur = await scrape_one(client, vid, proxy)
            durations.append(dur)
            if ok:
                successes += 1
            print(
                f'  {vid}: {"ok" if ok else "fail"} in {dur:.2f}s',
                file=sys.stderr,
            )
        total: float = time.monotonic() - run_start
    client.close()
    mean: float = (
        sum(durations) / len(durations) if durations else 0.0
    )
    return {
        'runtime': runtime,
        'videos': len(video_ids),
        'successes': successes,
        'total_seconds': total,
        'mean_seconds': mean,
        'subprocess_count': counter.count,
        'subprocess_per_video': (
            counter.count / len(video_ids) if video_ids else 0.0
        ),
    }


def print_table(rows: list[dict]) -> None:
    '''
    Print results as a Markdown-style table to stdout so the output
    is pasteable into PR descriptions and dashboards. stderr is
    reserved for per-video progress noise.
    '''

    headers: list[str] = [
        'runtime', 'videos', 'ok', 'total(s)', 'mean(s)',
        'subprocs', 'subprocs/vid',
    ]
    print('\n| ' + ' | '.join(headers) + ' |')
    print('|' + '|'.join('---' for _ in headers) + '|')
    for r in rows:
        print(
            f'| {r["runtime"]} '
            f'| {r["videos"]} '
            f'| {r["successes"]} '
            f'| {r["total_seconds"]:.2f} '
            f'| {r["mean_seconds"]:.2f} '
            f'| {r["subprocess_count"]} '
            f'| {r["subprocess_per_video"]:.2f} |'
        )


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Benchmark yt-dlp JS runtimes',
    )
    parser.add_argument(
        '--runtimes', required=True,
        help=(
            'Comma-separated yt-dlp js_runtimes keys to benchmark, '
            'e.g. deno,quickjs,node,bun'
        ),
    )
    parser.add_argument(
        '--videos', required=True,
        help=(
            'Comma-separated YouTube video IDs to scrape against '
            'each runtime'
        ),
    )
    parser.add_argument(
        '--proxy', default=None,
        help='Proxy URL to use for all scrapes (optional)',
    )
    parser.add_argument(
        '--po-token-url', default=PO_TOKEN_URL,
        help='bgutil PO token provider HTTP URL',
    )
    parser.add_argument(
        '--ytdlp-cache-dir', default='/var/tmp/yt_dlp',
        help='yt-dlp on-disk cache directory',
    )
    parser.add_argument(
        '--runtime-paths', default='',
        help=(
            'Optional explicit binary paths per runtime as a '
            'comma-separated list of name=path pairs, e.g. '
            'quickjs=/usr/local/bin/qjs,deno=/opt/deno/bin/deno. '
            'Bypasses yt-dlp PATH auto-discovery and any version '
            'probe that might evict an otherwise-working runtime.'
        ),
    )
    args: argparse.Namespace = parser.parse_args()

    runtimes: list[str] = [
        r.strip() for r in args.runtimes.split(',') if r.strip()
    ]
    videos: list[str] = [
        v.strip() for v in args.videos.split(',') if v.strip()
    ]
    runtime_paths: dict[str, str] = {}
    for pair in args.runtime_paths.split(','):
        pair = pair.strip()
        if not pair:
            continue
        if '=' not in pair:
            sys.exit(
                f'--runtime-paths entry {pair!r} is not name=path'
            )
        name, path = pair.split('=', 1)
        runtime_paths[name.strip()] = path.strip()

    rows: list[dict] = []
    for runtime in runtimes:
        rows.append(asyncio.run(benchmark_runtime(
            runtime, videos, args.po_token_url, args.proxy,
            args.ytdlp_cache_dir, runtime_paths.get(runtime),
        )))

    print_table(rows)


if __name__ == '__main__':
    main()
