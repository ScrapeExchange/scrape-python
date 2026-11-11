'''
Detect edits to the proxy files a scraper was started with.

The scrapers read their proxy files once, at startup. A
:class:`ProxyFileWatcher` re-checks those files periodically so the
scraper supervisor (or a standalone scraper) can restart its workers
when the set of proxies changes. A file that is missing, empty or has
a malformed line, e.g. because it is caught half-way through an edit,
is reported once and otherwise ignored: the scraper keeps running with
the proxies it has.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from scrape_exchange.proxy_loader import ProxyCatalog, load_proxy_catalog

_LOGGER: logging.Logger = logging.getLogger(__name__)

DEFAULT_PROXY_RELOAD_INTERVAL_SECONDS: float = 60.0

# Set by the supervisor in every child's environment: the supervisor
# watches the proxy files, so its children must not.
SUPERVISED_CHILD_ENV_VAR: str = 'SCRAPER_SUPERVISED'

_FileFingerprint = tuple[str, int, int, int] | tuple[str, None]


class ProxyFileWatcher:
    '''
    Report when the proxies listed in a set of proxy files change.

    :param paths: the proxy files, as passed to
        :func:`load_proxy_catalog`
    :param baseline: the proxies the files listed at startup; when
        None the files are read now
    :param interval: seconds between checks in :meth:`poll`
    :param clock: monotonic clock (tests)
    '''

    def __init__(
        self,
        paths: Sequence[Path],
        baseline: Sequence[str] | None = None,
        interval: float = DEFAULT_PROXY_RELOAD_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._paths: list[Path] = [
            Path(p).expanduser() for p in paths
        ]
        self._interval: float = interval
        self._clock: Callable[[], float] = clock
        self._last_check: float = clock()
        # None forces the first check to parse the files, so an edit
        # made after *baseline* was read is not missed.
        self._fingerprint: tuple[_FileFingerprint, ...] | None = None
        self._baseline: list[str]
        if baseline is not None:
            self._baseline = list(baseline)
            return
        self._fingerprint = self._stat_files()
        try:
            self._baseline = load_proxy_catalog(self._paths).entries
        except (OSError, ValueError) as exc:
            _LOGGER.warning(
                'Cannot load proxy files to watch them',
                exc=exc,
                extra={'proxy_files': [str(p) for p in self._paths]},
            )
            self._baseline = []

    @property
    def interval(self) -> float:
        return self._interval

    @property
    def paths(self) -> list[Path]:
        return list(self._paths)

    @property
    def proxies(self) -> list[str]:
        '''The proxies as of the last accepted change.'''
        return list(self._baseline)

    def poll(self) -> list[str] | None:
        '''
        Run :meth:`check` when at least ``interval`` seconds have
        passed since the previous check.

        :returns: the new proxy list when it changed, else None
        :raises: (none)
        '''

        now: float = self._clock()
        if now - self._last_check < self._interval:
            return None
        self._last_check = now
        return self.check()

    def check(self) -> list[str] | None:
        '''
        Re-read the proxy files if any of them changed on disk.

        :returns: the new proxy list when the set of proxies changed,
            else None (also when the files cannot be parsed)
        :raises: (none)
        '''

        fingerprint: tuple[_FileFingerprint, ...] = self._stat_files()
        if fingerprint == self._fingerprint:
            return None
        self._fingerprint = fingerprint
        try:
            catalog: ProxyCatalog = load_proxy_catalog(self._paths)
        except (OSError, ValueError) as exc:
            _LOGGER.warning(
                'Proxy files changed but cannot be loaded; keeping '
                'the current proxies',
                exc=exc,
                extra={'proxy_files': [str(p) for p in self._paths]},
            )
            return None
        if not catalog.entries:
            _LOGGER.warning(
                'Proxy files changed but list no proxies; keeping '
                'the current proxies',
                extra={'proxy_files': [str(p) for p in self._paths]},
            )
            return None
        if set(catalog.entries) == set(self._baseline):
            return None
        added: int = len(set(catalog.entries) - set(self._baseline))
        removed: int = len(set(self._baseline) - set(catalog.entries))
        self._baseline = list(catalog.entries)
        _LOGGER.info(
            'Proxy files changed',
            extra={
                'proxy_files': [str(p) for p in self._paths],
                'proxies_count': len(catalog.entries),
                'proxies_added': added,
                'proxies_removed': removed,
            },
        )
        return list(catalog.entries)

    def _stat_files(self) -> tuple[_FileFingerprint, ...]:
        fingerprints: list[_FileFingerprint] = []
        path: Path
        for path in self._paths:
            try:
                stat = path.stat()
            except OSError:
                fingerprints.append((str(path), None))
                continue
            fingerprints.append((
                str(path), stat.st_mtime_ns, stat.st_size, stat.st_ino,
            ))
        return tuple(fingerprints)
