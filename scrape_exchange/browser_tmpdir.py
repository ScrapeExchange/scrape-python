'''
Per-process temp directory for browser-driven scrapers.

Playwright creates a ~100 MB Firefox profile directory under the
Node driver's ``os.tmpdir()`` for every browser launch and only
removes it on ``browser.close()``. A worker that dies without closing
its browsers (watchdog ``os._exit``, OOM, SIGKILL) leaks one profile
per browser. Inside a container ``/tmp`` lives in the writable layer
and survives ``docker restart``, so a crash loop fills the host disk.

:func:`isolate_browser_tmpdir` points ``TMPDIR`` at a directory owned
by this worker slot and wipes whatever a previous incarnation of the
same slot left behind. Worker IDs are stable per supervisor slot, so
a respawned child cleans up after its predecessor without touching
the live profiles of sibling workers.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import os
import shutil
import tempfile

_LOGGER: logging.Logger = logging.getLogger(__name__)

_DIR_PREFIX: str = 'scrape-browser'


def browser_tmpdir_path(
    scraper_label: str, worker_id: str, base_dir: str | None = None,
) -> str:
    '''Return the temp directory owned by one worker slot.'''
    base: str = base_dir or tempfile.gettempdir()
    return os.path.join(base, f'{_DIR_PREFIX}-{scraper_label}-{worker_id}')


def isolate_browser_tmpdir(
    scraper_label: str, worker_id: str, base_dir: str | None = None,
) -> str:
    '''
    Wipe and recreate this worker slot's temp directory, then export
    it as ``TMPDIR`` so browser drivers started afterwards (which
    inherit ``os.environ``) create their profiles inside it.

    Must run before the first browser launch in the process.

    :returns: the directory path.
    '''
    path: str = browser_tmpdir_path(scraper_label, worker_id, base_dir)
    if os.path.isdir(path):
        _LOGGER.info(
            'Removing browser temp files left by a previous run',
            extra={'path': path},
        )
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, mode=0o700, exist_ok=True)
    os.environ['TMPDIR'] = path
    return path
