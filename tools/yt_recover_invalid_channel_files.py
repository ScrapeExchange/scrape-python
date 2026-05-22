#!/usr/bin/env python3
'''
Best-effort recovery of ``channel-*.json.br.invalid`` files.

For each matching file in the given directory:

* Decompress the brotli payload, tolerating mid-stream corruption
  and truncation by retaining whatever bytes were emitted before
  the error.
* Parse a single JSON object from the decompressed bytes, falling
  back to trailing-garbage and last-``}`` heuristics.
* Rename ``channel`` -> ``channel_handle``.
* Remove ``categories``; if its value was a non-empty list, add
  ``category`` set to the first item.
* Re-serialise + brotli-compress and atomically write to
  ``channel-<id>.json.br`` (the same path minus ``.invalid``).
  Unlink the original ``.invalid`` source on success.
'''

import argparse
import logging
import sys
from pathlib import Path

from scrape_exchange.brotli import (
    _best_effort_decompress,
    _best_effort_json,
    brotli_write,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)


def transform(record: dict) -> dict:
    '''Return *record* with ``channel`` renamed and ``categories``
    collapsed to a single ``category``.'''
    new_record: dict = dict(record)
    if 'channel' in new_record:
        new_record['channel_handle'] = new_record.pop('channel')
    categories: object = new_record.pop('categories', None)
    if isinstance(categories, list) and categories:
        new_record['category'] = categories[0]
    return new_record


def _is_invalid_channel_file(path: Path) -> bool:
    return (
        path.is_file()
        and path.name.startswith('channel-')
        and path.name.endswith('.json.br.invalid')
    )


def _process(path: Path, *, dry_run: bool) -> bool:
    '''Recover a single ``.invalid`` file. Return True on success.'''
    decompressed: bytes = _best_effort_decompress(path.read_bytes())
    record: object = _best_effort_json(decompressed)
    if not isinstance(record, dict):
        _LOGGER.warning(f'unrecoverable (no JSON): {path}')
        return False

    new_record: dict = transform(record)
    target: Path = path.with_suffix('')  # drops trailing .invalid
    if target.exists():
        _LOGGER.warning(
            f'target already present, leaving .invalid in place: '
            f'{target}',
        )
        return False

    if dry_run:
        _LOGGER.info(f'would write {target}')
        return True

    brotli_write(target, new_record)
    path.unlink(missing_ok=True)
    _LOGGER.info(f'recovered {target.name}')
    return True


def main(argv: list[str] | None = None) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            'Best-effort recovery of channel-*.json.br.invalid '
            'files in a directory.'
        ),
    )
    parser.add_argument(
        'directory',
        type=Path,
        help='Directory containing channel-*.json.br.invalid files.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Report planned actions without modifying any files.',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Emit DEBUG-level logs (per-chunk decompress aborts).',
    )
    args: argparse.Namespace = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(levelname)s %(message)s',
    )

    directory: Path = args.directory
    if not directory.is_dir():
        _LOGGER.error(f'not a directory: {directory}')
        return 2

    scanned: int = 0
    recovered: int = 0
    failed: int = 0
    for path in sorted(directory.iterdir()):
        if not _is_invalid_channel_file(path):
            continue
        scanned += 1
        try:
            ok: bool = _process(path, dry_run=args.dry_run)
        except Exception as exc:
            _LOGGER.exception(f'error processing {path}: {exc}')
            failed += 1
            continue
        if ok:
            recovered += 1
        else:
            failed += 1

    _LOGGER.info(
        f'scanned={scanned} recovered={recovered} failed={failed}',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
