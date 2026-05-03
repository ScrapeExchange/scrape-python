#!/usr/bin/env python3
'''
Import yt-dlp ``--write-info-json`` files from a directory,
transform each into our YouTube video schema shape, validate
against the canonical schema, and write to a target directory
as ``video-dlp-<video_id>.json.br`` (brotli-compressed JSON).

Default behaviour is **skip-existing**: a video whose output file
already exists is left alone. Pass ``--overwrite`` to replace
existing files. Validation against
``tests/collateral/boinko-youtube-video-schema.json`` always runs
before a record is written; a record that fails validation is
counted as an error and not written.

Input directory is walked flat (top-level only). Only files
ending in ``.json`` are considered.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import json
import os
import secrets
import sys
import time
from pathlib import Path

import brotli
from jsonschema import Draft202012Validator

from scrape_exchange.youtube.youtube_video import YouTubeVideo


_SCHEMA_PATH: Path = (
    Path(__file__).parent.parent
    / 'tests' / 'collateral'
    / 'boinko-youtube-video-schema.json'
)
_OUTPUT_PREFIX: str = 'video-dlp-'
_OUTPUT_SUFFIX: str = '.json.br'
_PROGRESS_INTERVAL: int = 1000
_PROGRESS_SECONDS: float = 60.0


def _atomic_write_brotli(path: Path, data: bytes) -> None:
    '''Write brotli-compressed *data* to *path* atomically.'''
    tmp: Path = path.with_name(
        f'.tmp-{secrets.token_hex(8)}{path.name}'
    )
    try:
        with open(tmp, 'wb') as f:
            f.write(brotli.compress(data))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _load_info(info_path: Path) -> dict | None:
    '''Read and parse the input file. Logs and returns ``None``
    on any failure.'''
    try:
        info = json.loads(info_path.read_text())
    except Exception as exc:
        print(f'parse failed {info_path}: {exc}', file=sys.stderr)
        return None
    if not isinstance(info, dict):
        print(
            f'top-level not a dict {info_path}', file=sys.stderr,
        )
        return None
    return info


def _validate_and_write(
    record: dict, output_path: Path, info_path: Path,
    validator: Draft202012Validator, dry_run: bool,
) -> str:
    '''Validate *record* against the schema and (if not dry-run)
    write to *output_path*. Returns ``'written'`` or
    ``'error'``.'''
    try:
        validator.validate(record)
    except Exception as exc:
        first_line: str = str(exc).splitlines()[0]
        print(
            f'schema validation failed {info_path}: '
            f'{first_line}',
            file=sys.stderr,
        )
        return 'error'
    if dry_run:
        return 'written'
    encoded: bytes = json.dumps(
        record, ensure_ascii=False,
    ).encode('utf-8')
    try:
        _atomic_write_brotli(output_path, encoded)
    except OSError as exc:
        print(
            f'write failed {output_path}: {exc}',
            file=sys.stderr,
        )
        return 'error'
    return 'written'


def _process_one(
    info_path: Path, output_dir: Path,
    validator: Draft202012Validator,
    overwrite: bool, dry_run: bool,
) -> str:
    '''Process a single yt-dlp .json file. Returns one of
    ``'written'`` | ``'skipped'`` | ``'error'``.'''
    info: dict | None = _load_info(info_path)
    if info is None:
        return 'error'
    video_id = info.get('id')
    if not isinstance(video_id, str) or not video_id:
        print(f'missing id {info_path}', file=sys.stderr)
        return 'error'
    output_path: Path = output_dir / (
        f'{_OUTPUT_PREFIX}{video_id}{_OUTPUT_SUFFIX}'
    )
    if output_path.exists() and not overwrite:
        return 'skipped'
    try:
        video: YouTubeVideo = YouTubeVideo.from_yt_dlp(info)
    except Exception as exc:
        print(
            f'mapping failed {info_path}: {exc}',
            file=sys.stderr,
        )
        return 'error'
    return _validate_and_write(
        video.to_dict(), output_path, info_path,
        validator, dry_run,
    )


def _run_import(
    input_dir: Path, output_dir: Path,
    validator: Draft202012Validator,
    overwrite: bool, dry_run: bool, limit: int | None,
) -> tuple[int, int, int, int]:
    '''
    Walk *input_dir* flat for ``*.json`` files, process each
    via :func:`_process_one`, emit a progress line whichever
    comes first: every ``_PROGRESS_INTERVAL`` files OR
    ``_PROGRESS_SECONDS`` of wall-clock time since the last
    progress line. Returns
    ``(scanned, written, skipped, errors)``.
    '''
    scanned: int = 0
    written: int = 0
    skipped: int = 0
    errors: int = 0
    last_progress_at: float = time.monotonic()
    for entry in os.scandir(input_dir):
        if limit is not None and scanned >= limit:
            break
        if not entry.is_file(follow_symlinks=False):
            continue
        if not entry.name.endswith('.json'):
            continue
        scanned += 1
        now: float = time.monotonic()
        if (
            scanned % _PROGRESS_INTERVAL == 0
            or now - last_progress_at >= _PROGRESS_SECONDS
        ):
            print(
                f'  progress: scanned={scanned} '
                f'written={written} skipped={skipped} '
                f'errors={errors}',
                file=sys.stderr, flush=True,
            )
            last_progress_at = now
        outcome: str = _process_one(
            Path(entry.path), output_dir, validator,
            overwrite, dry_run,
        )
        if outcome == 'written':
            written += 1
        elif outcome == 'skipped':
            skipped += 1
        elif outcome == 'error':
            errors += 1
    return scanned, written, skipped, errors


def main() -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            'Convert yt-dlp --write-info-json files in a flat '
            'directory to brotli-compressed video-dlp-*.json.br '
            'files in an output directory.'
        ),
    )
    parser.add_argument('input_dir', type=Path)
    parser.add_argument('output_dir', type=Path)
    parser.add_argument(
        '--limit', type=int, default=None,
        help='stop after processing this many input files',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='validate and map but do not write',
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help=(
            'replace existing output files (default behaviour '
            'is to skip when the target file exists)'
        ),
    )
    args: argparse.Namespace = parser.parse_args()

    if not args.input_dir.is_dir():
        print(
            f'input dir missing: {args.input_dir}',
            file=sys.stderr,
        )
        return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)

    schema: dict = json.loads(_SCHEMA_PATH.read_text())
    validator: Draft202012Validator = Draft202012Validator(
        schema,
    )

    scanned: int
    written: int
    skipped: int
    errors: int
    scanned, written, skipped, errors = _run_import(
        args.input_dir, args.output_dir, validator,
        args.overwrite, args.dry_run, args.limit,
    )

    print(
        f'scanned={scanned} written={written} '
        f'skipped={skipped} errors={errors}'
    )
    return 0 if errors == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
