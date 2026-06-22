#!/usr/bin/env python3

'''
One-shot repair for TikTok video Brotli JSON files.

The historical invalid files contain retired top-level fields that
are not in the public TikTok video schema. This script removes those
fields, derives ``author_avatar_url`` from ``author_avatar_urls``,
validates the repaired payload JSON against the local schema, writes
repaired ``.invalid`` files to the original basename without
``.invalid``, and rewrites normal ``.json.br`` files in place. When
a file is wrapped in an envelope with a ``data`` object, the envelope
is preserved and only ``data`` is repaired.
'''

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import brotli
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError


REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_PATH: Path = (
    REPO_ROOT / 'tests' / 'collateral'
    / 'drand-tiktok-video-schema.json'
)
AUTHOR_AVATAR_PREFERENCE: tuple[str, ...] = (
    'medium', 'thumb', 'large',
)
RETIRED_TOP_LEVEL_FIELDS: frozenset[str] = frozenset({
    'aspect_ratio',
    'bitrate',
    'codec',
    'video_height',
    'video_ratio',
    'video_width',
    'volume_loudness',
    'volume_peak',
})


@dataclass
class RepairStats:
    seen: int = 0
    repaired: int = 0
    skipped: int = 0
    failed: int = 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Repair invalid TikTok video Brotli JSON files.',
    )
    parser.add_argument(
        'directory',
        type=Path,
        help='Directory containing TikTok video Brotli JSON files.',
    )
    parser.add_argument(
        '--schema',
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help='TikTok video JSON Schema path.',
    )
    parser.add_argument(
        '--limit',
        type=positive_int,
        default=None,
        help='Maximum number of files to repair.',
    )
    parser.add_argument(
        '--all-json-br',
        action='store_true',
        help=(
            'Process every file whose name contains ".json.br"; '
            'normal .json.br files are repaired in place.'
        ),
    )
    return parser.parse_args(argv)


def positive_int(value: str) -> int:
    parsed: int = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError(
            '--limit must be greater than zero',
        )
    return parsed


def format_error(error: ValidationError) -> str:
    pointer: str = '/' + '/'.join(str(p) for p in error.absolute_path)
    if pointer == '/':
        pointer = '<root>'
    return f'{pointer}: {error.message}'


def load_validator(schema_path: Path) -> Draft202012Validator:
    schema: Any = json.loads(schema_path.read_text(encoding='utf-8'))
    if not isinstance(schema, dict):
        raise ValueError(f'schema is not a JSON object: {schema_path}')
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_record(
    record: dict[str, Any],
    validator: Draft202012Validator,
) -> str | None:
    error: ValidationError
    for error in validator.iter_errors(record):
        return format_error(error)
    return None


def preferred_author_avatar_url(
    author_avatar_urls: object,
) -> str | None:
    if not isinstance(author_avatar_urls, list):
        return None

    by_name: dict[str, str] = {}
    for item in author_avatar_urls:
        if not isinstance(item, dict):
            continue
        name: object = item.get('name')
        url: object = item.get('url')
        if isinstance(name, str) and isinstance(url, str) and url:
            by_name[name] = url

    for name in AUTHOR_AVATAR_PREFERENCE:
        url: str | None = by_name.get(name)
        if url:
            return url
    return None


def repair_record(record: dict[str, Any]) -> dict[str, Any]:
    repaired: dict[str, Any] = dict(record)
    payload: dict[str, Any] = payload_record(repaired)
    for field_name in RETIRED_TOP_LEVEL_FIELDS:
        payload.pop(field_name, None)

    author_avatar_url: str | None = preferred_author_avatar_url(
        payload.get('author_avatar_urls'),
    )
    if author_avatar_url is not None:
        payload['author_avatar_url'] = author_avatar_url
    return repaired


def payload_record(record: dict[str, Any]) -> dict[str, Any]:
    data: object = record.get('data')
    if isinstance(data, dict):
        return data
    return record


def read_brotli_json(path: Path) -> dict[str, Any]:
    raw_json: bytes = brotli.decompress(path.read_bytes())
    data: Any = json.loads(raw_json.decode('utf-8'))
    if not isinstance(data, dict):
        raise ValueError('decoded JSON is not an object')
    return data


def encode_brotli_json(record: dict[str, Any]) -> bytes:
    raw_json: bytes = json.dumps(
        record,
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')
    return brotli.compress(raw_json, quality=5)


def atomic_write(path: Path, content: bytes) -> None:
    tmp_fd: int
    tmp_name: str
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f'.{path.name}.',
        suffix='.tmp',
        dir=str(path.parent),
    )
    tmp_path: Path = Path(tmp_name)
    try:
        with os.fdopen(tmp_fd, 'wb') as tmp_file:
            tmp_fd = -1
            tmp_file.write(content)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_fd >= 0:
            os.close(tmp_fd)
        tmp_path.unlink(missing_ok=True)


def target_path_for(
    path: Path,
    *,
    all_json_br: bool = False,
) -> Path:
    target_name: str = path.name.removesuffix('.invalid')
    if target_name == path.name and not all_json_br:
        raise ValueError('filename does not end with .invalid')
    if target_name == path.name and '.json.br' not in path.name:
        raise ValueError('filename does not contain .json.br')
    return path.with_name(target_name)


def repair_file(
    path: Path,
    validator: Draft202012Validator,
    *,
    all_json_br: bool = False,
) -> tuple[bool, str]:
    try:
        target_path: Path = target_path_for(
            path,
            all_json_br=all_json_br,
        )
        if target_path != path and target_path.exists():
            return False, f'target exists: {target_path}'

        repaired: dict[str, Any] = repair_record(read_brotli_json(path))
        validation_error: str | None = validate_record(
            payload_record(repaired),
            validator,
        )
        if validation_error is not None:
            return False, f'schema validation failed: {validation_error}'

        repaired_bytes: bytes = encode_brotli_json(repaired)
        verified: dict[str, Any] = json.loads(
            brotli.decompress(repaired_bytes).decode('utf-8'),
        )
        verify_error: str | None = validate_record(
            payload_record(verified),
            validator,
        )
        if verify_error is not None:
            return False, f'repaired JSON failed schema: {verify_error}'

        atomic_write(target_path, repaired_bytes)
        if target_path != path:
            path.unlink()
        return True, ''
    except Exception as exc:
        return False, f'{type(exc).__name__}: {exc}'


def iter_invalid_files(data_dir: Path) -> list[Path]:
    return sorted(
        [
            path
            for path in data_dir.iterdir()
            if path.is_file() and path.name.endswith('.invalid')
        ],
        key=lambda path: path.name,
    )


def iter_json_br_files(data_dir: Path) -> list[Path]:
    return sorted(
        [
            path
            for path in data_dir.iterdir()
            if path.is_file() and '.json.br' in path.name
        ],
        key=lambda path: path.name,
    )


def print_progress(stats: RepairStats) -> None:
    print(
        'progress '
        f'seen={stats.seen} '
        f'repaired={stats.repaired} '
        f'skipped={stats.skipped} '
        f'failed={stats.failed}',
        file=sys.stderr,
    )


def print_done(stats: RepairStats) -> None:
    print(
        'done '
        f'seen={stats.seen} '
        f'repaired={stats.repaired} '
        f'skipped={stats.skipped} '
        f'failed={stats.failed}',
        file=sys.stderr,
    )


def run(
    data_dir: Path,
    validator: Draft202012Validator,
    *,
    limit: int | None = None,
    all_json_br: bool = False,
) -> RepairStats:
    stats: RepairStats = RepairStats()
    paths: list[Path] = (
        iter_json_br_files(data_dir)
        if all_json_br
        else iter_invalid_files(data_dir)
    )
    for path in paths:
        if limit is not None and stats.repaired >= limit:
            break
        stats.seen += 1
        ok: bool
        message: str
        ok, message = repair_file(
            path,
            validator,
            all_json_br=all_json_br,
        )
        if ok:
            stats.repaired += 1
        elif message.startswith('target exists:'):
            stats.skipped += 1
            print(f'SKIP {path}: {message}', file=sys.stderr)
        else:
            stats.failed += 1
            print(f'FAIL {path}: {message}', file=sys.stderr)

        if stats.seen % 1000 == 0:
            print_progress(stats)
    return stats


def main(argv: list[str] | None = None) -> int:
    args: argparse.Namespace = parse_args(
        sys.argv[1:] if argv is None else argv,
    )
    data_dir: Path = args.directory
    if not data_dir.is_dir():
        print(f'Error: directory does not exist: {data_dir}', file=sys.stderr)
        return 2

    validator: Draft202012Validator = load_validator(args.schema)
    stats: RepairStats = run(
        data_dir,
        validator,
        limit=args.limit,
        all_json_br=args.all_json_br,
    )
    print_done(stats)
    return 1 if stats.failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
