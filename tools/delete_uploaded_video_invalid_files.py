#!/usr/bin/env python3
'''Delete invalid YouTube video files whose IDs are already uploaded.

This one-shot maintenance tool reads ``uploaded_videos.lst`` containing
one YouTube video ID per line, scans the YouTube video data directory,
extracts IDs from ``video-min-*.json.br.invalid`` and
``video-dlp-*.json.br.invalid`` filenames, and deletes matching invalid
files when ``--delete`` is supplied.
'''

from __future__ import annotations

import os
import sys

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.file_management import (
    COMPRESSED_JSON_SUFFIX,
    VIDEO_ID_RE,
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
)


INVALID_SUFFIX: str = '.invalid'
DEFAULT_UPLOADED_LIST: Path = (
    Path.home() / 'byoda' / 'data' / 'uploaded_videos.lst'
)
DEFAULT_VIDEO_DIR: Path = (
    Path.home() / 'byoda' / 'data' / 'scraped' / 'youtube' / 'videos'
)


class CleanupSettings(BaseSettings):
    '''CLI / env / .env settings for the one-shot cleanup.'''

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / '.env'),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_implicit_flags=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    uploaded_list: Path = Field(
        default=DEFAULT_UPLOADED_LIST,
        validation_alias=AliasChoices(
            'UPLOADED_VIDEOS_LIST', 'uploaded_list',
        ),
        description='File containing one uploaded video_id per line.',
    )
    video_dir: Path = Field(
        default=DEFAULT_VIDEO_DIR,
        validation_alias=AliasChoices(
            'VIDEO_INVALID_CLEANUP_DIR', 'video_dir',
        ),
        description='Directory containing YouTube video files.',
    )
    unmatched_list: Path = Field(
        default=Path('video_ids.lst'),
        validation_alias=AliasChoices(
            'UNMATCHED_VIDEO_IDS_LIST', 'unmatched_list',
        ),
        description='File to write unmatched video_ids to.',
    )
    delete: bool = Field(
        default=False,
        description='Actually delete matching invalid files.',
    )


@dataclass(frozen=True)
class CleanupResult:
    uploaded_ids: int = 0
    scanned: int = 0
    invalid_files: int = 0
    matched: int = 0
    unmatched: int = 0
    deleted: int = 0
    errors: int = 0


def load_uploaded_ids(path: Path) -> set[str]:
    '''Read uploaded video IDs, ignoring blank lines.'''

    video_ids: set[str] = set()
    invalid_lines: int = 0
    invalid_examples: list[str] = []
    with path.open('r', encoding='utf-8') as f:
        for line_no, raw in enumerate(f, start=1):
            video_id: str = raw.strip()
            if not video_id:
                continue
            if VIDEO_ID_RE.fullmatch(video_id) is None:
                invalid_lines += 1
                if len(invalid_examples) < 5:
                    invalid_examples.append(
                        f'{line_no}:{video_id!r}',
                    )
                continue
            video_ids.add(video_id)
    if invalid_lines:
        examples: str = ', '.join(invalid_examples)
        print(
            f'{path}: ignored {invalid_lines} invalid video_id '
            f'lines; examples: {examples}',
            file=sys.stderr,
        )
    return video_ids


def extract_invalid_video_id(filename: str) -> str | None:
    '''Extract a video ID from an invalid YouTube video filename.'''

    suffix: str = f'{COMPRESSED_JSON_SUFFIX}{INVALID_SUFFIX}'
    if not filename.endswith(suffix):
        return None

    for prefix in (VIDEO_MIN_FILE_PREFIX, VIDEO_YTDLP_FILE_PREFIX):
        if not filename.startswith(prefix):
            continue
        video_id: str = filename[len(prefix):-len(suffix)]
        if VIDEO_ID_RE.fullmatch(video_id) is not None:
            return video_id
        return None
    return None


def iter_video_invalid_files(video_dir: Path) -> Iterator[os.DirEntry[str]]:
    '''Yield candidate invalid video files without materialising a list.'''

    with os.scandir(video_dir) as entries:
        for entry in entries:
            if not entry.name.startswith('video-'):
                continue
            if not entry.name.endswith(INVALID_SUFFIX):
                continue
            try:
                if not entry.is_file():
                    continue
            except OSError:
                continue
            yield entry


def cleanup_invalid_files(
    uploaded_ids: set[str],
    video_dir: Path,
    *,
    delete: bool,
    unmatched_list: Path,
) -> CleanupResult:
    '''Delete matching invalid files, or count them in dry-run mode.'''

    scanned: int = 0
    invalid_files: int = 0
    matched: int = 0
    unmatched_ids: set[str] = set()
    deleted: int = 0
    errors: int = 0

    for entry in iter_video_invalid_files(video_dir):
        scanned += 1
        video_id: str | None = extract_invalid_video_id(entry.name)
        if video_id is None:
            continue
        invalid_files += 1
        if video_id not in uploaded_ids:
            unmatched_ids.add(video_id)
            continue
        matched += 1
        if not delete:
            continue
        try:
            os.unlink(entry.path)
        except OSError as exc:
            errors += 1
            print(
                f'failed to delete {entry.path}: {exc}',
                file=sys.stderr,
            )
            continue
        deleted += 1

    unmatched_list.write_text(
        ''.join(f'{video_id}\n' for video_id in sorted(unmatched_ids)),
        encoding='utf-8',
    )

    return CleanupResult(
        uploaded_ids=len(uploaded_ids),
        scanned=scanned,
        invalid_files=invalid_files,
        matched=matched,
        unmatched=len(unmatched_ids),
        deleted=deleted,
        errors=errors,
    )


def main(argv: list[str] | None = None) -> int:
    settings = CleanupSettings(_cli_parse_args=argv)
    uploaded_ids: set[str] = load_uploaded_ids(settings.uploaded_list)
    result: CleanupResult = cleanup_invalid_files(
        uploaded_ids,
        settings.video_dir,
        delete=settings.delete,
        unmatched_list=settings.unmatched_list,
    )
    mode: str = 'delete' if settings.delete else 'dry-run'
    print(
        f'mode={mode} uploaded_ids={result.uploaded_ids} '
        f'scanned={result.scanned} invalid_files={result.invalid_files} '
        f'matched={result.matched} unmatched={result.unmatched} '
        f'deleted={result.deleted} errors={result.errors}'
    )
    return 1 if result.errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
