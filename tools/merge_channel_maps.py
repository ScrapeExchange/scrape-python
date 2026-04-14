#!/usr/bin/env python3

'''
Merge multiple channel-map CSV files into one deduplicated
file.

Each input file contains lines of ``<channel-id>,<channel-handle>``.
The tool reads every file into a single dict (last-writer-wins on
duplicate channel IDs), renames the first input file to
``<filename>-<YYYYMMDD>`` as a backup, and writes the merged result
back to the original path of the first file.

Usage::

    python tools/merge_channel_maps.py map1.csv map2.csv map3.csv

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import os
import sys

from datetime import datetime, timezone


def read_channel_map(filepath: str) -> dict[str, str]:
    '''
    Read a channel-map CSV file into a dict mapping
    channel ID to channel handle.

    Blank lines and lines starting with ``#`` are skipped.
    Lines with fewer than two comma-separated fields are
    skipped with a warning on stderr.

    :param filepath: Path to the CSV file.
    :returns: A dict of channel_id -> channel_handle.
    '''

    channel_map: dict[str, str] = {}
    with open(filepath, 'r') as fd:
        line: str
        for line in fd:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts: list[str] = line.split(',')
            if len(parts) < 2:
                print(
                    f'Skipping malformed line in '
                    f'{filepath}: {line}',
                    file=sys.stderr,
                )
                continue
            channel_id: str = parts[0].strip()
            channel_handle: str = parts[1].strip()
            if channel_id and channel_handle:
                channel_map[channel_id] = channel_handle
    return channel_map


def write_channel_map(
    filepath: str, channel_map: dict[str, str],
) -> None:
    '''
    Write a channel-map dict to a CSV file, sorted by
    channel handle for stable output.

    :param filepath: Destination path.
    :param channel_map: Dict of channel_id -> channel_handle.
    '''

    with open(filepath, 'w') as fd:
        for channel_id, handle in sorted(
            channel_map.items(), key=lambda x: x[1],
        ):
            fd.write(f'{channel_id},{handle}\n')


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            'Merge multiple channel-map CSV files into a '
            'single deduplicated file.'
        ),
    )
    parser.add_argument(
        'files',
        nargs='+',
        metavar='FILE',
        help=(
            'Channel-map CSV files to merge. The first '
            'file is renamed to <name>-<YYYYMMDD> and '
            'the merged output is written in its place.'
        ),
    )
    args: argparse.Namespace = parser.parse_args()

    files: list[str] = args.files
    for filepath in files:
        if not os.path.isfile(filepath):
            print(
                f'File not found: {filepath}',
                file=sys.stderr,
            )
            sys.exit(1)

    merged: dict[str, str] = {}
    for filepath in files:
        file_map: dict[str, str] = read_channel_map(
            filepath,
        )
        print(
            f'Read {len(file_map)} entries from '
            f'{filepath}',
        )
        merged.update(file_map)

    print(f'Merged total: {len(merged)} unique entries')

    output_path: str = files[0]
    today: str = datetime.now(timezone.utc).strftime(
        '%Y%m%d',
    )
    root, ext = os.path.splitext(output_path)
    backup_path: str = f'{root}-{today}{ext}'
    os.rename(output_path, backup_path)
    print(f'Renamed {output_path} -> {backup_path}')

    write_channel_map(output_path, merged)
    print(f'Wrote {len(merged)} entries to {output_path}')


if __name__ == '__main__':
    main()
