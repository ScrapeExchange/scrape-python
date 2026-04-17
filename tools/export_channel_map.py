#!/usr/bin/env python3

'''
Export or import the channel map between Redis and CSV.

Export (Redis to CSV):
    python tools/export_channel_map.py --output map.csv

Import (CSV to Redis):
    python tools/export_channel_map.py --import-file map.csv

Requires REDIS_DSN to be set.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import asyncio
import sys

from scrape_exchange.channel_map import (
    FileChannelMap,
    RedisChannelMap,
)
from scrape_exchange.settings import ScraperSettings


async def export_to_csv(
    redis_cm: RedisChannelMap, output_path: str,
) -> None:
    '''Export all entries from Redis to a CSV file.'''
    data: dict[str, str] = await redis_cm.get_all()
    with open(output_path, 'w') as f:
        for cid, handle in sorted(
            data.items(),
            key=lambda x: x[1].lower(),
        ):
            f.write(f'{cid},{handle}\n')
    print(
        f'Exported {len(data)} entries to {output_path}'
    )


async def import_from_csv(
    redis_cm: RedisChannelMap, input_path: str,
) -> None:
    '''Import entries from a CSV file into Redis.'''
    file_cm: FileChannelMap = FileChannelMap(input_path)
    data: dict[str, str] = await file_cm.get_all()
    if not data:
        print('No entries found in input file')
        return
    await redis_cm.put_many(data)
    print(
        f'Imported {len(data)} entries from '
        f'{input_path}'
    )


async def main(args: argparse.Namespace) -> None:
    settings: ScraperSettings = ScraperSettings()
    if not settings.redis_dsn:
        print(
            'Error: REDIS_DSN must be set',
            file=sys.stderr,
        )
        sys.exit(1)

    redis_cm: RedisChannelMap = RedisChannelMap(
        settings.redis_dsn,
    )

    if args.output:
        await export_to_csv(redis_cm, args.output)
    elif args.import_file:
        await import_from_csv(
            redis_cm, args.import_file,
        )


if __name__ == '__main__':
    parser: argparse.ArgumentParser = (
        argparse.ArgumentParser(
            description=(
                'Export/import channel map between '
                'Redis and CSV'
            ),
        )
    )
    group = parser.add_mutually_exclusive_group(
        required=True,
    )
    group.add_argument(
        '--output',
        help='Export Redis channel map to CSV file',
    )
    group.add_argument(
        '--import-file',
        help='Import CSV file into Redis channel map',
    )
    # Parse our args first; leave the rest for
    # pydantic-settings (ScraperSettings has
    # cli_parse_args=True and would reject unknown flags).
    args: argparse.Namespace
    remaining: list[str]
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining
    asyncio.run(main(args))
