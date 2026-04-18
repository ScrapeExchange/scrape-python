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

import asyncio
import sys

from pydantic import Field, model_validator

from scrape_exchange.creator_map import (
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.settings import ScraperSettings


class ExportSettings(ScraperSettings):
    '''Settings for the channel-map export/import tool.'''

    output: str | None = Field(
        default='channel_map.csv',
        description=(
            'Export Redis channel map to CSV file'
        ),
    )
    import_file: str | None = Field(
        default=None,
        description=(
            'Import CSV file into Redis channel map'
        ),
    )

    @model_validator(mode='after')
    def _require_one_action(self) -> 'ExportSettings':
        if self.output and self.import_file:
            raise ValueError(
                '--output and --import-file are '
                'mutually exclusive'
            )
        if not self.output and not self.import_file:
            raise ValueError(
                'one of --output or --import-file '
                'is required'
            )
        return self


async def export_to_csv(
    redis_cm: RedisCreatorMap, output_path: str,
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
    redis_cm: RedisCreatorMap, input_path: str,
) -> None:
    '''Import entries from a CSV file into Redis.'''
    file_cm: FileCreatorMap = FileCreatorMap(input_path)
    data: dict[str, str] = await file_cm.get_all()
    if not data:
        print('No entries found in input file')
        return
    await redis_cm.put_many(data)
    print(
        f'Imported {len(data)} entries from '
        f'{input_path}'
    )


async def main() -> None:
    settings: ExportSettings = ExportSettings()
    if not settings.redis_dsn:
        print(
            'Error: REDIS_DSN must be set',
            file=sys.stderr,
        )
        sys.exit(1)

    redis_cm: RedisCreatorMap = RedisCreatorMap(
        settings.redis_dsn,
        platform='youtube',
    )

    if settings.output:
        await export_to_csv(redis_cm, settings.output)
    elif settings.import_file:
        await import_from_csv(
            redis_cm, settings.import_file,
        )


if __name__ == '__main__':
    asyncio.run(main())
