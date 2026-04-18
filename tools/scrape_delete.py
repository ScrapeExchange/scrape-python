#!/usr/bin/env python3

'''
Scrape Delete Tool, used when scraping errors made it into the system.
Iterates over brotli-compressed JSON files in a specified directory,
extracts the platform content ID from each, and calls the DELETE
/api/v1/data endpoint on Scrape.Exchange for the specified platform and entity.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import sys
import asyncio
import logging

from pathlib import Path

import orjson
import brotli

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from httpx import Response

from scrape_exchange.exchange_client import ExchangeClient


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / '.env'),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    exchange_url: str = Field(
        default='https://scrape.exchange',
        validation_alias=AliasChoices('EXCHANGE_URL', 'exchange_url'),
        description='Base URL for the Scrape.Exchange API'
    )
    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_ID', 'api_key_id'),
        description=(
            'API key ID for authenticating with the Scrape.Exchange API'
        ),
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_SECRET', 'api_key_secret'),
        description=(
            'API key secret for authenticating with the Scrape.Exchange API'
        ),
    )
    platform: str = Field(
        default='youtube',
        validation_alias=AliasChoices('PLATFORM', 'platform'),
        description=(
            'Platform to target for channel deletion (default: youtube)'
        ),
    )
    entity: str = Field(
        default='channel',
        validation_alias=AliasChoices('ENTITY', 'entity'),
        description='Entity type to target for deletion (default: channel)'
    )
    platform_content_id_field: str = Field(
        default='channel',
        validation_alias=AliasChoices(
            'PLATFORM_CONTENT_ID_FIELD', 'platform_content_id_field'
        ),
        description=(
            'Field name in the JSON files that contains the platform content '
            'ID (default: channel)'
        ),
    )
    username: str | None = Field(
        validation_alias=AliasChoices('USERNAME', 'username'),
    )
    schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices('SCHEMA_VERSION', 'schema_version'),
        description='Schema version for the JSON files (default: 0.0.1)'
    )
    delete_directory: str = Field(
        default='/tmp/404s',
        validation_alias=AliasChoices('DELETE_DIRECTORY', 'delete_directory'),
        description=(
            'Directory containing brotli-compressed channel JSON files'
            'to delete.'
        )
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
        description='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)'
    )
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices('DRY_RUN', 'dry_run'),
        description='If set, print item IDs without calling the API'
    )


async def get_item_id(client: ExchangeClient, exchange_url: str,
                      username: str, platform: str, entity: str,
                      version: str, platform_content_id: str) -> str | None:
    '''
    Returns the item_id for the given
    /username/platform/entity/platform_content_id
    '''
    url: str = (
        f'{exchange_url}{ExchangeClient.GET_DATA_PARAM}'
        f'/{username}/{platform}/{entity}/{version}/{platform_content_id}'
    )

    resp: Response = await client.get(url)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        logging.warning(
            'GET failed',
            extra={
                'url': url,
                'status_code': resp.status_code,
                'body': resp.text,
            },
        )
        return None
    data: dict = resp.json()
    return data.get('item_id')


async def delete_by_item_id(client: ExchangeClient, exchange_url: str,
                            item_id: str) -> bool:
    url: str = f'{exchange_url}/api/v1/data/item_id/{item_id}'
    resp: Response = await client.delete(url)
    if resp.status_code in (200, 204):
        logging.info('Deleted item', extra={'item_id': item_id})
        return True
    elif resp.status_code == 404:
        logging.warning(
            'item not found on server',
            extra={'item_id': item_id},
        )
        return True
    else:
        logging.warning(
            'Failed to delete item',
            extra={
                'item_id': item_id,
                'status_code': resp.status_code,
                'body': resp.text,
            },
        )
        return False


def _read_platform_content_id(path: Path, field: str) -> str | None:
    try:
        data: dict = orjson.loads(brotli.decompress(path.read_bytes()))
    except Exception as exc:
        logging.warning(
            'Failed to read/decompress',
            exc=exc,
            extra={'name': path.name},
        )
        return None
    platform_content_id: str | None = data.get(field)
    if not platform_content_id:
        logging.warning(
            'Field missing in file, skipping',
            extra={'field': field, 'name': path.name},
        )
    return platform_content_id


async def _process_content_id(client: ExchangeClient, settings: Settings,
                              platform_content_id: str
                              ) -> tuple[int, int, int]:
    item_id: str | None = await get_item_id(
        client, settings.exchange_url, settings.username,
        settings.platform, settings.entity, settings.schema_version,
        platform_content_id
    )
    if not item_id:
        logging.warning(
            'No items found on server for platform_content_id',
            extra={'platform_content_id': platform_content_id},
        )
        return 0, 0, 1

    if settings.dry_run:
        print(
            f'[dry-run] Would delete item_id={item_id} '

                f'({platform_content_id})'
            )
        return 0, 0, 0

    deleted: int = 0
    failed: int = 0
    if await delete_by_item_id(client, settings.exchange_url, item_id):
        deleted += 1
    else:
        failed += 1
    return deleted, failed, 0


async def main() -> None:
    settings: Settings = Settings()
    logging.basicConfig(
        level=settings.log_level,
        format='%(levelname)s:%(asctime)s:%(filename)s:%(funcName)s():%(lineno)d:%(message)s',
    )

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)

    delete_dir: Path = Path(settings.delete_directory)
    if not delete_dir.is_dir():
        print(f'Error: {delete_dir} is not a directory')
        sys.exit(1)

    files: list[Path] = [
        Path(delete_dir) / f for f in os.listdir(delete_dir)
        if f.endswith('.json.br')
    ]
    logging.info(
        'Found files in delete directory',
        extra={
            'files_length': len(files),
            'delete_dir': delete_dir,
        },
    )

    client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )

    deleted: int = 0
    failed: int = 0
    skipped: int = 0

    for path in files:
        platform_content_id: str | None = _read_platform_content_id(
            path, settings.platform_content_id_field
        )
        if not platform_content_id:
            skipped += 1
            continue

        d: int
        f: int
        s: int
        d, f, s = await _process_content_id(
            client, settings, platform_content_id
        )
        deleted += d
        failed += f
        skipped += s

    logging.info(
        'Done.',
        extra={
            'deleted': deleted,
            'failed': failed,
            'skipped': skipped,
        },
    )


if __name__ == '__main__':
    asyncio.run(main())
