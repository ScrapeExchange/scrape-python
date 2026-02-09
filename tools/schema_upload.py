#!/usr/bin/env python3


import os
import sys
import asyncio
import argparse

import orjson
import aiofiles
from scrape_exchange.exchange_client import ExchangeClient, Response


async def main(argv: list[str]) -> None:
    '''
    Main function to execute the YouTube channel upload process.

    :returns: (none)
    :raises: (none)
    '''

    args: argparse.Namespace = process_arguments(argv)

    client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=args.api_key_id,
        api_key_secret=args.api_key_secret,
        exchange_url=args.exchange_url
    )

    async with aiofiles.open(args.schema, mode='r') as file_desc:
        schema: dict[str, any] = orjson.loads(await file_desc.read())

    response: Response = await client.post(
        url=f'{args.exchange_url}{ExchangeClient.POST_SCHEMA_API}',
        json={
            'platform': 'YouTube',
            'version': args.version,
            'entity': args.entity,
            'json_schema': schema
        }
    )
    if response.status_code == 201:
        print('Schema uploaded successfully')
    elif response.status_code == 409:
        print(
            'Schema already exists on the exchange with the same platform, '
            'version, and entity'
        )
    else:
        print(f'Failed to upload schema: {response.status_code}')
        print(f'Response: {await response.text}')
        sys.exit(1)


def process_arguments(args: argparse.Namespace) -> argparse.Namespace:
    '''
    Process and validate command-line arguments.

    :param args: The parsed command-line arguments.
    :returns: The validated command-line arguments.
    :raises: (none)
    '''

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='''
        Tool to upload a JSONSchema to Scrape.Exchange so it
        can be used to validate scraped data.
    ''')

    parser.add_argument(
        '--schema', '-s', type=str, default=os.environ.get('SCHEMA_PATH'),
        help=(
            'Path to the JSON schema file (default: '
            'SCHEMA_PATH environment variable)'
        )
    )
    parser.add_argument(
        '--platform', '-p', type=str, default=os.environ.get('SCHEMA_PLATFORM'),
        help=(
            'Name of the platform the schema describes (default: '
            'SCHEMA_PLATFORM environment variable)'
        )
    )
    parser.add_argument(
        '--version', '-v', type=str,
        default=os.environ.get('SCHEMA_VERSION', '0.0.1'),
        help=(
            'Version of the schema being uploaded (default: '
            'SCHEMA_VERSION environment variable or "0.0.1")'
        )
    )
    parser.add_argument(
        '--entity', '-e', type=str, default=os.environ.get('SCHEMA_ENTITY'),
        help=(
            'Name of the entity the schema describes (default: SCHEMA_ENTITY '
            'environment variable)'
        )
    )
    parser.add_argument(
        '--exchange-url', type=str, default=os.environ.get(
            'SCRAPE_EXCHANGE_URL', 'https://scrape.exchange'
        ),
        help=(
            'Base URL for the Scrape.Exchange API (default: '
            'https://scrape.exchange)'
        )
    )
    parser.add_argument(
        '--api-key-id', type=str,
        default=os.environ.get('API_KEY_ID'),
        help='API key ID for authenticating with the Scrape.Exchange API'
    )
    parser.add_argument(
        '--api-key-secret', type=str,
        default=os.environ.get('API_KEY_SECRET'),
        help='API key secret for authenticating with the Scrape.Exchange API'
    )
    args: argparse.Namespace = parser.parse_args(args)

    if not args.api_key_id or not args.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via command-line '
            'arguments or environment variables API_KEY_ID and API_KEY_SECRET'
        )
        sys.exit(1)

    return args


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]))