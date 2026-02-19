#!/usr/bin/env python3

import os
import sys
import asyncio
import argparse

import orjson
import aiofiles

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL


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

    os.makedirs(os.path.join(args.save_directory, 'uploaded'), exist_ok=True)

    if not args.upload_only:
        await scrape_channels(args, client)


async def scrape_channels(args: argparse.Namespace, client: ExchangeClient
                          ) -> None:
    channel_names: set[str] = await read_channels(args.directory)
    print(f'Read {len(channel_names)} unique channel names from .lst files')
    channel_names.discard('')  # Remove empty channel names if any
    channel_name: str
    for channel_name in channel_names:
        print(f'Processing channel: {channel_name}')
        filename: str = f'{channel_name}.json'
        saved_filepath: str = os.path.join(args.save_directory, filename)
        uploaded_filepath: str = os.path.join(
            args.save_directory, 'uploaded', filename
        )
        if os.path.exists(uploaded_filepath):
            print(f'Channel {channel_name} already uploaded, skipping')
            continue
        if args.upload_only:
            continue

        if not os.path.exists(saved_filepath):
            print(f'Channel {channel_name} not scraped, scraping now')
            channel: YouTubeChannel = YouTubeChannel(
                name=channel_name, deno_path=DENO_PATH,
                po_token_url=PO_TOKEN_URL, debug=True,
                save_dir=args.save_directory
            )
            await channel.scrape_about_page()

            async with aiofiles.open(saved_filepath, 'w') as f:
                await f.write(
                    orjson.dumps(
                        channel.to_dict(), option=orjson.OPT_INDENT_2
                    ).decode('utf-8')
                )

            print(f'Uploading channel {channel_name} to Scrape Exchange')
            try:
                await client.post(
                        client.POST_DATA_API, json={
                            'channel_name': channel_name,
                        }
                    )
            except Exception as exc:
                print(f'Error uploading channel {channel_name}: {exc}')


def process_arguments(args: argparse.Namespace) -> argparse.Namespace:
    '''
    Process and validate command-line arguments.

    :param args: The parsed command-line arguments.
    :returns: The validated command-line arguments.
    :raises: (none)
    '''

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='''
        YouTube Channel Upload Tool. This tool reads YouTube channel names
        from .lst files in a specified directory with the first word of each
        line representing the channel name. For each channel, it checks
        whether the channel was already scraped, and if not, it scrapes the
        channel data, saving it to disk. It then checks whether the scraped
        data was already uploaded, and if not, it uploads the data to the
        Scrape Exchange. It then moves the saved data to an "uploaded"
        directory to avoid re-uploading in the future.
    ''')

    parser.add_argument(
        '--directory', '-d', type=str,
        default=os.environ.get('YOUTUBE_CHANNEL_SCRAPE_DIR'),
        help=(
            'Directory containing YouTube channel data in .lst files with '
            'one channel name per line'
        )
    )
    parser.add_argument(
        '--exchange-url', '-e', type=str,
        default=os.environ.get(
            'SCRAPE_EXCHANGE_URL', 'https://scrape.exchange'
        ),
        help=(
            'Base URL for the Scrape.Exchange API (default: '
            'https://scrape.exchange)'
        )
    )
    parser.add_argument(
        '--upload-only', '-u', action='store_true',
        help='Only perform the upload step, skipping data scraping'
    )
    parser.add_argument(
        '--save-directory', '-s', type=str,
        default=os.environ.get('YOUTUBE_CHANNEL_SAVE_DIR'),
        help='Directory to save the scraped data'
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
    if not args.directory:
        print(
            'Error: Directory containing .lst files must be provided via '
            'command-line argument or environment variable '
            'YOUTUBE_CHANNEL_SCRAPE_DIR'
        )
        sys.exit(1)
    if not args.save_directory:
        print(
            'Error: Save directory must be provided via command-line argument '
            'or environment variable YOUTUBE_CHANNEL_SAVE_DIR'
        )
        sys.exit(1)
    if not os.path.isdir(args.directory):
        print(f'Error: Directory {args.directory} does not exist')
        sys.exit(1)

    if not os.path.isdir(args.save_directory):
        print(f'Error: Save directory {args.save_directory} does not exist')
        sys.exit(1)

    return args


async def read_channels(directory: str) -> set[str]:
    '''
    Reads .lst files from the specified directory and extracts YouTube channel
    names.

    :param directory: The directory containing .lst files with channel names.
    :returns: A list of YouTube channel names.
    :raises: (none)
    '''

    print(f'Reading channel names from directory: {directory}')

    channels: set[str] = set()
    for entry in os.listdir(directory):
        if entry.endswith('.lst'):
            print(f'Found .lst file: {entry}')
            async with aiofiles.open(os.path.join(directory, entry), 'r') as f:
                async for line in f:
                    channel_name: str = line.split(' ')[0].strip()
                    channels.add(channel_name)
    return channels


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]))