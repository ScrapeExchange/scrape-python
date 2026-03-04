#!/usr/bin/env python3
'''
Tool to listen for real-time messages from the messages API via WebSocket.

Connects to the /api/messages/v1 WebSocket endpoint and prints incoming
messages to stdout. Filters can be applied server-side (username, platform,
entity, platform_topic_id) or client-side (schema_owner, schema_version).
'''

import argparse
import asyncio
import json
import logging
import sys
import uuid
from urllib.parse import urlparse

import orjson
import websockets

_LOGGER: logging.Logger = logging.getLogger(__name__)

# URL path pattern:
# /api/data/v1/param/{schema_username}/{platform}/{entity}/{version}/...
_DATA_URL_PARAM_PREFIX = '/api/data/v1/param/'


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Listen for real-time messages from the '
            'scrape-api messages WebSocket.'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--url',
        default='wss://scrape.exchange/api/messages/v1',
        help='WebSocket URL of the messages endpoint',
    )
    parser.add_argument(
        '--username', '-u',
        default=None,
        help='Filter by uploading username (server-side)',
    )
    parser.add_argument(
        '--platform', '-p',
        default=None,
        help='Filter by platform, e.g. youtube, twitch (server-side)',
    )
    parser.add_argument(
        '--entity', '-e',
        default=None,
        help='Filter by entity type, e.g. channel, video (server-side)',
    )
    parser.add_argument(
        '--schema-owner',
        default=None,
        help=(
            'Filter by schema owner username '
            '(client-side, matched against data_url)'
        ),
    )
    parser.add_argument(
        '--schema-version',
        default=None,
        help=(
            'Filter by schema version, e.g. 0.0.1 '
            '(client-side, matched against data_url)'
        ),
    )
    parser.add_argument(
        '--platform-creator-id',
        default=None,
        help='Filter by platform creator ID (server-side)',
    )
    parser.add_argument(
        '--platform-topic-id',
        default=None,
        help='Filter by platform topic ID (server-side)',
    )
    parser.add_argument(
        '--subscription-type',
        default='all',
        choices=['all', 'counters', 'metadata', 'payload'],
        help='Type of data to subscribe to',
    )
    parser.add_argument(
        '--log-level',
        default='WARNING',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level',
    )
    return parser.parse_args()


def _build_subscription(args: argparse.Namespace) -> dict:
    '''Build the SubscriptionRequest dict to send to the server.'''
    request: dict = {
        'request_id': str(uuid.uuid4()),
        'command': 'subscribe',
        'subscription_type': args.subscription_type,
    }
    if args.username:
        request['username'] = args.username
    if args.platform:
        request['platform'] = args.platform
    if args.entity:
        request['entity'] = args.entity
    if args.platform_topic_id:
        request['platform_topic_id'] = args.platform_topic_id
    if args.platform_creator_id:
        request['platform_creator_id'] = args.platform_creator_id
    return request


def _parse_data_url(data_url: str) -> dict[str, str]:
    '''
    Extract schema fields from the data URL.

    Expected URL path format:
    /api/data/v1/param/{schema_username}/{platform}/{entity}/{version}/...
    '''
    parsed = urlparse(data_url)
    path: str = parsed.path
    if not path.startswith(_DATA_URL_PARAM_PREFIX):
        return {}
    remainder: str = path[len(_DATA_URL_PARAM_PREFIX):]
    parts: list[str] = remainder.strip('/').split('/')
    result: dict[str, str] = {}
    if len(parts) >= 1:
        result['schema_owner'] = parts[0]
    if len(parts) >= 2:
        result['schema_platform'] = parts[1]
    if len(parts) >= 3:
        result['schema_entity'] = parts[2]
    if len(parts) >= 4:
        result['schema_version'] = parts[3]
    return result


def _matches_url_filters(
    url_fields: dict[str, str], args: argparse.Namespace
) -> bool:
    '''Check schema filters extracted from the data_url.'''
    if args.schema_owner:
        if url_fields.get('schema_owner') != args.schema_owner:
            return False
    if args.schema_version:
        if url_fields.get('schema_version') != args.schema_version:
            return False
    return True


def _matches_client_filters(
    data: dict, args: argparse.Namespace
) -> bool:
    '''Apply client-side filters to the message data dict.'''
    needs_url_parse: bool = any([
        args.schema_owner,
        args.schema_version,
    ])

    if needs_url_parse:
        data_url: str = data.get('data_url', '')
        url_fields: dict[str, str] = _parse_data_url(data_url)
        if not _matches_url_filters(url_fields, args):
            return False

    return True


async def _listen(args: argparse.Namespace) -> None:
    subscription: dict = _build_subscription(args)

    _LOGGER.info(f'Connecting to {args.url}')
    _LOGGER.debug(f'Subscription request: {subscription}')

    try:
        async with websockets.connect(args.url) as ws:
            await ws.send(orjson.dumps(subscription).decode())

            # First message is the subscription confirmation
            raw: str = await ws.recv()
            response: dict = orjson.loads(raw)

            if not response.get('success'):
                msg: str = response.get('message', 'unknown error')
                print(
                    f'Subscription failed: {msg}',
                    file=sys.stderr,
                )
                return

            print(
                f'Subscribed to channel: {response["channel"]}',
                file=sys.stderr,
            )

            async for raw_message in ws:
                try:
                    message: dict = orjson.loads(raw_message)
                except Exception:
                    _LOGGER.warning('Received non-JSON message, skipping')
                    continue

                data: dict = message.get('data', {})

                if not _matches_client_filters(data, args):
                    _LOGGER.debug(
                        'Message filtered out: '
                        f'{data.get("platform_content_id")}'
                    )
                    continue

                print(json.dumps(message, indent=2))
                sys.stdout.flush()

    except websockets.exceptions.ConnectionClosedError as exc:
        print(f'Connection closed: {exc}', file=sys.stderr)
    except OSError as exc:
        print(
            f'Could not connect to {args.url}: {exc}',
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> None:
    args: argparse.Namespace = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    try:
        asyncio.run(_listen(args))
    except KeyboardInterrupt:
        print('\nInterrupted.', file=sys.stderr)


if __name__ == '__main__':
    main()
