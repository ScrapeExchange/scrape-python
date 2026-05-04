#!/usr/bin/env python3

'''
Bulk-delete records on Scrape Exchange via the
``DELETE /api/v1/filter`` endpoint.

Filter criteria are read from settings (CLI flags / env vars / .env)
and capped by ``max_items``. Defaults to a dry-run preview; pass
``--confirm`` to actually delete.

Examples:

    # Preview every record this account uploaded under the boinko
    # YouTube channel schema (capped at 100 matches):
    python tools/yt_delete_items.py \\
        --schema-owner=boinko --platform=youtube --entity=channel

    # Same query, actually delete:
    python tools/yt_delete_items.py \\
        --schema-owner=boinko --platform=youtube --entity=channel \\
        --confirm

    # Delete a specific channel's records by content id:
    python tools/yt_delete_items.py \\
        --platform=youtube --entity=channel \\
        --platform-content-id=UC1234567890abcdefghij \\
        --confirm

Authentication uses the ``api_key_id`` / ``api_key_secret`` pair
inherited from :class:`ScraperSettings`. The DELETE endpoint scopes
matches to records the authenticated account uploaded, so an empty
filter cannot affect another account's data; it can still match
many records, which is what ``max_items`` is for.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import sys

from httpx import Response, Timeout
from pydantic import AliasChoices, Field

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import ScraperSettings


_LOGGER: logging.Logger = logging.getLogger(__name__)


FILTER_API_PATH: str = '/api/v1/filter'

# Server-side hard ceiling. Mirrors
# ``ABSOLUTE_DELETE_MAX_RECORDS`` in
# ``../scrape-api/server/routers/filter.py``.
ABSOLUTE_MAX_ITEMS: int = 100_000

# httpx defaults to a 5 s read timeout, which is shorter than the
# server takes to count + list (and optionally delete) tens of
# thousands of records. Use a generous cap so a single
# ``--max-items 100000`` call can complete; the API itself caps the
# work at ABSOLUTE_MAX_ITEMS, so the wait is bounded.
_DELETE_TIMEOUT: Timeout = Timeout(
    connect=30.0, read=600.0, write=30.0, pool=60.0,
)


class DeleteItemsSettings(ScraperSettings):
    '''
    Filter and safety settings for the bulk-delete tool. All
    filter fields are optional; the server further scopes the
    query to records uploaded by the authenticated account.
    '''

    schema_owner: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'SCHEMA_OWNER', 'schema_owner',
        ),
        description=(
            'Owner of the schema the records were posted under '
            '(e.g. ``boinko``). Maps to '
            '``DeleteFilterRequestModel.schema_owner``.'
        ),
    )
    platform: str | None = Field(
        default=None,
        validation_alias=AliasChoices('PLATFORM', 'platform'),
        description=(
            'Platform name to scope the delete to '
            '(e.g. ``youtube``).'
        ),
    )
    entity: str | None = Field(
        default=None,
        validation_alias=AliasChoices('ENTITY', 'entity'),
        description=(
            'Entity type within the platform '
            '(e.g. ``channel``, ``video``).'
        ),
    )
    version: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version', 'version',
        ),
        description=(
            'Schema version string (e.g. ``0.0.2``).'
        ),
    )
    platform_content_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'PLATFORM_CONTENT_ID', 'platform_content_id',
        ),
        description=(
            'Match a specific record by its '
            '``platform_content_id`` (e.g. ``UC...`` for a YouTube '
            'channel, the video id for a YouTube video).'
        ),
    )
    platform_creator_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'PLATFORM_CREATOR_ID', 'platform_creator_id',
        ),
        description=(
            'Match all records by ``platform_creator_id`` '
            '(YouTube: the channel handle).'
        ),
    )
    platform_topic_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'PLATFORM_TOPIC_ID', 'platform_topic_id',
        ),
        description=(
            'Match all records by ``platform_topic_id``.'
        ),
    )
    max_items: int = Field(
        default=100,
        ge=1,
        le=ABSOLUTE_MAX_ITEMS,
        validation_alias=AliasChoices(
            'MAX_ITEMS', 'max_items',
        ),
        description=(
            'Maximum number of records the server will accept '
            'for one DELETE call. The server returns 422 '
            '``DELETE_BATCH_TOO_LARGE`` when the filter matches '
            'more rows than this. Hard ceiling enforced server-'
            f'side: {ABSOLUTE_MAX_ITEMS}.'
        ),
    )
    confirm: bool = Field(
        default=False,
        validation_alias=AliasChoices('CONFIRM', 'confirm'),
        description=(
            'When False (default), preview the delete without '
            'removing anything. Pass ``--confirm`` to actually '
            'delete the matched records.'
        ),
    )
    deleted_item_ids_file: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'DELETED_ITEM_IDS_FILE', 'deleted_item_ids_file',
        ),
        description=(
            'When set, opt in to ``include_item_ids=true`` on '
            'the DELETE call so the server returns each '
            'matched record id, and append those ids to this '
            'file, one per line. Both preview and ``--confirm`` '
            'runs append (preview captures the ids that *would* '
            'be deleted). The file is opened in append mode so '
            'repeated runs accumulate.'
        ),
    )


def _build_filter_body(
    settings: DeleteItemsSettings,
) -> dict[str, str]:
    '''
    Translate the filter-related fields on *settings* into the
    JSON body the server's ``DeleteFilterRequestModel`` expects.
    Only keys with non-``None`` values are included so the
    server-side ``if filters.X`` checks behave the same as if
    the field were omitted.
    '''
    filter_fields: tuple[str, ...] = (
        'schema_owner', 'platform', 'entity', 'version',
        'platform_content_id', 'platform_creator_id',
        'platform_topic_id',
    )
    body: dict[str, str] = {}
    for name in filter_fields:
        value: str | None = getattr(settings, name)
        if value is not None:
            body[name] = value
    return body


async def delete_items(
    settings: DeleteItemsSettings,
    client: ExchangeClient,
) -> int:
    '''
    Issue one DELETE call against the ``/api/v1/filter`` endpoint
    using the filter and safety settings on *settings*. Logs and
    prints a summary of the response. Returns the process exit
    code (0 on success, non-zero on a request-level failure).
    '''
    
    body: dict[str, str] = _build_filter_body(settings)
    if not body and not settings.confirm:
        logging.info(
            'No filter fields set; preview will match every '
            'record this account uploaded',
        )

    url: str = f'{settings.exchange_url}{FILTER_API_PATH}'
    params: dict[str, str] = {
        'confirm': 'true' if settings.confirm else 'false',
        'max_records': str(settings.max_items),
    }
    if settings.deleted_item_ids_file is not None:
        params['include_item_ids'] = 'true'
    logging.info(
        'Requesting bulk delete',
        extra={
            'url': url,
            'filters': body,
            'confirm': settings.confirm,
            'max_items': settings.max_items,
        },
    )

    try:
        resp: Response = await client.request(
            'DELETE', url, json=body, params=params,
            timeout=_DELETE_TIMEOUT,
        )
    except Exception as exc:
        logging.error(
            'DELETE request failed', exc=exc,
            extra={'url': url},
        )
        return 1

    if resp.status_code != 200:
        logging.error(
            'DELETE request rejected',
            extra={
                'status_code': resp.status_code,
                'response_text': resp.text,
            },
        )
        return 1

    payload: dict = resp.json()
    deleted: int = payload.get('deleted', 0)
    would_delete: int = payload.get('would_delete', 0)
    item_ids: list[str] = payload.get('item_ids', [])

    if settings.confirm:
        logging.info(
            'Bulk delete complete',
            extra={
                'deleted': deleted,
                'matched': would_delete,
                'item_count': len(item_ids),
            },
        )
        print(f'Deleted {deleted} records.')
    else:
        logging.info(
            'Bulk delete preview',
            extra={
                'would_delete': would_delete,
                'item_count': len(item_ids),
            },
        )
        print(
            f'Preview: {would_delete} records would be deleted. '
            'Re-run with --confirm to actually delete.'
        )

    if item_ids:
        sample: list[str] = item_ids[:10]
        print('Sample item_ids:')
        for item_id in sample:
            print(f'  {item_id}')
        if len(item_ids) > len(sample):
            print(f'  ... ({len(item_ids) - len(sample)} more)')

    if (
        settings.deleted_item_ids_file is not None
        and item_ids
    ):
        with open(
            settings.deleted_item_ids_file, 'a',
            encoding='utf-8',
        ) as f:
            for item_id in item_ids:
                f.write(f'{item_id}\n')
        logging.info(
            'Appended item_ids to file',
            extra={
                'file': settings.deleted_item_ids_file,
                'count': len(item_ids),
                'confirm': settings.confirm,
            },
        )

    return 0


async def main() -> int:
    settings: DeleteItemsSettings = DeleteItemsSettings()

    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )

    if not settings.api_key_id or not settings.api_key_secret:
        logging.error(
            'API_KEY_ID and API_KEY_SECRET must be set in the '
            'environment / .env file',
        )
        return 1

    client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )
    try:
        return await delete_items(settings, client)
    finally:
        await client.aclose()


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
