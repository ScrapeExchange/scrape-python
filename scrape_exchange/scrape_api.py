'''Typed Python wrappers over the scrape.exchange Data + Filter API.

The Scrape Exchange API exposes ``GET /api/v1/data/...`` for direct
record lookup and ``POST /api/v1/filter`` for query-style retrieval.
Both return paginated JSON via :class:`QueryResponseModel`. This
module:

* Copies the pagination + response models from
  ``../scrape-api/server/datatypes.py`` /
  ``../scrape-api/server/models/data.py`` so callers can deserialise
  responses without a hard dependency on the server package.
* Provides async wrappers around each endpoint that use the
  existing :class:`scrape_exchange.exchange_client.ExchangeClient`
  for auth / retry / rate-limiting.
* Provides materialisers that download a record's ``data_url``
  (brotli-compressed JSON) and decode it into the appropriate
  :class:`scrape_exchange.youtube.youtube_channel.YouTubeChannel`
  or :class:`scrape_exchange.youtube.youtube_video.YouTubeVideo`.
* Provides async iterators that paginate transparently and yield
  either raw :class:`GetDataResponseModel`s or hydrated YouTube
  channel / video instances.

Keep the model definitions in sync with the server side by hand;
the runtime contract is just "matching field names". Optional
fields are typed accordingly so a slightly older server response
still validates.
'''

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Generic, TypeVar
from uuid import UUID

import brotli
import orjson
from pydantic import BaseModel, Field

from .brotli import _best_effort_decompress
from .datatypes import Platform
from .exchange_client import ExchangeClient
from .youtube.youtube_channel import YouTubeChannel
from .youtube.youtube_video import YouTubeVideo


_API_PREFIX: str = '/api/v1'
_DEFAULT_PAGE_SIZE: int = 100
_MAX_PAGE_SIZE: int = 1000


def _platform_path_component(platform: Platform | str) -> str:
    if isinstance(platform, Platform):
        return platform.value
    return platform


# ---------------------------------------------------------------
# Pagination models (mirror server/datatypes.py)
# ---------------------------------------------------------------


TypeX = TypeVar('TypeX')


class EdgeResponse(BaseModel, Generic[TypeX]):
    '''One page entry: an opaque ``cursor`` plus the typed ``node``.'''

    cursor: str
    node: TypeX


class PageInfoResponse(BaseModel):
    '''Cursor metadata for the next page.'''

    has_next_page: bool
    end_cursor: str | None


class QueryResponseModel(BaseModel, Generic[TypeX]):
    '''Generic paginated query response: ``edges`` + ``page_info``.

    ``total_count`` semantics differ by endpoint -- the path
    handlers in scrape-api set it to the page size for the
    ``/data/param/...`` routes and to the full match count for
    ``/filter``. Treat it as informational, not load-bearing.
    '''

    total_count: int
    edges: list[EdgeResponse[TypeX]]
    page_info: PageInfoResponse


# ---------------------------------------------------------------
# Record + filter models (mirror server/models/data.py)
# ---------------------------------------------------------------


class GetDataResponseModel(BaseModel):
    '''Index record describing one uploaded data item.

    ``data_url`` is the location of the brotli-compressed JSON
    payload on files.scrape.exchange; use
    :func:`fetch_node` (or :func:`fetch_youtube_channel` /
    :func:`fetch_youtube_video`) to materialise it.
    '''

    item_id: UUID
    username: str
    schema_username: str
    platform: Platform
    entity: str
    version: str
    platform_content_id: str
    platform_creator_id: str | None = None
    platform_topic_id: str | None = None
    source_url: str
    last_modified_timestamp: datetime
    created_timestamp: datetime
    data_url: str
    platform_content_thumbnail_url: str | None = None
    platform_creator_thumbnail_url: str | None = None
    platform_topic_thumbnail_url: str | None = None


class PostFilterRequestModel(BaseModel):
    '''Request body for ``POST /api/v1/filter``.

    All fields optional; the server matches records that satisfy
    every non-``None`` field. ``after`` / ``first`` drive cursor
    pagination (page size capped server-side at 1000).
    '''

    schema_username: str | None = None
    username: str | None = None
    platform: Platform | None = None
    entity: str | None = None
    version: str | None = None
    platform_content_id: str | None = None
    platform_creator_id: str | None = None
    platform_topic_id: str | None = None
    after: str | None = None
    first: int = Field(default=_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE)


# ---------------------------------------------------------------
# Endpoint wrappers
# ---------------------------------------------------------------


async def get_data_by_item_id(
    client: ExchangeClient, item_id: UUID | str,
) -> GetDataResponseModel:
    '''``GET /api/v1/data/item_id/{item_id}`` -> one record.

    :raises httpx.HTTPStatusError: On non-2xx (e.g. 404).
    '''
    response = await client.get(
        f'{client.exchange_url.rstrip("/")}{_API_PREFIX}/data/item_id/{item_id}',
    )
    response.raise_for_status()
    return GetDataResponseModel.model_validate(response.json())


async def get_data_by_param(
    client: ExchangeClient,
    *,
    username: str,
    platform: Platform | str,
    entity: str,
    version: str,
    platform_content_id: str,
) -> GetDataResponseModel:
    '''``GET /api/v1/data/param/.../{platform_content_id}`` -> one record.

    Full-key lookup; raises on 404 (record not found for that
    combination).
    '''
    platform_value: str = _platform_path_component(platform)
    url: str = (
        f'{client.exchange_url.rstrip("/")}{_API_PREFIX}/data/param/'
        f'{username}/{platform_value}/{entity}/{version}/'
        f'{platform_content_id}'
    )
    response = await client.get(url)
    response.raise_for_status()
    return GetDataResponseModel.model_validate(response.json())


async def get_data_by_username_platform_entity(
    client: ExchangeClient,
    *,
    username: str,
    platform: Platform | str,
    entity: str,
    version: str | None = None,
    after: str | None = None,
    first: int = _DEFAULT_PAGE_SIZE,
) -> QueryResponseModel[GetDataResponseModel]:
    '''``GET /api/v1/data/param/{username}/{platform}/{entity}[/{version}]``.

    Returns one page. To iterate, use
    :func:`iter_data_by_username_platform_entity` instead.
    '''
    suffix: str = (
        f'/{version}' if version is not None else ''
    )
    platform_value: str = _platform_path_component(platform)
    url: str = (
        f'{client.exchange_url.rstrip("/")}{_API_PREFIX}/data/param/'
        f'{username}/{platform_value}/{entity}{suffix}'
    )
    params: dict[str, str] = {'first': str(first)}
    if after is not None:
        params['after'] = after
    response = await client.get(url, params=params)
    response.raise_for_status()
    return QueryResponseModel[GetDataResponseModel].model_validate(
        response.json(),
    )


async def get_data_by_content(
    client: ExchangeClient,
    *,
    platform: Platform | str,
    entity: str,
    platform_content_id: str,
    after: str | None = None,
    first: int = _DEFAULT_PAGE_SIZE,
) -> QueryResponseModel[GetDataResponseModel]:
    '''``GET /api/v1/data/content/{platform}/{entity}/{platform_content_id}``.

    Returns all uploaders' records for one content item.
    '''
    platform_value: str = _platform_path_component(platform)
    url: str = (
        f'{client.exchange_url.rstrip("/")}{_API_PREFIX}/data/content/'
        f'{platform_value}/{entity}/{platform_content_id}'
    )
    params: dict[str, str] = {'first': str(first)}
    if after is not None:
        params['after'] = after
    response = await client.get(url, params=params)
    response.raise_for_status()
    return QueryResponseModel[GetDataResponseModel].model_validate(
        response.json(),
    )


async def filter_data(
    client: ExchangeClient,
    filters: PostFilterRequestModel | None = None,
) -> QueryResponseModel[GetDataResponseModel]:
    '''``POST /api/v1/filter`` -> paginated matches.

    Pass an empty :class:`PostFilterRequestModel` to walk every
    record. To iterate transparently across pages use
    :func:`iter_filter_data`.
    '''
    if filters is None:
        filters = PostFilterRequestModel()
    response = await client.post(
        f'{client.exchange_url.rstrip("/")}{_API_PREFIX}/filter',
        json=filters.model_dump(
            mode='json', exclude_none=True,
        ),
    )
    response.raise_for_status()
    return QueryResponseModel[GetDataResponseModel].model_validate(
        response.json(),
    )


# ---------------------------------------------------------------
# Async iterators (paginate transparently)
# ---------------------------------------------------------------


async def iter_filter_data(
    client: ExchangeClient,
    filters: PostFilterRequestModel | None = None,
) -> AsyncIterator[GetDataResponseModel]:
    '''Yield every ``GetDataResponseModel`` matching *filters*.

    Walks the cursor pagination until ``has_next_page`` is
    False. The page size from *filters* (default 100) governs the
    request batching.
    '''
    base: PostFilterRequestModel = (
        filters.model_copy(deep=True) if filters
        else PostFilterRequestModel()
    )
    while True:
        page: QueryResponseModel[GetDataResponseModel] = (
            await filter_data(client, base)
        )
        for edge in page.edges:
            yield edge.node
        if not page.page_info.has_next_page:
            return
        base.after = page.page_info.end_cursor


async def iter_data_by_username_platform_entity(
    client: ExchangeClient,
    *,
    username: str,
    platform: Platform | str,
    entity: str,
    version: str | None = None,
    first: int = _DEFAULT_PAGE_SIZE,
) -> AsyncIterator[GetDataResponseModel]:
    '''Paginated walk of the ``/data/param/.../`` GET endpoint.'''
    after: str | None = None
    while True:
        page: QueryResponseModel[GetDataResponseModel] = (
            await get_data_by_username_platform_entity(
                client,
                username=username,
                platform=platform,
                entity=entity,
                version=version,
                after=after,
                first=first,
            )
        )
        for edge in page.edges:
            yield edge.node
        if not page.page_info.has_next_page:
            return
        after = page.page_info.end_cursor


# ---------------------------------------------------------------
# Materialisers: index record -> typed model
# ---------------------------------------------------------------


async def fetch_dict_url(
    client: ExchangeClient, url: str,
) -> dict[str, Any]:
    '''GET *url* and parse the body as a JSON dict.

    Decoding precedence:

    1. Full ``brotli.decompress`` -> ``orjson.loads`` (live
       scraper format: a static brotli-wrapped JSON blob).
    2. Salvage via :func:`_best_effort_decompress` on
       partially-truncated brotli streams.
    3. Plain ``json.loads`` for older uploads that landed
       on disk without the brotli wrapper.

    Useful for callers that have a ``data_url`` string but no
    full :class:`GetDataResponseModel` to pass to
    :func:`_fetch_data_dict`. Relative URLs are resolved against
    :attr:`ExchangeClient.exchange_url`.
    '''
    if not url.startswith(('http://', 'https://')):
        url = (
            f'{client.exchange_url.rstrip("/")}/'
            f'{url.lstrip("/")}'
        )
    response = await client.get(url)
    response.raise_for_status()
    raw: bytes = response.content
    try:
        decompressed: bytes = brotli.decompress(raw)
    except brotli.error:
        decompressed = _best_effort_decompress(raw) or raw
    try:
        data: Any = orjson.loads(decompressed)
    except orjson.JSONDecodeError:
        data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(
            f'data_url {url} returned a '
            f'{type(data).__name__}, not a JSON object',
        )
    return data


async def _fetch_data_dict(
    client: ExchangeClient, entry: GetDataResponseModel,
) -> dict[str, Any]:
    '''Thin wrapper around :func:`fetch_dict_url` for callers
    holding a typed :class:`GetDataResponseModel`.'''
    return await fetch_dict_url(client, entry.data_url)


async def fetch_youtube_channel(
    client: ExchangeClient, entry: GetDataResponseModel,
) -> YouTubeChannel:
    '''Download and deserialise *entry* as a :class:`YouTubeChannel`.

    Asserts *entry*'s platform is YouTube and entity is "channel".
    '''
    if entry.platform != Platform.YOUTUBE or entry.entity != 'channel':
        raise ValueError(
            f'entry is not a YouTube channel '
            f'(platform={entry.platform!r}, entity={entry.entity!r})',
        )
    data: dict[str, Any] = await _fetch_data_dict(client, entry)
    return YouTubeChannel.from_dict(
        data, with_download_client=False,
    )


async def fetch_youtube_video(
    client: ExchangeClient, entry: GetDataResponseModel,
) -> YouTubeVideo:
    '''Download and deserialise *entry* as a :class:`YouTubeVideo`.

    Asserts *entry*'s platform is YouTube and entity is "video".
    '''
    if entry.platform != Platform.YOUTUBE or entry.entity != 'video':
        raise ValueError(
            f'entry is not a YouTube video '
            f'(platform={entry.platform!r}, entity={entry.entity!r})',
        )
    data: dict[str, Any] = await _fetch_data_dict(client, entry)
    return YouTubeVideo.from_dict(data)


async def fetch_node(
    client: ExchangeClient, entry: GetDataResponseModel,
) -> YouTubeChannel | YouTubeVideo | dict[str, Any]:
    '''Dispatch *entry* to the appropriate typed materialiser.

    Falls back to returning the raw dict when the
    (platform, entity) pair has no typed model.
    '''
    if entry.platform == Platform.YOUTUBE:
        if entry.entity == 'channel':
            return await fetch_youtube_channel(client, entry)
        if entry.entity == 'video':
            return await fetch_youtube_video(client, entry)
    return await _fetch_data_dict(client, entry)


# ---------------------------------------------------------------
# Convenience: iterate YouTube models directly
# ---------------------------------------------------------------


async def iter_youtube_channels(
    client: ExchangeClient,
    *,
    username: str | None = None,
    after: str | None = None,
    first: int = _DEFAULT_PAGE_SIZE,
) -> AsyncIterator[YouTubeChannel]:
    '''Yield every YouTube channel that matches the filter.

    Uses ``POST /api/v1/filter`` under the hood -- the channel
    side ID payload is downloaded for each edge.
    '''
    filters: PostFilterRequestModel = PostFilterRequestModel(
        platform=Platform.YOUTUBE,
        entity='channel',
        username=username,
        after=after,
        first=first,
    )
    async for entry in iter_filter_data(client, filters):
        yield await fetch_youtube_channel(client, entry)


async def iter_youtube_videos(
    client: ExchangeClient,
    *,
    username: str | None = None,
    after: str | None = None,
    first: int = _DEFAULT_PAGE_SIZE,
) -> AsyncIterator[YouTubeVideo]:
    '''Yield every YouTube video that matches the filter.'''
    filters: PostFilterRequestModel = PostFilterRequestModel(
        platform=Platform.YOUTUBE,
        entity='video',
        username=username,
        after=after,
        first=first,
    )
    async for entry in iter_filter_data(client, filters):
        yield await fetch_youtube_video(client, entry)
