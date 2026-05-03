#!/usr/bin/env python3

'''
Queries the Scrape.Exchange ``POST /api/v1/filter`` endpoint for
every record matching ``(platform, entity, username)`` and writes
the chosen identifier field (``platform_content_id`` by default,
``platform_creator_id`` opt-in) of each row, left-stripped of any
leading ``@``, to ``channel_handles.lst``, one per line. All four
filter inputs are configurable via pydantic-settings (env vars or
``.env``).

The endpoint requires no authentication. Pagination follows the
``page_info.end_cursor`` / ``page_info.has_next_page`` GraphQL-style
contract returned by ``QueryResponseModel``. Transient transport
errors and HTTP 5xx / 429 responses are retried with exponential
backoff; non-retryable HTTP errors abort the run.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import sys

from pathlib import Path
from typing import Any, Literal

from httpx import AsyncClient, HTTPError, Response, Timeout
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


FILTER_API_PATH: str = '/api/v1/filter'
DEFAULT_EXCHANGE_URL: str = 'https://scrape.exchange'
DEFAULT_PLATFORM: str = 'youtube'
DEFAULT_ENTITY: str = 'channel'
DEFAULT_RETURN_FIELD: str = 'platform_content_id'
DEFAULT_OUTPUT_FILE: str = 'out.lst'
PAGE_SIZE: int = 1000

ReturnField = Literal['platform_content_id', 'platform_creator_id']

_REQUEST_TIMEOUT: Timeout = Timeout(
    connect=30.0, read=600.0, write=30.0, pool=60.0,
)

MAX_ATTEMPTS: int = 6
RETRY_BASE_DELAY: float = 2.0


class ExportSettings(BaseSettings):
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_file='.env', extra='ignore',
        cli_parse_args=True,
        cli_kebab_case=True,
    )

    exchange_url: str = Field(
        default=DEFAULT_EXCHANGE_URL,
        description='Base URL of the Scrape.Exchange API.',
    )
    username: str = Field(
        default=None,
        description=(
            'Uploader username to filter on (the '
            'POST /api/v1/filter ``username`` field). '
            'Defaults to ``None``.'
        ),
    )
    platform: str = Field(
        default=DEFAULT_PLATFORM,
        description=(
            'Platform to filter on (the POST /api/v1/filter '
            '``platform`` field). Defaults to ``youtube``.'
        ),
    )
    entity: str = Field(
        default=DEFAULT_ENTITY,
        description=(
            'Entity type to filter on (the POST /api/v1/filter '
            '``entity`` field, e.g. ``channel``, ``video``). '
            'Defaults to ``channel``.'
        ),
    )
    return_field: ReturnField = Field(
        default=DEFAULT_RETURN_FIELD,
        aliases=['return-field', 'RETURN_FIELD'],
        description=(
            'Identifier field to extract from each row. Either '
            '``platform_content_id`` (channel id ``UC…`` for '
            'channel rows, video id for video rows; the default) '
            'or ``platform_creator_id`` (the channel @handle, '
            'left-stripped of ``@``).'
        ),
    )
    output_file: str = Field(
        default=DEFAULT_OUTPUT_FILE,
        aliases=['output-file', 'OUTPUT_FILE'],
        description=(
            'Path of the file to write the extracted values to '
            '(one value per line). Relative paths are resolved '
            'against the current working directory. Defaults to '
            '``out.lst``.'
        ),
    )


async def _post_filter_with_retry(
    client: AsyncClient, url: str, body: dict[str, Any],
) -> dict[str, Any]:
    '''
    POST to the filter endpoint, retrying transient failures
    (network error, HTTP 5xx, HTTP 429) with exponential backoff.
    Raises ``RuntimeError`` after :data:`MAX_ATTEMPTS` failures
    or on any non-retryable HTTP error (4xx other than 429).
    '''
    last_err: str = ''
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp: Response = await client.post(
                url, json=body, timeout=_REQUEST_TIMEOUT,
            )
        except HTTPError as exc:
            last_err = f'transport error: {type(exc).__name__}: {exc}'
        else:
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code < 500 and resp.status_code != 429:
                raise RuntimeError(
                    f'filter request failed: '
                    f'status={resp.status_code} '
                    f'body={resp.text[:500]}'
                )
            last_err = (
                f'status={resp.status_code} body={resp.text[:200]}'
            )

        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(
                f'filter request failed after {MAX_ATTEMPTS} '
                f'attempts: {last_err}'
            )
        delay: float = RETRY_BASE_DELAY * (2 ** (attempt - 1))
        print(
            f'  attempt={attempt} transient failure ({last_err}); '
            f'retrying in {delay:.1f}s'
        )
        await asyncio.sleep(delay)
    raise RuntimeError('unreachable')


async def fetch_field_values(
    client: AsyncClient, exchange_url: str,
    username: str, platform: str, entity: str,
    return_field: ReturnField,
) -> list[str]:
    '''
    Page through ``POST /api/v1/filter`` for every record matching
    ``(platform, entity, username)`` and return the ordered list
    of unique values of *return_field* on each row, with any
    leading ``@`` left-stripped. Rows for which *return_field* is
    null/empty are skipped.
    '''
    url: str = f'{exchange_url}{FILTER_API_PATH}'
    seen: set[str] = set()
    ordered: list[str] = []
    skipped_no_value: int = 0
    after: str | None = None
    page: int = 0

    while True:
        body: dict[str, Any] = {
            'username': username,
            'platform': platform,
            'entity': entity,
            'first': PAGE_SIZE,
        }
        if after is not None:
            body['after'] = after

        payload: dict[str, Any] = await _post_filter_with_retry(
            client, url, body,
        )
        edges: list[dict[str, Any]] = payload.get('edges', [])
        page += 1

        for edge in edges:
            node: dict[str, Any] = edge.get('node') or {}
            raw: str | None = node.get(return_field)
            if not raw:
                skipped_no_value += 1
                continue
            value: str = raw.lstrip('@')
            if not value:
                skipped_no_value += 1
                continue
            if value not in seen:
                seen.add(value)
                ordered.append(value)

        page_info: dict[str, Any] = payload.get('page_info') or {}
        total: int = payload.get('total_count', 0)
        print(
            f'  page={page} fetched={len(edges)} '
            f'unique_so_far={len(ordered)} '
            f'skipped_no_value={skipped_no_value} '
            f'total_count={total}'
        )

        if not page_info.get('has_next_page'):
            break
        after = page_info.get('end_cursor')
        if not after:
            break

    return ordered


def write_lines(path: Path, lines: list[str]) -> None:
    path.write_text(''.join(f'{line}\n' for line in lines))


async def main() -> int:
    settings: ExportSettings = ExportSettings()
    print(f'Using exchange URL: {settings.exchange_url}')

    out: Path = Path(settings.output_file)

    async with AsyncClient(trust_env=False) as client:
        print(
            f'Fetching {settings.platform}/{settings.entity} '
            f'records uploaded by {settings.username!r}; '
            f'extracting {settings.return_field}...'
        )
        values: list[str] = await fetch_field_values(
            client, settings.exchange_url,
            settings.username, settings.platform,
            settings.entity, settings.return_field,
        )
        write_lines(out, values)
        print(f'Wrote {len(values)} values to {out}.')

    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
