#!/usr/bin/env python3

'''
Review scraped YouTube channel/video files in a directory against
their JSON schemas.

Reads ``schema_owner`` + ``schema_version`` from settings (CLI /
env / .env), downloads the channel and video schemas from Scrape
Exchange via ``GET /api/v1/schema/param/...``, then iterates over
every file in the configured directory whose name starts with
``channel-`` or ``video-``, parses it (handles brotli-compressed
``.json.br`` and plain ``.json``), validates against the matching
schema, and prints one line per file:

    <filename>: success
    <filename>: failure: <reason>

The ``<reason>`` is either a parse-time error (decompression /
JSON parse / read failure) or the JSON-pointer-prefixed
validation error from
:class:`scrape_exchange.schema_validator.SchemaValidator`.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import sys
import asyncio
import logging

from pathlib import Path

import brotli
import orjson

from pydantic import AliasChoices, Field

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.logging import configure_logging
from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)
from scrape_exchange.settings import ScraperSettings


_LOGGER: logging.Logger = logging.getLogger(__name__)


_CHANNEL_PREFIX: str = 'channel-'
_VIDEO_PREFIX: str = 'video-'


class ReviewSettings(ScraperSettings):
    '''Settings for the YouTube schema-review tool.'''

    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'SCHEMA_OWNER', 'schema_owner',
        ),
        description=(
            'Owner of the JSON schemas to download for review '
            '(typically the same value the scrapers use as '
            '``SCHEMA_OWNER``).'
        ),
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version',
        ),
        description=(
            'Schema version string. Used for both the channel '
            'and video schema fetches.'
        ),
    )
    directory: str = Field(
        default='.',
        validation_alias=AliasChoices('DIRECTORY', 'directory'),
        description=(
            'Directory whose ``channel-*`` and ``video-*`` files '
            'should be reviewed. Defaults to the current working '
            'directory.'
        ),
    )
    limit: int = Field(
        default=10,
        validation_alias=AliasChoices('LIMIT', 'limit'),
        description=(
            'Maximum number of matching files to review. Use 0 '
            'to disable the cap and review every file in the '
            'directory. Defaults to 10.'
        ),
    )


def _read_data_file(path: Path) -> dict:
    '''
    Read and decode a scraped data file. Supports brotli-
    compressed ``.json.br`` (the on-disk format the scrapers
    produce) and plain ``.json``. Anything else raises
    :class:`ValueError` so the caller can surface "unsupported
    extension" as the failure reason.
    '''
    name: str = path.name
    raw: bytes = path.read_bytes()
    if name.endswith('.json.br'):
        decompressed: bytes = brotli.decompress(raw)
        return orjson.loads(decompressed)
    if name.endswith('.json'):
        return orjson.loads(raw)
    raise ValueError(
        f'unsupported extension (need .json or .json.br): {name}'
    )


def _select_validator(
    filename: str,
    channel_validator: SchemaValidator,
    video_validator: SchemaValidator,
) -> SchemaValidator | None:
    '''
    Return the validator that matches *filename*'s prefix, or
    ``None`` when the prefix is unrecognised.
    '''
    if filename.startswith(_CHANNEL_PREFIX):
        return channel_validator
    if filename.startswith(_VIDEO_PREFIX):
        return video_validator
    return None


def review_file(
    path: Path,
    channel_validator: SchemaValidator,
    video_validator: SchemaValidator,
) -> str:
    '''
    Validate one file against its matching schema and return the
    one-line outcome string. Pure function over filesystem +
    validators so it can be unit-tested without spinning up an
    HTTP client.
    '''
    validator: SchemaValidator | None = _select_validator(
        path.name, channel_validator, video_validator,
    )
    if validator is None:
        return f'{path.name}: skipped: prefix not recognised'

    try:
        data: dict = _read_data_file(path)
    except brotli.error as exc:
        return (
            f'{path.name}: failure: '
            f'brotli decompression failed: {exc}'
        )
    except orjson.JSONDecodeError as exc:
        return f'{path.name}: failure: json parse failed: {exc}'
    except OSError as exc:
        return f'{path.name}: failure: read failed: {exc}'
    except ValueError as exc:
        return f'{path.name}: failure: {exc}'

    err: str | None = validator.validate(data)
    if err is None:
        return f'{path.name}: success'
    return f'{path.name}: failure: {err}'


def _list_targets(directory: Path) -> list[Path]:
    '''
    Return the files in *directory* whose name starts with
    ``channel-`` or ``video-``, sorted for deterministic output.
    Skips subdirectories silently.
    '''
    targets: list[Path] = []
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        if (
            entry.name.startswith(_CHANNEL_PREFIX)
            or entry.name.startswith(_VIDEO_PREFIX)
        ):
            targets.append(entry)
    return targets


async def main() -> int:
    settings: ReviewSettings = ReviewSettings()

    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )

    directory: Path = Path(settings.directory)
    if not directory.is_dir():
        _LOGGER.error(
            'Directory not found',
            extra={'directory': str(directory)},
        )
        return 1

    # The schema GET endpoint is unauthenticated, so no JWT setup
    # is required; construct a bare ExchangeClient and skip the
    # token round-trip.
    client: ExchangeClient = ExchangeClient(
        exchange_url=settings.exchange_url,
    )
    try:
        channel_schema_dict: dict = await fetch_schema_dict(
            client, settings.exchange_url,
            settings.schema_owner, 'youtube', 'channel',
            settings.schema_version,
        )
        video_schema_dict: dict = await fetch_schema_dict(
            client, settings.exchange_url,
            settings.schema_owner, 'youtube', 'video',
            settings.schema_version,
        )
    finally:
        await client.aclose()

    channel_validator: SchemaValidator = SchemaValidator(
        channel_schema_dict,
    )
    video_validator: SchemaValidator = SchemaValidator(
        video_schema_dict,
    )

    targets: list[Path] = _list_targets(directory)
    if settings.limit > 0:
        targets = targets[: settings.limit]
    for target in targets:
        line: str = review_file(
            target, channel_validator, video_validator,
        )
        print(line)
        sys.stdout.flush()

    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
