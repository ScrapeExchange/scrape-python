'''
Client-side schema fetch and JSON-Schema validation for scrapers.

A scraper constructs one :class:`SchemaValidator` per
``(owner, platform, entity, version)`` it uploads to, calls
:func:`fetch_schema_dict` once at startup to retrieve the JSON
Schema document from Scrape Exchange, and then calls
:meth:`SchemaValidator.validate` on every record before any
``POST /api/v1/data`` or bulk upload. Records that fail validation
must not be uploaded; callers are responsible for the surrounding
log + ``mark_invalid`` side effects so they can include their own
call-site context (filename, content_id, etc.).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
from collections.abc import Iterable
from logging import Logger
from typing import TYPE_CHECKING

import jsonschema_rs
from httpx2 import Response

if TYPE_CHECKING:
    from .exchange_client import ExchangeClient


_LOGGER: Logger = logging.getLogger(__name__)

_SCHEMA_PARAM_PATH: str = '/api/v1/schema/param'


class SchemaValidator:
    '''
    Compiled JSON-Schema validator for one
    ``(owner, platform, entity, version)`` tuple. Pure predicate;
    holds no I/O state.

    Records are validated with the Rust ``jsonschema-rs`` validator,
    which is two orders of magnitude faster than the pure-Python
    ``jsonschema`` one, which dominated the uploader's CPU time.
    '''

    def __init__(self, json_schema: dict) -> None:
        '''
        Compile *json_schema* for draft 2020-12. Raises
        ``jsonschema_rs.ValidationError`` if the schema itself is
        malformed; callers should let that propagate so the scraper
        fails fast at startup.
        '''
        self._validator: jsonschema_rs.Draft202012Validator = (
            jsonschema_rs.Draft202012Validator(json_schema)
        )

    def validate(self, data: dict) -> str | None:
        '''
        Return ``None`` when *data* validates. On failure, return a
        short human-readable string of the form
        ``"<json-pointer>: <message>"`` built from the first
        validation error (validators are deterministic in
        iteration order so this is stable across runs).

        A record holding a Python value that has no JSON equivalent
        (e.g. a ``datetime`` a model's ``to_dict()`` forgot to
        convert) fails validation: that is a model bug, so it is
        also logged as a warning.
        '''
        try:
            if self._validator.is_valid(data):
                return None
            for error in self._validator.iter_errors(data):
                return _format_error(error.instance_path, error.message)
            return None
        except ValueError as exc:
            _LOGGER.warning(
                'Record holds a value with no JSON equivalent; '
                'the model that built it did not convert it',
                extra={'error': str(exc)},
            )
            return _format_error((), f'non-JSON value in record: {exc}')


def _format_error(path: Iterable[str | int], message: str) -> str:
    pointer: str = '/' + '/'.join(str(p) for p in path)
    if pointer == '/':
        pointer = '<root>'
    return f'{pointer}: {message}'


async def fetch_schema_dict(
    client: 'ExchangeClient',
    exchange_url: str,
    schema_owner: str,
    platform: str,
    entity: str,
    version: str,
) -> dict:
    '''
    Fetch the JSON Schema document for one
    ``(owner, platform, entity, version)`` tuple via
    ``GET /api/v1/schema/param/{owner}/{platform}/{entity}/{version}``.

    Returns the bare ``json_schema`` dict from the response. Raises
    :class:`RuntimeError` on any non-200 response, on a missing
    ``json_schema`` field, or on transport failure (after the
    client's own retries are exhausted).
    '''
    url: str = (
        f'{exchange_url}{_SCHEMA_PARAM_PATH}'
        f'/{schema_owner}/{platform}/{entity}/{version}'
    )
    _LOGGER.info(
        'Fetching JSON schema for client-side validation',
        extra={
            'url': url,
            'schema_owner': schema_owner,
            'platform': platform,
            'entity': entity,
            'version': version,
        },
    )
    try:
        resp: Response = await client.get(url)
    except Exception as exc:
        raise RuntimeError(
            f'Failed to fetch schema {schema_owner}/{platform}/'
            f'{entity}/{version}: {exc}'
        ) from exc

    if resp.status_code != 200:
        raise RuntimeError(
            f'Schema fetch returned {resp.status_code} for '
            f'{schema_owner}/{platform}/{entity}/{version}: '
            f'{resp.text}'
        )

    body: dict = resp.json()
    json_schema: dict | None = body.get('json_schema')
    if not isinstance(json_schema, dict):
        raise RuntimeError(
            f'Schema fetch response missing ``json_schema`` field '
            f'for {schema_owner}/{platform}/{entity}/{version}'
        )
    return json_schema
