'''
Decode InnerTube responses with orjson.

The innertube library decodes every response with httpx's
``Response.json()``, which uses the standard library ``json`` module.
InnerTube browse responses are hundreds of kilobytes; a profile of the
channel scraper showed decoding them took about a third of a worker's
CPU. orjson decodes the same bytes several times faster.

:func:`install` replaces ``InnerTubeAdaptor.dispatch`` with a copy of
the library's method (innertube 2.1.19) that decodes with orjson. A
body orjson rejects (invalid UTF-8, integers beyond 64 bits) is decoded
by ``Response.json()`` instead, so such responses behave as before.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Any

import orjson
from httpx import Response
from innertube import api
from innertube.adaptor import InnerTubeAdaptor
from innertube.errors import RequestError, ResponseError


def decode_json(response: Response) -> Any:
    '''Decode the body of *response* with orjson, falling back to
    ``Response.json()`` for bodies orjson rejects.'''
    try:
        return orjson.loads(response.content)
    except orjson.JSONDecodeError:
        return response.json()


def _dispatch(
    self: InnerTubeAdaptor,
    endpoint: str,
    params: dict | None = None,
    body: dict | None = None,
) -> dict:
    '''``InnerTubeAdaptor.dispatch`` with orjson decoding.'''
    response: Response = self._request(endpoint, params=params, body=body)
    content_type: str | None = response.headers.get('Content-Type')
    if (
        content_type is not None
        and not content_type.lower().startswith('application/json')
    ):
        raise ResponseError(f'Expected JSON response, got {content_type!r}')
    response_data: dict = decode_json(response)
    visitor_data: str | None = response_data.get(
        'responseContext', {},
    ).get('visitorData')
    if visitor_data is not None:
        self.session.headers['X-Goog-Visitor-Id'] = visitor_data
    error: dict | None = response_data.get('error')
    if error is not None:
        raise RequestError(api.error(error))
    return response_data


def install() -> None:
    '''Make every InnerTube client in this process decode responses
    with orjson. Idempotent.'''
    InnerTubeAdaptor.dispatch = _dispatch
