'''
Unit tests for scrape_exchange.youtube.innertube_orjson.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import unittest
from typing import Any
from unittest.mock import patch

import httpx
import orjson
from innertube.adaptor import InnerTubeAdaptor
from innertube.errors import RequestError, ResponseError
from innertube.models import ClientContext

import scrape_exchange.youtube  # noqa: F401  (installs the patch)
from scrape_exchange.youtube import innertube_orjson


def _adaptor(
    body: bytes, content_type: str = 'application/json; charset=UTF-8',
) -> InnerTubeAdaptor:
    '''An adaptor whose session answers every request with *body*.'''

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=body, headers={'Content-Type': content_type},
        )

    return InnerTubeAdaptor(
        ClientContext(client_name='WEB', client_version='2.0'),
        session=httpx.Client(
            base_url='https://localhost',
            transport=httpx.MockTransport(_handler),
        ),
    )


class TestInnerTubeOrjson(unittest.TestCase):

    def test_package_import_installs_dispatch(self) -> None:
        self.assertIs(
            InnerTubeAdaptor.dispatch, innertube_orjson._dispatch,
        )

    def test_decodes_with_orjson_and_sets_visitor_id(self) -> None:
        payload: dict[str, Any] = {
            'responseContext': {'visitorData': 'abc'},
            'contents': {'title': 'café \U0001f600'},
        }
        adaptor: InnerTubeAdaptor = _adaptor(orjson.dumps(payload))
        with patch.object(
            innertube_orjson.orjson, 'loads', wraps=orjson.loads,
        ) as loads:
            self.assertEqual(adaptor.dispatch('browse'), payload)
        loads.assert_called_once()
        self.assertEqual(
            adaptor.session.headers['X-Goog-Visitor-Id'], 'abc',
        )

    def test_error_response_raises_request_error(self) -> None:
        adaptor: InnerTubeAdaptor = _adaptor(orjson.dumps({
            'error': {
                'code': 400, 'message': 'bad', 'status': 'INVALID',
            },
        }))
        with self.assertRaises(RequestError):
            adaptor.dispatch('browse')

    def test_non_json_content_type_raises_response_error(self) -> None:
        adaptor: InnerTubeAdaptor = _adaptor(b'<html/>', 'text/html')
        with self.assertRaises(ResponseError):
            adaptor.dispatch('browse')

    def test_falls_back_for_bodies_orjson_rejects(self) -> None:
        big: int = 2 ** 70
        adaptor: InnerTubeAdaptor = _adaptor(
            json.dumps({'n': big}).encode(),
        )
        self.assertEqual(adaptor.dispatch('browse'), {'n': big})

    def test_invalid_json_still_raises(self) -> None:
        adaptor: InnerTubeAdaptor = _adaptor(b'{not json')
        with self.assertRaises(ValueError):
            adaptor.dispatch('browse')


if __name__ == '__main__':
    unittest.main()
