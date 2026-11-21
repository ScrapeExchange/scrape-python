'''
Unit tests for ``scrape_exchange.schema_validator``.
'''

import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import jsonschema_rs

from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)

_SIMPLE_SCHEMA: dict = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object',
    'properties': {
        'channel_id': {'type': 'string'},
        'channel_handle': {'type': 'string'},
        'url': {'type': 'string', 'format': 'uri'},
        'subscriber_count': {'type': ['integer', 'null']},
    },
    'required': ['channel_id', 'channel_handle', 'url'],
    'additionalProperties': False,
}


class TestSchemaValidatorValidate(unittest.TestCase):

    def test_valid_record_returns_none(self) -> None:
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_id': 'UC1234567890abcdefghij',
            'channel_handle': 'somehandle',
            'url': 'https://www.youtube.com/@somehandle',
            'subscriber_count': 1000,
        }
        self.assertIsNone(validator.validate(record))

    def test_missing_required_field_returns_pointer_message(
        self,
    ) -> None:
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_handle': 'somehandle',
            'url': 'https://www.youtube.com/@somehandle',
        }
        msg: str | None = validator.validate(record)
        self.assertIsNotNone(msg)
        self.assertIn('channel_id', msg)
        self.assertIn(': ', msg)

    def test_type_mismatch_returns_pointer_message(self) -> None:
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_id': 'UC1234567890abcdefghij',
            'channel_handle': 'somehandle',
            'url': 'https://www.youtube.com/@somehandle',
            'subscriber_count': 'not-an-integer',
        }
        msg: str | None = validator.validate(record)
        self.assertIsNotNone(msg)
        self.assertIn('/subscriber_count', msg)

    def test_extra_property_with_additional_properties_false(
        self,
    ) -> None:
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_id': 'UC1234567890abcdefghij',
            'channel_handle': 'somehandle',
            'url': 'https://www.youtube.com/@somehandle',
            'unknown_field': 'unexpected',
        }
        msg: str | None = validator.validate(record)
        self.assertIsNotNone(msg)
        self.assertIn('unknown_field', msg)

    def test_root_error_uses_root_pointer(self) -> None:
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        msg: str | None = validator.validate({})
        self.assertIsNotNone(msg)
        self.assertTrue(msg.startswith('<root>: '), msg)

    def test_nested_error_pointer(self) -> None:
        schema: dict = {
            '$schema': 'https://json-schema.org/draft/2020-12/schema',
            'type': 'object',
            'properties': {
                'items': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {'n': {'type': 'integer'}},
                    },
                },
            },
        }
        validator: SchemaValidator = SchemaValidator(schema)
        msg: str | None = validator.validate(
            {'items': [{'n': 1}, {'n': 'x'}]},
        )
        self.assertIsNotNone(msg)
        self.assertTrue(msg.startswith('/items/1/n: '), msg)

    def test_formats_are_not_asserted(self) -> None:
        '''Draft 2020-12 treats ``format`` as an annotation; the
        previous validator did not check it either.'''
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_id': 'UC1234567890abcdefghij',
            'channel_handle': 'somehandle',
            'url': 'not a uri',
        }
        self.assertIsNone(validator.validate(record))

    def test_non_json_value_is_rejected_and_logged(self) -> None:
        '''jsonschema-rs raises on Python values with no JSON
        equivalent; the record must be rejected, not raise.'''
        validator: SchemaValidator = SchemaValidator(_SIMPLE_SCHEMA)
        record: dict = {
            'channel_id': datetime(2026, 1, 1, tzinfo=UTC),
            'channel_handle': 'somehandle',
            'url': 'https://www.youtube.com/@somehandle',
        }
        with self.assertLogs(
            'scrape_exchange.schema_validator', level='WARNING',
        ):
            msg: str | None = validator.validate(record)
        self.assertIsNotNone(msg)
        self.assertTrue(
            msg.startswith('<root>: non-JSON value in record: '), msg,
        )

    def test_conversion_error_is_rejected(self) -> None:
        schema: dict = {
            '$schema': 'https://json-schema.org/draft/2020-12/schema',
            'type': 'object',
            'properties': {'when': {}, 'n': {'type': 'integer'}},
        }
        validator: SchemaValidator = SchemaValidator(schema)
        rust_validator: MagicMock = MagicMock()
        rust_validator.is_valid.side_effect = ValueError(
            "Unsupported type: 'datetime'",
        )
        with (
            patch.object(validator, '_validator', rust_validator),
            self.assertLogs(
                'scrape_exchange.schema_validator', level='WARNING',
            ),
        ):
            msg: str | None = validator.validate({'when': 1})
        self.assertIsNotNone(msg)
        self.assertIn('datetime', msg)

    def test_malformed_schema_raises(self) -> None:
        with self.assertRaises(jsonschema_rs.ValidationError):
            SchemaValidator({'type': 12})


class TestFetchSchemaDict(unittest.IsolatedAsyncioTestCase):

    def _client_with_response(
        self, status_code: int, body: dict | None,
    ) -> MagicMock:
        '''Build an ExchangeClient mock whose ``get`` returns the
        supplied (status_code, body) response.'''
        client = MagicMock()
        response = MagicMock()
        response.status_code = status_code
        response.json = MagicMock(return_value=body)
        response.text = '<body>' if body is None else str(body)
        client.get = AsyncMock(return_value=response)
        return client

    async def test_returns_json_schema_on_200(self) -> None:
        client = self._client_with_response(
            status_code=200,
            body={
                'username': 'boinko',
                'platform': 'youtube',
                'entity': 'channel',
                'version': '0.0.2',
                'json_schema': _SIMPLE_SCHEMA,
            },
        )
        result: dict = await fetch_schema_dict(
            client, 'http://test',
            'boinko', 'youtube', 'channel', '0.0.2',
        )
        self.assertEqual(result, _SIMPLE_SCHEMA)
        client.get.assert_awaited_once()
        sent_url: str = client.get.await_args.args[0]
        self.assertEqual(
            sent_url,
            'http://test/api/v1/schema/param/'
            'boinko/youtube/channel/0.0.2',
        )

    async def test_raises_runtime_error_on_404(self) -> None:
        client = self._client_with_response(
            status_code=404, body={'detail': 'not found'},
        )
        with self.assertRaises(RuntimeError) as cm:
            await fetch_schema_dict(
                client, 'http://test',
                'boinko', 'youtube', 'channel', '9.9.9',
            )
        self.assertIn('404', str(cm.exception))

    async def test_raises_runtime_error_when_json_schema_missing(
        self,
    ) -> None:
        client = self._client_with_response(
            status_code=200,
            body={'username': 'boinko'},  # no json_schema field
        )
        with self.assertRaises(RuntimeError) as cm:
            await fetch_schema_dict(
                client, 'http://test',
                'boinko', 'youtube', 'channel', '0.0.2',
            )
        self.assertIn('json_schema', str(cm.exception))

    async def test_raises_runtime_error_on_transport_exception(
        self,
    ) -> None:
        client = MagicMock()
        client.get = AsyncMock(
            side_effect=ConnectionError('boom'),
        )
        with self.assertRaises(RuntimeError) as cm:
            await fetch_schema_dict(
                client, 'http://test',
                'boinko', 'youtube', 'channel', '0.0.2',
            )
        self.assertIn('Failed to fetch schema', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
