'''Tests for tools/jsonschema_validate.py.'''

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import brotli


def _load_tool() -> ModuleType:
    if 'jsonschema_validate' in sys.modules:
        return sys.modules['jsonschema_validate']
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'jsonschema_validate.py'
    spec = importlib.util.spec_from_file_location(
        'jsonschema_validate', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['jsonschema_validate'] = module
    spec.loader.exec_module(module)
    return module


tool: ModuleType = _load_tool()


_SCHEMA: dict = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object',
    'properties': {
        'name': {'type': 'string'},
        'count': {'type': 'integer'},
    },
    'required': ['name'],
    'additionalProperties': False,
}


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding='utf-8')


def _write_brotli_json(path: Path, payload: object) -> None:
    path.write_bytes(
        brotli.compress(json.dumps(payload).encode('utf-8')),
    )


class TestJsonschemaValidate(unittest.TestCase):

    def test_valid_file_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema_path = root / 'schema.json'
            data_path = root / 'data.json'
            _write_json(schema_path, _SCHEMA)
            _write_json(data_path, {'name': 'test', 'count': 2})

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result: int = tool.main(
                    ['-s', str(schema_path), str(data_path)],
                )

        self.assertEqual(result, 0)
        self.assertEqual(stderr.getvalue(), '')

    def test_brotli_file_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema_path = root / 'schema.json'
            data_path = root / 'data.json.br'
            _write_json(schema_path, _SCHEMA)
            _write_brotli_json(data_path, {'name': 'compressed', 'count': 3})

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result: int = tool.main(
                    ['-s', str(schema_path), str(data_path)],
                )

        self.assertEqual(result, 0)
        self.assertEqual(stderr.getvalue(), '')

    def test_invalid_file_reports_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema_path = root / 'schema.json'
            data_path = root / 'data.json'
            _write_json(schema_path, _SCHEMA)
            _write_json(data_path, {'name': 'test', 'count': 'two'})

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result: int = tool.main(
                    ['-s', str(schema_path), str(data_path)],
                )

        self.assertEqual(result, 1)
        self.assertIn('/count', stderr.getvalue())
        self.assertIn('is not of type', stderr.getvalue())

    def test_invalid_json_reports_input_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema_path = root / 'schema.json'
            data_path = root / 'data.json'
            _write_json(schema_path, _SCHEMA)
            data_path.write_text('{', encoding='utf-8')

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result: int = tool.main(['-s', str(schema_path), str(data_path)])

        self.assertEqual(result, 1)
        self.assertIn('invalid JSON', stderr.getvalue())

    def test_omitted_input_reads_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            schema_path = Path(td) / 'schema.json'
            _write_json(schema_path, _SCHEMA)

            stderr = io.StringIO()
            stdin = io.StringIO('{"name": "stdin"}')
            with patch.object(sys, 'stdin', stdin), redirect_stderr(stderr):
                result: int = tool.main(['--schema', str(schema_path)])

        self.assertEqual(result, 0)
        self.assertEqual(stderr.getvalue(), '')

    def test_bad_schema_returns_usage_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            schema_path = Path(td) / 'schema.json'
            _write_json(schema_path, {'type': 12})

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                result: int = tool.main(['--schema', str(schema_path)])

        self.assertEqual(result, 2)
        self.assertIn('invalid JSON Schema', stderr.getvalue())
