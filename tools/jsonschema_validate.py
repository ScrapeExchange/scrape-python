#!/usr/bin/env python3
'''
Validate JSON from stdin or files against a JSON Schema file.
'''

import argparse
import json
import sys
from pathlib import Path
from typing import Any, TextIO

import brotli
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError


def main(argv: list[str] | None = None) -> int:
    args: argparse.Namespace = _parse_args(argv)

    try:
        schema: Any = _load_json_file(args.schema)
        Draft202012Validator.check_schema(schema)
    except OSError as exc:
        print(f'{args.schema}: failed to read schema: {exc}', file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        print(f'{args.schema}: invalid schema JSON: {exc}', file=sys.stderr)
        return 2
    except SchemaError as exc:
        print(
            f'{args.schema}: invalid JSON Schema: {exc.message}',
            file=sys.stderr,
        )
        return 2

    validator = Draft202012Validator(schema)
    inputs: list[str] = args.inputs or ['-']
    valid: bool = True

    for input_name in inputs:
        try:
            data: Any = _load_input(input_name, sys.stdin)
        except OSError as exc:
            print(f'{input_name}: failed to read JSON: {exc}', file=sys.stderr)
            valid = False
            continue
        except json.JSONDecodeError as exc:
            print(f'{input_name}: invalid JSON: {exc}', file=sys.stderr)
            valid = False
            continue
        except brotli.error as exc:
            print(
                f'{input_name}: failed to decompress brotli: {exc}',
                file=sys.stderr,
            )
            valid = False
            continue

        errors: list[ValidationError] = sorted(
            validator.iter_errors(data),
            key=lambda error: list(error.absolute_path),
        )
        if errors:
            valid = False
            for error in errors:
                print(
                    f'{input_name}: {_format_error(error)}',
                    file=sys.stderr,
                )

    return 0 if valid else 1


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--schema',
        '-s',
        type=Path,
        required=True,
        help='Path to the JSON Schema file.',
    )
    parser.add_argument(
        'inputs',
        nargs='*',
        help='JSON file paths to validate. Use "-" or omit paths for stdin.',
    )
    return parser.parse_args(argv)


def _load_json_file(path: Path) -> Any:
    with path.open(encoding='utf-8') as fh:
        return json.load(fh)


def _load_input(input_name: str, stdin: TextIO) -> Any:
    if input_name == '-':
        return json.load(stdin)
    path = Path(input_name)
    if path.suffix == '.br':
        return json.loads(brotli.decompress(path.read_bytes()))
    with path.open(encoding='utf-8') as fh:
        return json.load(fh)


def _format_error(error: ValidationError) -> str:
    pointer: str = '/' + '/'.join(str(part) for part in error.absolute_path)
    if pointer == '/':
        pointer = '<root>'
    return f'{pointer}: {error.message}'


if __name__ == '__main__':
    sys.exit(main())
