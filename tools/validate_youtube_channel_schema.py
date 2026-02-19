#!/usr/bin/env python3

import sys
import json

from pathlib import Path

import jsonschema
from jsonschema import ValidationError

from scrape_exchange.youtube.youtube_channel import YouTubeChannel


def main() -> sys.NoReturn:
    schema_path = Path('tests/collateral/boinko-youtube-channel-schema.json')
    if not schema_path.exists():
        print(f'Schema not found at {schema_path}')
        sys.exit(2)

    schema: dict[str, any] = json.loads(schema_path.read_text())

    # Build a sample channel dict
    ch = YouTubeChannel(name='TestChannel')
    ch.title = 'Test Channel Title'
    ch.description = 'A sample channel for testing'
    ch.subscriber_count = 42
    ch.video_count = 3
    ch.view_count = 1000

    sample: dict[str, any] = ch.to_dict()

    try:
        jsonschema.validate(instance=sample, schema=schema)
        print('Validation successful: sample conforms to schema')
        sys.exit(0)
    except ValidationError as ve:
        print('Validation failed:')
        print(ve)
        print('Sample output:\n')
        print(json.dumps(sample, indent=2, default=str))
        sys.exit(1)


if __name__ == '__main__':
    main()
