'''Unit tests never read the repository's .env file (a symlink to the
production settings on a production host) and never talk to a real
MongoDB.

Every pydantic-settings class reads ``tests/collateral/.env`` instead
of its configured ``env_file``, unless a test passes ``_env_file``
itself. An empty MONGO_DSN in the environment also overrides any value
exported in a developer's shell, so VideoScrapeQueueSettings() stays in
Redis-only mode unless a test injects a backlog explicitly.

Running ``discover -s tests/unit`` does not import this package, so
``test_00_env_isolation`` imports it before any other test module.
'''

import os
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

TEST_ENV_FILE: Path = (
    Path(__file__).resolve().parent.parent / 'collateral' / '.env'
)

os.environ['MONGO_DSN'] = ''

if not getattr(BaseSettings.__init__, '_test_env_isolated', False):
    _settings_init: Any = BaseSettings.__init__

    def _isolated_init(self: BaseSettings, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('_env_file', str(TEST_ENV_FILE))
        _settings_init(self, *args, **kwargs)

    _isolated_init._test_env_isolated = True
    BaseSettings.__init__ = _isolated_init
