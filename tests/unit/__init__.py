'''Unit tests never read the repository's .env file (a symlink to the
production settings on a production host).

Every pydantic-settings class reads ``tests/collateral/.env`` instead
of its configured ``env_file``, unless a test passes ``_env_file``
itself.

Running ``discover -s tests/unit`` does not import this package, so
``test_00_env_isolation`` imports it before any other test module.
'''

from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

TEST_ENV_FILE: Path = (
    Path(__file__).resolve().parent.parent / 'collateral' / '.env'
)

if not getattr(BaseSettings.__init__, '_test_env_isolated', False):
    _settings_init: Any = BaseSettings.__init__

    def _isolated_init(self: BaseSettings, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('_env_file', str(TEST_ENV_FILE))
        _settings_init(self, *args, **kwargs)

    _isolated_init._test_env_isolated = True
    BaseSettings.__init__ = _isolated_init
