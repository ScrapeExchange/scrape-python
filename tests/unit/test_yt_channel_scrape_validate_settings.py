'''Tests for ``tools.yt_channel_scrape._validate_settings``.

The validate-settings guard must:

* skip the ``channel_list`` file-existence check when ``REDIS_DSN``
  is set (Redis mode reads channels from
  ``RedisChannelScrapeQueue``, not the file).
* still enforce ``channel_list`` exists when ``REDIS_DSN`` is
  unset (legacy file-based path needs it).
* still enforce ``channel_data_directory`` is set in either mode.
'''

import os
import tempfile
import unittest
from unittest.mock import MagicMock


class TestValidateSettingsRedisMode(unittest.TestCase):
    '''When ``redis_dsn`` is set, ``channel_list`` is not consulted
    and a missing path must not exit the process.'''

    def _settings(
        self,
        *,
        redis_dsn: str | None,
        channel_list: str,
        channel_data_directory: str,
    ) -> MagicMock:
        s: MagicMock = MagicMock()
        s.redis_dsn = redis_dsn
        s.channel_list = channel_list
        s.channel_data_directory = channel_data_directory
        return s

    def test_redis_mode_does_not_require_channel_list(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        with tempfile.TemporaryDirectory() as data_dir:
            settings: MagicMock = self._settings(
                redis_dsn='redis://localhost:6379/0',
                channel_list='/nonexistent/path.lst',
                channel_data_directory=data_dir,
            )
            # No SystemExit — the missing channel_list is
            # ignored when Redis is the source of channels.
            _validate_settings(settings)

    def test_redis_mode_does_not_require_channel_list_setting(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        with tempfile.TemporaryDirectory() as data_dir:
            settings: MagicMock = self._settings(
                redis_dsn='redis://localhost:6379/0',
                channel_list='',
                channel_data_directory=data_dir,
            )
            _validate_settings(settings)

    def test_file_mode_requires_channel_list_to_exist(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        with tempfile.TemporaryDirectory() as data_dir:
            settings: MagicMock = self._settings(
                redis_dsn=None,
                channel_list='/nonexistent/path.lst',
                channel_data_directory=data_dir,
            )
            with self.assertRaises(SystemExit) as ctx:
                _validate_settings(settings)
            self.assertEqual(ctx.exception.code, 1)

    def test_file_mode_requires_channel_list_setting(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        with tempfile.TemporaryDirectory() as data_dir:
            settings: MagicMock = self._settings(
                redis_dsn=None,
                channel_list='',
                channel_data_directory=data_dir,
            )
            with self.assertRaises(SystemExit) as ctx:
                _validate_settings(settings)
            self.assertEqual(ctx.exception.code, 1)

    def test_file_mode_passes_when_channel_list_exists(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        with tempfile.TemporaryDirectory() as data_dir:
            lst_path: str = os.path.join(
                data_dir, 'channels.lst',
            )
            with open(lst_path, 'w') as fh:
                fh.write('')
            settings: MagicMock = self._settings(
                redis_dsn=None,
                channel_list=lst_path,
                channel_data_directory=data_dir,
            )
            _validate_settings(settings)

    def test_channel_data_directory_required_in_redis_mode(
        self,
    ) -> None:
        from tools.yt_channel_scrape import _validate_settings

        settings: MagicMock = self._settings(
            redis_dsn='redis://localhost:6379/0',
            channel_list='/nonexistent/path.lst',
            channel_data_directory='',
        )
        with self.assertRaises(SystemExit) as ctx:
            _validate_settings(settings)
        self.assertEqual(ctx.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
