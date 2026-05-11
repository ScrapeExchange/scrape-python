'''Tests for YouTubeScraperSettings new fields.'''

import os
import unittest
from unittest import mock

from scrape_exchange.youtube.settings import (
    YouTubeScraperSettings,
)


class TestChannelPriorityDirectorySetting(
    unittest.TestCase,
):
    '''The channel_priority_directory setting must default
    to the literal string "priority"; an absolute path
    overrides; the env var alias is
    YOUTUBE_CHANNEL_PRIORITY_DIRECTORY.'''

    def _settings(self, env: dict) -> YouTubeScraperSettings:
        with mock.patch.dict(os.environ, env, clear=True):
            return YouTubeScraperSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

    def test_default_value_is_priority(self) -> None:
        s: YouTubeScraperSettings = self._settings({})
        self.assertEqual(
            s.channel_priority_directory, 'priority',
        )

    def test_env_var_override_relative(self) -> None:
        s: YouTubeScraperSettings = self._settings({
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY': 'pri2',
        })
        self.assertEqual(
            s.channel_priority_directory, 'pri2',
        )

    def test_env_var_override_absolute(self) -> None:
        s: YouTubeScraperSettings = self._settings({
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY':
            '/var/spool/priority',
        })
        self.assertEqual(
            s.channel_priority_directory,
            '/var/spool/priority',
        )


class TestChannelPriorityDirectoryPath(
    unittest.TestCase,
):
    '''channel_priority_directory_path resolves the setting
    against channel_data_directory when the setting is a
    relative path, and returns absolute paths verbatim.'''

    def _settings(self, env: dict) -> YouTubeScraperSettings:
        with mock.patch.dict(os.environ, env, clear=True):
            return YouTubeScraperSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

    def test_relative_resolved_against_data_dir(
        self,
    ) -> None:
        s: YouTubeScraperSettings = self._settings({
            'YOUTUBE_CHANNEL_DATA_DIR': '/data/channels',
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY': 'priority',
        })
        self.assertEqual(
            s.channel_priority_directory_path,
            '/data/channels/priority',
        )

    def test_absolute_used_verbatim(self) -> None:
        s: YouTubeScraperSettings = self._settings({
            'YOUTUBE_CHANNEL_DATA_DIR': '/data/channels',
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY':
            '/var/spool/priority',
        })
        self.assertEqual(
            s.channel_priority_directory_path,
            '/var/spool/priority',
        )

    def test_raises_when_data_dir_unset_and_relative(
        self,
    ) -> None:
        '''Cannot resolve a relative priority dir without a
        channel_data_directory. Surface the misconfiguration
        rather than silently returning a confusing path.'''
        s: YouTubeScraperSettings = self._settings({
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY': 'priority',
        })
        with self.assertRaises(ValueError):
            _ = s.channel_priority_directory_path


if __name__ == '__main__':
    unittest.main()
