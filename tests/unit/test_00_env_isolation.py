'''
The unit tests must not depend on the repository's .env file. This
module sorts first, so ``discover -s tests/unit`` (which does not import
the tests.unit package) installs the isolation in tests/unit/__init__.py
before any other test module is imported.
'''

import os
import tempfile
import unittest
from pathlib import Path

import tests.unit  # noqa: F401  (installs the .env isolation)
from tests.unit import TEST_ENV_FILE

from scrape_exchange.channel_scrape_queue import ChannelScrapeQueueSettings

DEFAULT_CHANNEL_QUEUES: str = '7:1000000,30:100000,90:10000,180:1000,365:0'


class TestEnvIsolation(unittest.TestCase):

    def test_settings_ignore_the_working_directory_env(self) -> None:
        # A .env in the working directory, as on a production host,
        # must not reach the settings the tests build.
        cwd: str = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, '.env').write_text(
                'CHANNEL_PRIORITY_QUEUES=1:0\n', encoding='utf-8',
            )
            os.chdir(directory)
            try:
                settings: ChannelScrapeQueueSettings = (
                    ChannelScrapeQueueSettings()
                )
            finally:
                os.chdir(cwd)
        self.assertEqual(
            settings.channel_priority_queues, DEFAULT_CHANNEL_QUEUES,
        )

    def test_explicit_env_file_still_wins(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file: Path = Path(directory, 'custom.env')
            env_file.write_text(
                'CHANNEL_PRIORITY_QUEUES=1:0\n', encoding='utf-8',
            )
            settings: ChannelScrapeQueueSettings = (
                ChannelScrapeQueueSettings(_env_file=str(env_file))
            )
        self.assertEqual(settings.channel_priority_queues, '1:0')

    def test_test_env_file_exists(self) -> None:
        self.assertTrue(TEST_ENV_FILE.is_file())


if __name__ == '__main__':
    unittest.main()
