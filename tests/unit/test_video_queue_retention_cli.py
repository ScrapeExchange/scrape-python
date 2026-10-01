'''The operator command defaults to a non-mutating, scoped dry run.'''

import unittest

from pydantic import ValidationError

from tools.video_queue_retention import RetentionSettings


class TestRetentionSettings(unittest.TestCase):
    def test_cli_defaults_to_dry_run(self) -> None:
        settings: RetentionSettings = RetentionSettings(
            _cli_parse_args=['--redis-dsn', 'redis://localhost:6379/0'],
            _env_file=None,
        )
        self.assertFalse(settings.apply)
        self.assertEqual(settings.platform, 'youtube')

    def test_cli_can_bound_an_explicit_apply(self) -> None:
        settings: RetentionSettings = RetentionSettings(
            _cli_parse_args=[
                '--redis-dsn', 'redis://localhost:6379/0',
                '--apply', '--limit', '10', '--platform', 'tiktok',
            ],
            _env_file=None,
        )
        self.assertTrue(settings.apply)
        self.assertEqual(settings.limit, 10)
        self.assertEqual(settings.platform, 'tiktok')

    def test_rejects_invalid_scan_limits(self) -> None:
        with self.assertRaises(ValidationError):
            RetentionSettings(
                redis_dsn='redis://localhost:6379/0',
                batch_size=0, _cli_parse_args=[], _env_file=None,
            )
