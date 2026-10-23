'''
Settings and marker suffixes for the YouTube privacy limits.
'''

import os
import unittest
from unittest import mock

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.settings import YouTubeScraperSettings


_PRIVACY_ENV: tuple[str, ...] = (
    'CHANNEL_MIN_SUBSCRIBERS',
    'YOUTUBE_CHANNEL_PRIVACY_SUBSCRIBERS',
    'YOUTUBE_CHANNEL_VIEWS_PER_SUBSCRIBER',
    'YOUTUBE_PRIVACY_ALLOWED_DOMAINS',
)


def _settings() -> YouTubeScraperSettings:
    return YouTubeScraperSettings(_env_file=None, _cli_parse_args=[])


class TestPrivacySettings(unittest.TestCase):

    def setUp(self) -> None:
        clean: dict[str, str] = {
            key: value for key, value in os.environ.items()
            if key not in _PRIVACY_ENV
        }
        patcher = mock.patch.dict(os.environ, clean, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_defaults(self) -> None:
        settings: YouTubeScraperSettings = _settings()
        self.assertEqual(settings.channel_min_subscribers, 10)
        self.assertEqual(settings.channel_privacy_subscribers, 4000)
        self.assertEqual(settings.channel_views_per_subscriber, 100)
        self.assertEqual(settings.extra_allowed_domains, frozenset())

    def test_env_names(self) -> None:
        with mock.patch.dict(os.environ, {
            'YOUTUBE_CHANNEL_PRIVACY_SUBSCRIBERS': '5000',
            'YOUTUBE_CHANNEL_VIEWS_PER_SUBSCRIBER': '50',
        }):
            settings: YouTubeScraperSettings = _settings()
        self.assertEqual(settings.channel_privacy_subscribers, 5000)
        self.assertEqual(settings.channel_views_per_subscriber, 50)

    def test_allowed_domains_parsing(self) -> None:
        with mock.patch.dict(os.environ, {
            'YOUTUBE_PRIVACY_ALLOWED_DOMAINS':
                ' Example.org, www.foo.example ,,',
        }):
            settings: YouTubeScraperSettings = _settings()
        self.assertEqual(
            settings.extra_allowed_domains,
            frozenset({'example.org', 'foo.example'}),
        )

    def test_allowed_domains_strip_scheme_and_path(self) -> None:
        with mock.patch.dict(os.environ, {
            'YOUTUBE_PRIVACY_ALLOWED_DOMAINS':
                'https://mysite.example/, http://WWW.Band.example/tour',
        }):
            settings: YouTubeScraperSettings = _settings()
        self.assertEqual(
            settings.extra_allowed_domains,
            frozenset({'mysite.example', 'band.example'}),
        )


class TestMarkerSuffixes(unittest.TestCase):

    def test_new_suffixes_are_markers(self) -> None:
        self.assertTrue(AssetFileManagement.is_marker(
            'channel-UCx.json.br.low_subs'))
        self.assertTrue(AssetFileManagement.is_marker(
            'video-min-abc.json.br.unknown_channel'))


if __name__ == '__main__':
    unittest.main()
