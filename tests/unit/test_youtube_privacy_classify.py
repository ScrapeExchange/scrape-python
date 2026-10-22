'''
Channel size classification and the domain allowlist for the YouTube
privacy limits.
'''

import os
import unittest
from unittest import mock

from scrape_exchange.datatypes import SocialNetworks
from scrape_exchange.youtube.privacy import (
    HARDCODED_ALLOWED_DOMAINS,
    SOCIAL_NETWORK_DOMAINS,
    ChannelSize,
    allowed_domains,
    classify_channel_size,
    classify_for_settings,
    is_allowed_url,
    unwrap_youtube_redirect,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings


KW: dict[str, int] = {
    'min_subscribers': 10,
    'privacy_subscribers': 4000,
    'views_per_subscriber': 100,
}


class TestClassify(unittest.TestCase):

    def test_subscriber_count_decides(self) -> None:
        self.assertEqual(
            classify_channel_size(9, None, **KW), ChannelSize.TINY)
        self.assertEqual(
            classify_channel_size(0, 10**9, **KW), ChannelSize.TINY)
        self.assertEqual(
            classify_channel_size(10, None, **KW), ChannelSize.SMALL)
        self.assertEqual(
            classify_channel_size(3999, None, **KW), ChannelSize.SMALL)
        self.assertEqual(
            classify_channel_size(4000, 0, **KW), ChannelSize.LARGE)

    def test_view_count_fallback(self) -> None:
        self.assertEqual(
            classify_channel_size(None, 999, **KW), ChannelSize.TINY)
        self.assertEqual(
            classify_channel_size(None, 1000, **KW), ChannelSize.SMALL)
        self.assertEqual(
            classify_channel_size(None, 399_999, **KW),
            ChannelSize.SMALL)
        self.assertEqual(
            classify_channel_size(None, 400_000, **KW),
            ChannelSize.LARGE)

    def test_both_unknown_is_tiny(self) -> None:
        self.assertEqual(
            classify_channel_size(None, None, **KW), ChannelSize.TINY)

    def test_zero_thresholds_disable(self) -> None:
        off: dict[str, int] = dict(
            KW, min_subscribers=0, privacy_subscribers=0,
        )
        self.assertEqual(
            classify_channel_size(None, None, **off), ChannelSize.LARGE)
        self.assertEqual(
            classify_channel_size(3, None, **off), ChannelSize.LARGE)

    def test_rule_1_off_rule_2_on(self) -> None:
        only_2: dict[str, int] = dict(KW, min_subscribers=0)
        self.assertEqual(
            classify_channel_size(None, None, **only_2),
            ChannelSize.SMALL)
        self.assertEqual(
            classify_channel_size(3, None, **only_2), ChannelSize.SMALL)

    def test_rule_2_off_rule_1_on(self) -> None:
        only_1: dict[str, int] = dict(KW, privacy_subscribers=0)
        self.assertEqual(
            classify_channel_size(3, None, **only_1), ChannelSize.TINY)
        self.assertEqual(
            classify_channel_size(500, None, **only_1),
            ChannelSize.LARGE)

    def test_classify_for_settings_uses_settings(self) -> None:
        with mock.patch.dict(os.environ, {
            'CHANNEL_MIN_SUBSCRIBERS': '10',
            'YOUTUBE_CHANNEL_PRIVACY_SUBSCRIBERS': '4000',
            'YOUTUBE_CHANNEL_VIEWS_PER_SUBSCRIBER': '100',
        }):
            settings: YouTubeScraperSettings = YouTubeScraperSettings(
                _env_file=None, _cli_parse_args=[],
            )
        self.assertEqual(
            classify_for_settings(None, 1000, settings),
            ChannelSize.SMALL)


class TestAllowlist(unittest.TestCase):

    def test_every_social_network_has_domains(self) -> None:
        self.assertEqual(
            set(SOCIAL_NETWORK_DOMAINS), set(SocialNetworks))
        domains: tuple[str, ...]
        for domains in SOCIAL_NETWORK_DOMAINS.values():
            self.assertTrue(domains)
            self.assertTrue(
                set(domains) <= HARDCODED_ALLOWED_DOMAINS)

    def test_well_known_sites_included(self) -> None:
        domain: str
        for domain in ('instagram.com', 'x.com', 'tiktok.com',
                       'linktr.ee', 'beacons.ai', 'wikipedia.org',
                       'amazon.com'):
            self.assertIn(domain, HARDCODED_ALLOWED_DOMAINS)

    def test_label_boundary(self) -> None:
        a: frozenset[str] = HARDCODED_ALLOWED_DOMAINS
        self.assertTrue(is_allowed_url('https://www.Instagram.COM/me', a))
        self.assertTrue(is_allowed_url('m.instagram.com/me', a))
        self.assertTrue(is_allowed_url('https://instagram.com./me', a))
        self.assertFalse(
            is_allowed_url('https://instagram.com.evil.example/', a))
        self.assertFalse(is_allowed_url('https://notinstagram.com/', a))
        self.assertFalse(is_allowed_url('https://myblog.example/', a))

    def test_redirect_wrapper_uses_target(self) -> None:
        a: frozenset[str] = HARDCODED_ALLOWED_DOMAINS
        ok: str = (
            'https://www.youtube.com/redirect?event=x'
            '&q=https%3A%2F%2Flinktr.ee%2Fme'
        )
        bad: str = (
            'https://www.youtube.com/redirect?event=x'
            '&q=https%3A%2F%2Fmyblog.example%2F'
        )
        self.assertEqual(
            unwrap_youtube_redirect(ok), 'https://linktr.ee/me')
        self.assertTrue(is_allowed_url(ok, a))
        self.assertFalse(is_allowed_url(bad, a))

    def test_plain_youtube_url_is_not_unwrapped(self) -> None:
        url: str = 'https://www.youtube.com/watch?v=abc&q=x'
        self.assertEqual(unwrap_youtube_redirect(url), url)

    def test_env_example_lists_hardcoded_domains(self) -> None:
        path: str = os.path.join(
            os.path.dirname(__file__), '..', '..', '.env-example',
        )
        with open(path, encoding='utf-8') as handle:
            text: str = handle.read()
        block: str = text.split('Built in:', 1)[1].split(
            'YOUTUBE_PRIVACY_ALLOWED_DOMAINS=', 1)[0]
        listed: set[str] = {
            item.strip()
            for line in block.splitlines()
            for item in line.lstrip('# ').split(',')
            if item.strip()
        }
        self.assertEqual(listed, set(HARDCODED_ALLOWED_DOMAINS))

    def test_allowed_domains_adds_settings(self) -> None:
        with mock.patch.dict(os.environ, {
            'YOUTUBE_PRIVACY_ALLOWED_DOMAINS': 'myband.example',
        }):
            settings: YouTubeScraperSettings = YouTubeScraperSettings(
                _env_file=None, _cli_parse_args=[],
            )
        domains: frozenset[str] = allowed_domains(settings)
        self.assertIn('myband.example', domains)
        self.assertTrue(HARDCODED_ALLOWED_DOMAINS <= domains)
        self.assertTrue(
            is_allowed_url('https://www.myband.example/tour', domains))


if __name__ == '__main__':
    unittest.main()
