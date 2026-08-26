'''
Unit tests for Instagram creator scrape tool configuration.
'''

import os
import unittest

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import tools.ig_creator_scrape as tool


class _AsyncSession:

    def __init__(self, page: MagicMock) -> None:
        self._page: MagicMock = page

    async def __aenter__(self) -> SimpleNamespace:
        return SimpleNamespace(page=self._page)

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        return None


def _rich_profile_html() -> str:
    return '''
    <html><head>
    <meta property="og:description"
          content="382M Followers, 363 Following, 8,445 Posts">
    <meta property="og:title"
          content="Dwayne Johnson (@therock) • Instagram photos and videos">
    <script type="application/json">
    {"require":[["RelayPrefetchedStreamCache","next",[],[
      "query", {"__bbox":{"result":{"data":{
        "xig_user_by_username":{
          "pk":"232192182",
          "username":"therock",
          "profile_pic_url":"https://files.scrape.exchange/p.jpg",
          "biography":"raising daughters",
          "full_name":"Dwayne Johnson",
          "is_verified":true,
          "bio_links":[{
            "link_type":"external",
            "url":"http://therock.komi.io",
            "title":"",
            "creation_source":"NONE"
          }],
          "follower_count":382245561,
          "following_count":353,
          "all_media_count":8445,
          "id":"17841400005463628"
        }}}}}]]]}
    </script></head><body></body></html>
    '''


class TestInstagramCreatorSettings(unittest.TestCase):

    def test_default_metrics_port_avoids_youtube_video(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = tool.CreatorSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

        self.assertEqual(settings.metrics_port, 9900)

    def test_orphan_recovery_defaults_to_once_per_day(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = tool.CreatorSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

        self.assertEqual(
            settings.creator_orphan_recovery_interval_seconds,
            86400,
        )

    def test_disable_proxies_env_is_accepted(self) -> None:
        with patch.dict(
            os.environ,
            {'IG_CREATOR_DISABLE_PROXIES': 'true'},
            clear=True,
        ):
            settings = tool.CreatorSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

        self.assertTrue(settings.creator_disable_proxies)

    def test_disable_proxies_ignores_missing_proxy_files(self) -> None:
        with patch.dict(
            os.environ,
            {
                'IG_CREATOR_DISABLE_PROXIES': 'true',
                'PROXY_FILES': '/tmp/missing-instagram.proxies.lst',
            },
            clear=True,
        ):
            settings = tool.CreatorSettings(
                _env_file=None,
                _cli_parse_args=[],
            )

        self.assertTrue(settings.creator_disable_proxies)
        self.assertEqual(settings.proxies, [])

    def test_disable_proxies_clears_loaded_proxy_pool(self) -> None:
        settings = tool.CreatorSettings(
            _env_file=None,
            _cli_parse_args=[],
        )
        object.__setattr__(
            settings,
            'proxies',
            ['http://proxy-1.example:8080'],
        )
        settings.creator_disable_proxies = True

        tool._apply_proxy_settings(settings)

        self.assertEqual(settings.proxies, [])

    def test_proxy_pool_is_preserved_by_default(self) -> None:
        settings = tool.CreatorSettings(
            _env_file=None,
            _cli_parse_args=[],
        )
        object.__setattr__(
            settings,
            'proxies',
            ['http://proxy-1.example:8080'],
        )

        tool._apply_proxy_settings(settings)

        self.assertEqual(
            settings.proxies,
            ['http://proxy-1.example:8080'],
        )


class TestInstagramCreatorFailureHandling(unittest.IsolatedAsyncioTestCase):

    async def test_fetch_waits_for_hydrated_profile_json(self) -> None:
        events: list[str] = []
        page: MagicMock = MagicMock()
        page.url = 'https://www.instagram.com/therock/'
        page.goto = AsyncMock(
            return_value=SimpleNamespace(status=200),
        )
        page.title = AsyncMock(return_value='The Rock')

        async def wait_for_function(*args: object, **kwargs: object) -> None:
            del args
            del kwargs
            events.append('wait')

        async def content() -> str:
            events.append('content')
            return _rich_profile_html()

        page.wait_for_function = AsyncMock(side_effect=wait_for_function)
        page.content = AsyncMock(side_effect=content)
        pool: MagicMock = MagicMock()
        pool.gate_profile_request = AsyncMock()
        pool.session_for.return_value = _AsyncSession(page)
        settings: MagicMock = MagicMock()
        settings.session_bootstrap_timeout_ms = 30_000
        settings.creator_profile_data_timeout_ms = 12_000

        profile, evidence = await tool._fetch_profile_data(
            pool, '__direct__', 'therock', settings,
        )

        self.assertEqual(events, ['wait', 'content'])
        page.wait_for_function.assert_awaited_once()
        self.assertEqual(
            page.wait_for_function.await_args.kwargs['timeout'], 12_000,
        )
        self.assertTrue(profile.verified)
        self.assertEqual(profile.biography, 'raising daughters')
        self.assertEqual(profile.bio_links[0].url, 'http://therock.komi.io')
        self.assertEqual(
            evidence['detected_markers'],
            ['structured_profile'],
        )

    async def test_fetch_fails_when_hydrated_profile_json_is_missing(
        self,
    ) -> None:
        page: MagicMock = MagicMock()
        page.url = 'https://www.instagram.com/therock/'
        page.goto = AsyncMock(
            return_value=SimpleNamespace(status=200),
        )
        page.title = AsyncMock(return_value='The Rock')
        page.wait_for_function = AsyncMock(
            side_effect=tool.PlaywrightTimeoutError('missing profile JSON'),
        )
        page.content = AsyncMock(
            return_value='<html>/accounts/login/</html>',
        )
        pool: MagicMock = MagicMock()
        pool.gate_profile_request = AsyncMock()
        pool.session_for.return_value = _AsyncSession(page)
        settings: MagicMock = MagicMock()
        settings.session_bootstrap_timeout_ms = 30_000
        settings.creator_profile_data_timeout_ms = 12_000

        with self.assertRaisesRegex(
            tool.InstagramProfileJsonTimeoutError,
            'profile JSON timeout',
        ) as cm:
            await tool._fetch_profile_data(
                pool, '__direct__', 'therock', settings,
            )

        page.content.assert_awaited_once()
        self.assertEqual(
            cm.exception.evidence['detected_markers'],
            ['login'],
        )
        self.assertEqual(cm.exception.evidence['http_status'], 200)
        self.assertEqual(cm.exception.evidence['page_title'], 'The Rock')
        self.assertGreater(cm.exception.evidence['html_length'], 0)

    async def test_rate_limit_uses_retry_delay_not_tier_interval(
        self,
    ) -> None:
        queue: MagicMock = MagicMock()
        queue.remove = AsyncMock()
        queue.release = AsyncMock()
        queue.reschedule_in = AsyncMock()
        queue.show_member = AsyncMock(
            return_value={'score': 123.0},
        )
        queue.record_scrape_failure = AsyncMock()
        settings: MagicMock = MagicMock()
        settings.creator_rate_limit_retry_interval_seconds = 1800.0
        settings.creator_unknown_followers_retry_interval_seconds = 86400.0
        settings.creator_retry_interval_seconds = 300.0
        settings.creator_retry_jitter_fraction = 0.25

        with patch.object(
            tool, '_jittered_retry_delay', return_value=1800.0,
        ):
            reason: str = await tool._handle_failure(
                tool.InstagramRateLimitError('blocked'),
                'cristiano',
                queue,
                settings,
                'worker-1',
                '__direct__',
                'none',
            )

        self.assertEqual(reason, 'rate_limit')
        queue.reschedule_in.assert_awaited_once_with(
            'cristiano', 1800.0,
        )
        queue.release.assert_not_awaited()
        queue.record_scrape_failure.assert_awaited_once()

    async def test_login_wall_timeout_uses_rate_limit_path(self) -> None:
        queue: MagicMock = MagicMock()
        queue.remove = AsyncMock()
        queue.release = AsyncMock()
        queue.reschedule_in = AsyncMock()
        queue.show_member = AsyncMock(
            return_value={'score': 123.0},
        )
        queue.record_scrape_failure = AsyncMock()
        settings: MagicMock = MagicMock()
        settings.creator_rate_limit_retry_interval_seconds = 1800.0
        settings.creator_unknown_followers_retry_interval_seconds = 86400.0
        settings.creator_retry_interval_seconds = 300.0
        settings.creator_retry_jitter_fraction = 0.25
        exc = tool.InstagramProfileJsonTimeoutError(
            12_000,
            {
                'last_url': (
                    'https://www.instagram.com/accounts/login/?next=/x/'
                ),
                'page_title': 'Instagram',
                'http_status': 200,
                'detected_markers': ['login'],
                'html_length': 575_000,
            },
        )

        with patch.object(
            tool, '_jittered_retry_delay', return_value=1800.0,
        ):
            reason: str = await tool._handle_failure(
                exc,
                'cristiano',
                queue,
                settings,
                'worker-1',
                '__direct__',
                'none',
            )

        self.assertEqual(reason, 'rate_limit')
        queue.reschedule_in.assert_awaited_once_with(
            'cristiano', 1800.0,
        )
        queue.release.assert_not_awaited()
        queue.record_scrape_failure.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
