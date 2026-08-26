'''Unit tests for the pooled InnerTube pool.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import scrape_exchange.youtube.youtube_channel_tabs as youtube_channel_tabs
from scrape_exchange.youtube.youtube_channel_tabs import (
    aclose_pooled_innertube,
    build_innertube_with_pool_limits,
    pooled_innertube_for_entry,
    pooled_player_innertube_for_entry,
)


class TestPooledInnerTube(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        youtube_channel_tabs._reset_pool_for_tests()

    async def asyncTearDown(self) -> None:
        await aclose_pooled_innertube()

    def test_web_factory_uses_current_profile(self) -> None:
        old_session: MagicMock = MagicMock()
        old_session.headers = {}
        client: MagicMock = MagicMock(
            adaptor=MagicMock(session=old_session),
        )
        with patch.object(
            youtube_channel_tabs,
            'InnerTube',
            return_value=client,
        ) as innertube_type, patch.object(
            youtube_channel_tabs.httpx,
            'Client',
            return_value=MagicMock(),
        ):
            build_innertube_with_pool_limits(None)

        innertube_type.assert_called_once_with(
            'WEB',
            '2.20260708.00.00',
            proxies=None,
        )

    async def test_same_entry_returns_same_instance(self) -> None:
        with patch.object(
            youtube_channel_tabs, 'InnerTube',
            return_value=MagicMock(adaptor=MagicMock(
                session=MagicMock(),
            )),
        ), patch.object(
            youtube_channel_tabs.httpx, 'Client',
            return_value=MagicMock(),
        ), patch.object(
            youtube_channel_tabs.YouTubeCookieJar, 'get',
            return_value=MagicMock(load_into_session=MagicMock()),
        ), patch.object(
            youtube_channel_tabs, 'generate_visitor_info',
            return_value='visitor',
        ):
            a: object = pooled_innertube_for_entry(
                'http://1.1.1.1:80',
            )
            b: object = pooled_innertube_for_entry(
                'http://1.1.1.1:80',
            )
        self.assertIs(a, b)

    async def test_player_factory_uses_android_profile(self) -> None:
        proxy: str = 'http://localhost:8080'
        old_session: MagicMock = MagicMock()
        old_session.headers = {'User-Agent': 'android-agent'}
        client: MagicMock = MagicMock(
            adaptor=MagicMock(session=old_session),
        )
        replacement_session: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs,
            'InnerTube',
            return_value=client,
        ) as innertube_type, patch.object(
            youtube_channel_tabs.httpx,
            'Client',
            return_value=replacement_session,
        ) as httpx_client, patch.object(
            youtube_channel_tabs.YouTubeCookieJar,
            'get',
        ) as cookie_jar_get:
            result: object = pooled_player_innertube_for_entry(proxy)

        self.assertIs(result, client)
        innertube_type.assert_called_once_with(
            'ANDROID',
            '21.26.364',
            user_agent=(
                'com.google.android.youtube/21.26.364 '
                '(Linux; U; Android 11) gzip'
            ),
            proxies=proxy,
        )
        self.assertEqual(
            httpx_client.call_args.kwargs['headers'],
            {'User-Agent': 'android-agent'},
        )
        self.assertIs(client.adaptor.session, replacement_session)
        old_session.close.assert_called_once_with()
        cookie_jar_get.assert_not_called()

        await aclose_pooled_innertube()
        replacement_session.close.assert_called_once_with()

    async def test_refresh_retires_only_challenged_player_client(
        self,
    ) -> None:
        proxy: str = 'http://localhost:8080'
        web_client: MagicMock = MagicMock()
        player_client: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs._INNERTUBE_POOL,
            'factory',
            MagicMock(return_value=web_client),
        ), patch.object(
            youtube_channel_tabs._PLAYER_INNERTUBE_POOL,
            'factory',
            MagicMock(return_value=player_client),
        ):
            youtube_channel_tabs._reset_pool_for_tests()
            pooled_innertube_for_entry(proxy)
            pooled_player_innertube_for_entry(proxy)
            await youtube_channel_tabs.refresh_pooled_innertube_for_entry(
                proxy, challenged=player_client,
            )

        web_client.close.assert_not_called()
        player_client.close.assert_called_once_with()

    async def test_web_refresh_retires_only_challenged_web_client(
        self,
    ) -> None:
        proxy: str = 'http://localhost:8080'
        web_client: MagicMock = MagicMock()
        player_client: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs._INNERTUBE_POOL,
            'factory',
            MagicMock(return_value=web_client),
        ), patch.object(
            youtube_channel_tabs._PLAYER_INNERTUBE_POOL,
            'factory',
            MagicMock(return_value=player_client),
        ):
            youtube_channel_tabs._reset_pool_for_tests()
            pooled_innertube_for_entry(proxy)
            pooled_player_innertube_for_entry(proxy)
            retire_web = getattr(
                youtube_channel_tabs,
                'refresh_pooled_web_innertube_for_entry',
                None,
            )
            self.assertIsNotNone(retire_web)
            if retire_web is None:
                return
            await retire_web(proxy, challenged=web_client)

        web_client.close.assert_called_once_with()
        player_client.close.assert_not_called()

    async def test_player_refresh_waits_for_existing_borrowers(
        self,
    ) -> None:
        proxy: str = 'http://localhost:8080'
        challenged: MagicMock = MagicMock()
        replacement: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs._PLAYER_INNERTUBE_POOL,
            'factory',
            MagicMock(side_effect=[challenged, replacement]),
        ):
            first: object = (
                youtube_channel_tabs
                .borrow_pooled_player_innertube_for_entry(proxy)
            )
            second: object = (
                youtube_channel_tabs
                .borrow_pooled_player_innertube_for_entry(proxy)
            )
            retired: bool = await (
                youtube_channel_tabs
                .refresh_pooled_innertube_for_entry(
                    proxy, challenged=challenged,
                )
            )

            self.assertTrue(retired)
            self.assertIs(first, challenged)
            self.assertIs(second, challenged)
            self.assertIs(
                pooled_player_innertube_for_entry(proxy),
                replacement,
            )
            challenged.close.assert_not_called()

            await youtube_channel_tabs.release_pooled_player_innertube(
                first,
            )
            challenged.close.assert_not_called()
            await youtube_channel_tabs.release_pooled_player_innertube(
                second,
            )

        challenged.close.assert_called_once_with()

    async def test_factory_only_runs_once_per_entry(self) -> None:
        '''Cookie-jar load and visitor_id generation happen on
        first creation per entry; subsequent calls reuse the
        cached client without re-running the factory.'''
        load_into_session: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs, 'InnerTube',
            return_value=MagicMock(adaptor=MagicMock(
                session=MagicMock(),
            )),
        ), patch.object(
            youtube_channel_tabs.httpx, 'Client',
            return_value=MagicMock(),
        ), patch.object(
            youtube_channel_tabs.YouTubeCookieJar, 'get',
            return_value=MagicMock(
                load_into_session=load_into_session,
            ),
        ), patch.object(
            youtube_channel_tabs, 'generate_visitor_info',
            return_value='visitor-abc',
        ) as gen_visitor:
            pooled_innertube_for_entry('http://1.1.1.1:80')
            pooled_innertube_for_entry('http://1.1.1.1:80')
            pooled_innertube_for_entry('http://1.1.1.1:80')
        self.assertEqual(load_into_session.call_count, 1)
        self.assertEqual(gen_visitor.call_count, 1)

    async def test_different_entries_run_factory_separately(
        self,
    ) -> None:
        load_into_session: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs, 'InnerTube',
            return_value=MagicMock(adaptor=MagicMock(
                session=MagicMock(),
            )),
        ), patch.object(
            youtube_channel_tabs.httpx, 'Client',
            return_value=MagicMock(),
        ), patch.object(
            youtube_channel_tabs.YouTubeCookieJar, 'get',
            return_value=MagicMock(
                load_into_session=load_into_session,
            ),
        ), patch.object(
            youtube_channel_tabs, 'generate_visitor_info',
            return_value='visitor',
        ):
            pooled_innertube_for_entry('http://1.1.1.1:80')
            pooled_innertube_for_entry('http://2.2.2.2:80')
        self.assertEqual(load_into_session.call_count, 2)

    async def test_refresh_rebuilds_only_challenged_entry(
        self,
    ) -> None:
        first_proxy: str = 'http://localhost:8080'
        second_proxy: str = 'http://scrape.exchange:8080'
        clients: list[MagicMock] = [
            MagicMock()
            for _ in range(3)
        ]
        with patch.object(
            youtube_channel_tabs._PLAYER_INNERTUBE_POOL,
            'factory',
            side_effect=clients,
        ):
            first: object = pooled_player_innertube_for_entry(first_proxy)
            second: object = pooled_player_innertube_for_entry(second_proxy)
            await youtube_channel_tabs.refresh_pooled_innertube_for_entry(
                first_proxy, challenged=first,
            )
            rebuilt: object = pooled_player_innertube_for_entry(
                first_proxy,
            )

        self.assertIsNot(rebuilt, first)
        self.assertIs(
            pooled_player_innertube_for_entry(second_proxy),
            second,
        )
        clients[0].close.assert_called_once_with()
        clients[1].close.assert_not_called()

    async def test_stale_refresh_does_not_evict_replacement(
        self,
    ) -> None:
        proxy: str = 'http://localhost:8080'
        first: MagicMock = MagicMock()
        replacement: MagicMock = MagicMock()
        with patch.object(
            youtube_channel_tabs._PLAYER_INNERTUBE_POOL,
            'factory',
            side_effect=[first, replacement],
        ):
            original: object = pooled_player_innertube_for_entry(proxy)
            await youtube_channel_tabs.refresh_pooled_innertube_for_entry(
                proxy, challenged=first,
            )
            rebuilt: object = pooled_player_innertube_for_entry(proxy)
            retired: bool = await (
                youtube_channel_tabs.refresh_pooled_innertube_for_entry(
                    proxy, challenged=first,
                )
            )

        self.assertIs(original, first)
        self.assertIs(rebuilt, replacement)
        self.assertFalse(retired)
        first.close.assert_called_once_with()
        replacement.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
