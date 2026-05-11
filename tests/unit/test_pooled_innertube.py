'''Unit tests for the pooled InnerTube pool.'''

import unittest
from unittest.mock import MagicMock, patch

import scrape_exchange.youtube.youtube_channel_tabs as youtube_channel_tabs
from scrape_exchange.youtube.youtube_channel_tabs import (
    aclose_pooled_innertube,
    pooled_innertube_for_entry,
)


class TestPooledInnerTube(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        youtube_channel_tabs._reset_pool_for_tests()

    async def asyncTearDown(self) -> None:
        await aclose_pooled_innertube()

    async def test_same_entry_returns_same_instance(self) -> None:
        with patch.object(
            youtube_channel_tabs, 'InnerTube',
            return_value=MagicMock(adaptor=MagicMock(
                session=MagicMock(),
            )),
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


if __name__ == '__main__':
    unittest.main()
