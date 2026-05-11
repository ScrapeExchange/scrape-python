'''Unit tests for the cookie-fetch jitter helper in
scrape_exchange.youtube.youtube_cookiejar.

Mirrors the structure of
tests/unit/test_proxy_loader.py::TestJitterPoolWarmup so the
two herd-breaking helpers stay consistent.
'''

import asyncio
import unittest
import unittest.mock


class TestJitterFirstCookieFetch(unittest.IsolatedAsyncioTestCase):
    '''The cookie-fetch jitter helper sleeps a uniform-random
    0..COOKIE_FETCH_JITTER_MAX_SECONDS the first time it is
    awaited for a given proxy in the current process. Subsequent
    calls for the same proxy are no-ops. Used to spread the
    fleet-wide cookie-acquisition stampede that otherwise saturates
    the proxy infrastructure when N worker processes boot together.
    '''

    def setUp(self) -> None:
        from scrape_exchange.youtube import youtube_cookiejar
        youtube_cookiejar._reset_jitter_for_tests()

    async def test_first_call_sleeps_within_window(self) -> None:
        from scrape_exchange.youtube.youtube_cookiejar import (
            jitter_first_cookie_fetch,
            COOKIE_FETCH_JITTER_MAX_SECONDS,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.random.uniform',
            return_value=4.2,
        ), unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_first_cookie_fetch('http://a:3128')
        self.assertEqual(slept, [4.2])
        self.assertGreaterEqual(
            COOKIE_FETCH_JITTER_MAX_SECONDS, 4.2,
        )

    async def test_second_call_for_same_proxy_does_not_sleep(
        self,
    ) -> None:
        from scrape_exchange.youtube.youtube_cookiejar import (
            jitter_first_cookie_fetch,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.random.uniform',
            return_value=10.0,
        ), unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_first_cookie_fetch('http://a:3128')
            await jitter_first_cookie_fetch('http://a:3128')
            await jitter_first_cookie_fetch('http://a:3128')
        self.assertEqual(slept, [10.0])

    async def test_distinct_proxies_each_sleep_once(self) -> None:
        from scrape_exchange.youtube.youtube_cookiejar import (
            jitter_first_cookie_fetch,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.random.uniform',
            side_effect=[1.0, 2.0, 3.0],
        ), unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_first_cookie_fetch('http://a:3128')
            await jitter_first_cookie_fetch('http://b:3128')
            await jitter_first_cookie_fetch('http://c:3128')
            await jitter_first_cookie_fetch('http://a:3128')
            await jitter_first_cookie_fetch('http://b:3128')
        self.assertEqual(slept, [1.0, 2.0, 3.0])

    async def test_concurrent_first_use_serializes(self) -> None:
        '''Two coroutines awaiting the same fresh proxy at the
        same time must not both sleep — the lock serialises them
        and only the first does the work.'''
        from scrape_exchange.youtube.youtube_cookiejar import (
            jitter_first_cookie_fetch,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.random.uniform',
            return_value=5.0,
        ), unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await asyncio.gather(
                jitter_first_cookie_fetch('http://a:3128'),
                jitter_first_cookie_fetch('http://a:3128'),
                jitter_first_cookie_fetch('http://a:3128'),
            )
        self.assertEqual(slept, [5.0])

    async def test_none_proxy_jitters_independently(self) -> None:
        '''The direct (no-proxy) path takes its own slot in the
        seen-set so it is jittered exactly once.'''
        from scrape_exchange.youtube.youtube_cookiejar import (
            jitter_first_cookie_fetch,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.random.uniform',
            side_effect=[7.0, 8.0],
        ), unittest.mock.patch(
            'scrape_exchange.youtube.youtube_cookiejar.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_first_cookie_fetch(None)
            await jitter_first_cookie_fetch('http://a:3128')
            await jitter_first_cookie_fetch(None)
        self.assertEqual(slept, [7.0, 8.0])
