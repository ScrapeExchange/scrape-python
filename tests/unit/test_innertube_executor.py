'''Tests for the dedicated InnerTube thread-pool executor.

The synchronous InnerTube player/next/browse calls must run on a
dedicated executor, isolated from the default ThreadPoolExecutor, so a
wedged InnerTube call cannot starve the cookie-file/brotli ``to_thread``
work that keeps the worker alive.
'''

import asyncio
import threading
import unittest

from scrape_exchange.youtube.youtube_channel_tabs import (
    configure_innertube_executor,
    innertube_executor,
    shutdown_innertube_executor,
)


class TestInnertubeExecutor(unittest.TestCase):
    def setUp(self) -> None:
        shutdown_innertube_executor()

    def tearDown(self) -> None:
        shutdown_innertube_executor()

    def test_executor_is_singleton(self) -> None:
        configure_innertube_executor(4)
        first = innertube_executor()
        second = innertube_executor()
        self.assertIs(first, second)

    def test_configured_size_is_applied(self) -> None:
        configure_innertube_executor(3)
        self.assertEqual(innertube_executor()._max_workers, 3)

    def test_threads_named_innertube(self) -> None:
        configure_innertube_executor(2)

        async def _body() -> str:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                innertube_executor(),
                lambda: threading.current_thread().name,
            )

        name: str = asyncio.run(_body())
        self.assertTrue(
            name.startswith('innertube'), msg=name,
        )

    def test_block_does_not_starve_default_executor(self) -> None:
        # Fill the only InnerTube thread with a blocked call, then prove
        # a default-executor to_thread call still completes.
        configure_innertube_executor(1)

        async def _body() -> str:
            loop = asyncio.get_running_loop()
            started: threading.Event = threading.Event()
            release: threading.Event = threading.Event()

            def blocker() -> None:
                started.set()
                release.wait(5.0)

            fut = loop.run_in_executor(
                innertube_executor(), blocker,
            )
            self.assertTrue(started.wait(2.0))
            try:
                result: str = await asyncio.wait_for(
                    asyncio.to_thread(lambda: 'ok'), 2.0,
                )
            finally:
                release.set()
                await fut
            return result

        self.assertEqual(asyncio.run(_body()), 'ok')


class TestCallInnertubeRouting(unittest.TestCase):
    def setUp(self) -> None:
        shutdown_innertube_executor()

    def tearDown(self) -> None:
        shutdown_innertube_executor()

    def test_call_innertube_uses_dedicated_executor(self) -> None:
        from scrape_exchange.youtube.youtube_video_innertube import (
            _call_innertube,
        )
        configure_innertube_executor(2)

        def fn() -> str:
            return threading.current_thread().name

        name: str = asyncio.run(_call_innertube(object(), fn))
        self.assertTrue(name.startswith('innertube'), msg=name)


if __name__ == '__main__':
    unittest.main()
