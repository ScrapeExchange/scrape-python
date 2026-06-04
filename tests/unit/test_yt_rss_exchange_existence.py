'''
Tests for RSS scrape.exchange existence-check backpressure.
'''

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock

from httpx import Response

from tools import yt_rss_scrape


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestRssExchangeExistenceConcurrency(unittest.TestCase):

    def tearDown(self) -> None:
        os.environ.pop(
            'RSS_EXCHANGE_EXISTENCE_CONCURRENCY', None,
        )

    def test_setting_defaults_to_64(self) -> None:
        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )

        self.assertEqual(
            settings.rss_exchange_existence_concurrency,
            64,
        )

    def test_setting_reads_env_override(self) -> None:
        os.environ[
            'RSS_EXCHANGE_EXISTENCE_CONCURRENCY'
        ] = '32'

        settings = yt_rss_scrape.RssSettings(
            _env_file=None, _cli_parse_args=[],
        )

        self.assertEqual(
            settings.rss_exchange_existence_concurrency,
            32,
        )

    def test_gate_wait_happens_before_client_get(self) -> None:
        async def scenario() -> None:
            gate = asyncio.Semaphore(1)
            await gate.acquire()

            client = MagicMock()
            client.get = AsyncMock(
                return_value=Response(404),
            )
            settings = MagicMock()
            settings.exchange_url = 'https://scrape.exchange'

            task = asyncio.create_task(
                yt_rss_scrape.check_video_exists(
                    client, settings, 'video-id',
                    gate=gate,
                )
            )
            await asyncio.sleep(0)
            client.get.assert_not_called()

            gate.release()
            result = await task

            self.assertIs(result, False)
            client.get.assert_awaited_once()

        _run(scenario())
