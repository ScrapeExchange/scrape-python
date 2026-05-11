'''_do_scrape_channel_to_disk must emit METRIC_SCRAPE_DURATION
observations only on the paths that pair with the success and
failure counters: success (at the existing
METRIC_CHANNELS_SCRAPED.inc site) and the early return after
_try_scrape_channel reports ok=False (paired with the
METRIC_SCRAPE_FAILURES.inc call inside _record_scrape_failure).
The "no content" and "persist failed" early returns must not
observe — they don't increment the success/failure counters
either.'''

import unittest

from typing import Any
from unittest.mock import MagicMock, patch

from tools import yt_channel_scrape


class TestDoScrapeChannelToDiskLatency(
    unittest.IsolatedAsyncioTestCase,
):

    async def _run(
        self,
        scenario: str,
    ) -> MagicMock:
        '''Drive _do_scrape_channel_to_disk through one of:
            'success', 'try_scrape_failed',
            'no_content', 'persist_failed'.
        Returns the patched METRIC_SCRAPE_DURATION mock so the
        caller can assert on it.'''

        async def stub_try_scrape(
            channel: Any, settings: Any, fm: Any,
            channel_handle: str, extra: dict[str, str],
        ) -> tuple[bool, str | None]:
            if scenario == 'try_scrape_failed':
                return False, None
            return True, 'http://1.2.3.4:8080'

        def stub_no_content(
            channel: Any, ip: str, network: str,
            proxy: str | None, handle: str,
        ) -> bool:
            return scenario == 'no_content'

        async def stub_persist(
            fm: Any, filename: str, channel: Any,
            channel_handle: str,
        ) -> bool:
            return scenario != 'persist_failed'

        settings: MagicMock = MagicMock()
        settings.channel_data_directory = '/tmp/scrape-test'
        fm: MagicMock = MagicMock()

        with patch.object(
            yt_channel_scrape, 'YouTubeChannel',
        ), patch.object(
            yt_channel_scrape, '_try_scrape_channel',
            side_effect=stub_try_scrape,
        ), patch.object(
            yt_channel_scrape, '_channel_has_no_content',
            side_effect=stub_no_content,
        ), patch.object(
            yt_channel_scrape, '_persist_scraped_channel',
            side_effect=stub_persist,
        ), patch.object(
            yt_channel_scrape, 'METRIC_SCRAPE_DURATION',
        ) as duration, patch.object(
            yt_channel_scrape, 'METRIC_CHANNELS_SCRAPED',
        ):
            await yt_channel_scrape._do_scrape_channel_to_disk(
                settings=settings, fm=fm,
                channel_handle='@test',
                filename='test',
                extra={},
            )
        return duration

    async def test_success_records_one_observation(self) -> None:
        duration: MagicMock = await self._run('success')
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['outcome'], 'success')
        self.assertEqual(kwargs['scraper'], 'channel_scraper')
        self.assertEqual(kwargs['entity'], 'channel')
        self.assertEqual(kwargs['api'], 'html')
        self.assertEqual(kwargs['platform'], 'youtube')
        duration.labels.return_value.observe.assert_called_once()

    async def test_try_scrape_failed_records_failure(self) -> None:
        duration: MagicMock = await self._run('try_scrape_failed')
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['outcome'], 'failure')
        self.assertEqual(kwargs['scraper'], 'channel_scraper')
        self.assertEqual(kwargs['api'], 'html')
        duration.labels.return_value.observe.assert_called_once()

    async def test_no_content_does_not_observe(self) -> None:
        duration: MagicMock = await self._run('no_content')
        duration.labels.assert_not_called()

    async def test_persist_failed_does_not_observe(self) -> None:
        duration: MagicMock = await self._run('persist_failed')
        duration.labels.assert_not_called()


if __name__ == '__main__':
    unittest.main()
