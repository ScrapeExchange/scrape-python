'''Tests for _scrape_one_queued's retry contract.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.video_scrape_queue import (
    VideoState,
)


def _mock_settings() -> MagicMock:
    s: MagicMock = MagicMock()
    s.video_transient_max_attempts = 3
    s.video_transient_backoff_seconds = 0
    return s


def _mock_uploaded(is_uploaded: bool = False) -> AsyncMock:
    uploaded: AsyncMock = AsyncMock()
    uploaded.contains.return_value = is_uploaded
    return uploaded


class TestScrapeOneQueued(
    unittest.IsolatedAsyncioTestCase,
):

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_success_calls_complete(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.return_value = None
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=['http://test-proxy:8080'],
            uploaded=_mock_uploaded(),
        )
        queue.complete.assert_awaited_once_with('aaa')
        queue.mark.assert_not_called()

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_unavailable_marks(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = RuntimeError(
            'this video is private',
        )
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=['http://test-proxy:8080'],
            uploaded=_mock_uploaded(),
        )
        queue.mark.assert_awaited_once()
        kwargs: dict = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            VideoState.UNAVAILABLE,
        )

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_transient_retries_then_failed(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = [
            RuntimeError(' 429 '),
            RuntimeError(' 429 '),
            RuntimeError(' 429 '),
        ]
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=['http://test-proxy:8080'],
            uploaded=_mock_uploaded(),
        )
        self.assertEqual(
            mock_scrape.await_count, 3,
        )
        self.assertEqual(
            queue.bump_attempts.await_count, 3,
        )
        queue.mark.assert_awaited_once()
        kwargs: dict = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'], VideoState.FAILED,
        )

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_transient_then_success(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = [
            RuntimeError(' 429 '),
            RuntimeError(' 429 '),
            None,
        ]
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=['http://test-proxy:8080'],
            uploaded=_mock_uploaded(),
        )
        self.assertEqual(
            mock_scrape.await_count, 3,
        )
        self.assertEqual(
            queue.bump_attempts.await_count, 2,
        )
        queue.complete.assert_awaited_once_with('aaa')
        queue.mark.assert_not_called()

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_unknown_reason_marks_failed(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = RuntimeError(
            'totally weird error string',
        )
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import (
            _scrape_one_queued,
        )
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=['http://test-proxy:8080'],
            uploaded=_mock_uploaded(),
        )
        queue.mark.assert_awaited_once()
        kwargs: dict = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'], VideoState.FAILED,
        )

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_short_circuits_when_uploaded(
        self, mock_scrape: AsyncMock,
    ) -> None:
        queue: AsyncMock = AsyncMock()
        # Not forced: the uploaded skip applies.
        queue.consume_force.return_value = False
        from tools.yt_video_scrape import _scrape_one_queued
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=[],
            uploaded=_mock_uploaded(True),
        )
        queue.consume_force.assert_awaited_once_with('aaa')
        queue.complete.assert_awaited_once_with('aaa')
        mock_scrape.assert_not_awaited()

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_forced_uploaded_is_scraped(
        self, mock_scrape: AsyncMock,
    ) -> None:
        queue: AsyncMock = AsyncMock()
        # Forced: bypass the uploaded skip and scrape anyway.
        queue.consume_force.return_value = True
        from tools.yt_video_scrape import _scrape_one_queued
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=[],
            uploaded=_mock_uploaded(True),
        )
        queue.consume_force.assert_awaited_once_with('aaa')
        mock_scrape.assert_awaited_once()
        # Successful scrape still completes the item.
        queue.complete.assert_awaited_once_with('aaa')

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_proceeds_when_not_uploaded(
        self, mock_scrape: AsyncMock,
    ) -> None:
        queue: AsyncMock = AsyncMock()
        from tools.yt_video_scrape import _scrape_one_queued
        await _scrape_one_queued(
            'aaa',
            queue=queue,
            settings=_mock_settings(),
            proxies=[],
            uploaded=_mock_uploaded(False),
        )
        mock_scrape.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
