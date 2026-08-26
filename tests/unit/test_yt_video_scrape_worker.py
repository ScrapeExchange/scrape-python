'''Tests for _scrape_one_queued's retry contract.'''

import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.video_scrape_queue import (
    VideoState,
)
from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.youtube.youtube_video_innertube import (
    YouTubeBotDetectionError,
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


def _creator_map() -> InMemoryCreatorMap:
    return InMemoryCreatorMap()


class TestScrapeOneQueued(
    unittest.IsolatedAsyncioTestCase,
):

    @patch(
        'tools.yt_video_scrape._scrape_to_disk',
        new_callable=AsyncMock,
    )
    async def test_failure_log_has_context_without_proxy_url(
        self,
        mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = RuntimeError('unexpected failure')
        queue: AsyncMock = AsyncMock()
        settings: MagicMock = _mock_settings()
        settings.video_transient_max_attempts = 1
        settings.video_use_yt_dlp = False
        proxy: str = 'http://user:secret@localhost:8080'
        from tools.yt_video_scrape import _scrape_one_queued

        with self.assertLogs(level='WARNING') as logs:
            await _scrape_one_queued(
                'video-123',
                queue=queue,
                settings=settings,
                proxies=[proxy],
                uploaded=_mock_uploaded(),
                creator_map_backend=_creator_map(),
            )

        record: logging.LogRecord = logs.records[0]
        self.assertEqual(getattr(record, 'video_id'), 'video-123')
        self.assertEqual(getattr(record, 'reason'), 'other')
        self.assertEqual(getattr(record, 'api'), 'innertube')
        self.assertTrue(getattr(record, 'worker_id'))
        self.assertEqual(getattr(record, 'proxy_ip'), 'localhost')
        self.assertEqual(getattr(record, 'proxy_port'), '8080')
        self.assertNotIn(proxy, logs.output[0])
        self.assertNotIn('secret', logs.output[0])
        self.assertFalse(hasattr(record, 'proxy'))
        self.assertIsNotNone(record.exc_info)

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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
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
    async def test_bot_detection_retries_with_different_proxy(
        self, mock_scrape: AsyncMock,
    ) -> None:
        mock_scrape.side_effect = [
            YouTubeBotDetectionError('bot challenge'),
            None,
        ]
        queue: AsyncMock = AsyncMock()
        first_proxy: str = 'http://localhost:8080'
        second_proxy: str = 'http://scrape.exchange:8080'
        selections: list[list[str]] = []

        def choose(candidates: list[str]) -> str:
            selections.append(candidates.copy())
            return candidates[0]

        from tools.yt_video_scrape import _scrape_one_queued
        with patch(
            'tools.yt_video_scrape.random.choice',
            side_effect=choose,
        ):
            await _scrape_one_queued(
                'aaa',
                queue=queue,
                settings=_mock_settings(),
                proxies=[first_proxy, second_proxy],
                uploaded=_mock_uploaded(),
                creator_map_backend=_creator_map(),
            )

        self.assertEqual(
            selections,
            [
                [first_proxy, second_proxy],
                [second_proxy],
            ],
        )
        self.assertEqual(mock_scrape.await_count, 2)
        self.assertEqual(
            mock_scrape.await_args_list[0].kwargs['proxy'],
            first_proxy,
        )
        self.assertEqual(
            mock_scrape.await_args_list[1].kwargs['proxy'],
            second_proxy,
        )
        queue.bump_attempts.assert_awaited_once_with(
            'aaa', last_error='bot_detection',
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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
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
            creator_map_backend=_creator_map(),
        )
        mock_scrape.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
