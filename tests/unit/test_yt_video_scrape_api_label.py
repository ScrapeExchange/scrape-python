'''The api label on METRIC_SCRAPE_DURATION for the video scraper
must reflect the backend actually used:
    - 'ytdlp' when settings.video_use_yt_dlp is True
    - 'innertube' when settings.video_use_yt_dlp is False (default)

Prior behavior was a literal api='ytdlp' regardless of the
setting, which is incorrect since innertube is the default
backend.'''

import unittest

from unittest.mock import AsyncMock, MagicMock, patch

from tools import yt_video_scrape


class TestVideoScrapeApiLabel(
    unittest.IsolatedAsyncioTestCase,
):

    async def _run(
        self, video_use_yt_dlp: bool, raise_in_scrape: bool,
    ) -> MagicMock:
        '''Drive _scrape with the given setting and return the
        patched METRIC_SCRAPE_DURATION mock.'''
        settings: MagicMock = MagicMock()
        settings.video_use_yt_dlp = video_use_yt_dlp
        settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
        settings.video_data_directory = '/tmp/video-data'
        settings.log_level = 'INFO'

        async def fake_scrape(*args: object, **kwargs: object):
            if raise_in_scrape:
                raise RuntimeError('boom')
            return MagicMock()

        video_fm: MagicMock = MagicMock()
        with patch.object(
            yt_video_scrape.YouTubeVideo, 'scrape',
            side_effect=fake_scrape,
        ), patch.object(
            yt_video_scrape, '_handle_scrape_failure',
            new=AsyncMock(return_value=0),
        ), patch.object(
            yt_video_scrape, 'METRIC_SCRAPE_DURATION',
        ) as duration:
            await yt_video_scrape._scrape(
                entry='video-min-test.json',
                video_id='dQw4w9WgXcQ',
                channel_handle='Test',
                download_client=None,
                settings=settings,
                video_fm=video_fm,
                proxy='http://1.2.3.4:8080',
                sleep=0,
                from_priority=False,
            )
        return duration

    async def test_innertube_label_when_yt_dlp_disabled(
        self,
    ) -> None:
        duration: MagicMock = await self._run(
            video_use_yt_dlp=False, raise_in_scrape=False,
        )
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['api'], 'innertube')
        self.assertEqual(kwargs['outcome'], 'success')

    async def test_ytdlp_label_when_yt_dlp_enabled(self) -> None:
        duration: MagicMock = await self._run(
            video_use_yt_dlp=True, raise_in_scrape=False,
        )
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['api'], 'ytdlp')
        self.assertEqual(kwargs['outcome'], 'success')

    async def test_failure_path_uses_same_api_derivation(
        self,
    ) -> None:
        duration: MagicMock = await self._run(
            video_use_yt_dlp=False, raise_in_scrape=True,
        )
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['api'], 'innertube')
        self.assertEqual(kwargs['outcome'], 'failure')


class TestVideoScrapedCounterApiLabel(
    unittest.IsolatedAsyncioTestCase,
):
    '''METRIC_VIDEOS_SCRAPED (exposed as scrapes_completed_total
    {scraper="video_scraper"}) is emitted in _scrape_and_track on
    success. Pre-fix it was hardcoded api='ytdlp' regardless of
    backend, so per-network distribution was wrong when innertube
    is the default. Verify both labels.'''

    async def _drive(
        self, video_use_yt_dlp: bool,
    ) -> MagicMock:
        settings: MagicMock = MagicMock()
        settings.video_use_yt_dlp = video_use_yt_dlp

        claim: MagicMock = MagicMock()
        claim.acquire = AsyncMock(return_value=True)
        claim.release = AsyncMock()
        video: MagicMock = MagicMock()
        video.channel_handle = 'Test'

        with patch.object(
            yt_video_scrape, '_scrape_and_save',
            new=AsyncMock(return_value=(video, 0)),
        ), patch.object(
            yt_video_scrape, 'extract_proxy_ip',
            return_value='1.2.3.4',
        ), patch.object(
            yt_video_scrape, '_proxy_network',
            return_value='1.2.3.0/24',
        ), patch.object(
            yt_video_scrape, 'proxy_file_label',
            return_value='test.proxies.lst',
        ), patch.object(
            yt_video_scrape, 'METRIC_VIDEOS_SCRAPED',
        ) as completed:
            await yt_video_scrape._scrape_and_track(
                entry='video-min-X.json',
                video_id='X',
                video=video,
                from_uploaded=False,
                download_client=None,
                settings=settings,
                video_fm=MagicMock(),
                proxy='http://1.2.3.4:8080',
                sleep=0,
                claim=claim,
            )
        return completed

    async def test_innertube_label_default(self) -> None:
        completed: MagicMock = await self._drive(
            video_use_yt_dlp=False,
        )
        completed.labels.assert_called_once()
        self.assertEqual(
            completed.labels.call_args.kwargs['api'],
            'innertube',
        )

    async def test_ytdlp_label_when_enabled(self) -> None:
        completed: MagicMock = await self._drive(
            video_use_yt_dlp=True,
        )
        completed.labels.assert_called_once()
        self.assertEqual(
            completed.labels.call_args.kwargs['api'],
            'ytdlp',
        )


class TestScrapeAndSaveFailureApiLabel(
    unittest.IsolatedAsyncioTestCase,
):
    '''The wrapper failure counter in _scrape_and_save (the
    ``except Exception`` branch around _scrape) was also
    hardcoded api='ytdlp'. Verify it now mirrors the actual
    backend.'''

    async def _drive(
        self, video_use_yt_dlp: bool,
    ) -> MagicMock:
        settings: MagicMock = MagicMock()
        settings.video_use_yt_dlp = video_use_yt_dlp
        settings.video_data_directory = '/tmp/video-data'

        async def boom(*args: object, **kwargs: object):
            raise RuntimeError('boom')

        with patch.object(
            yt_video_scrape, '_scrape', side_effect=boom,
        ), patch.object(
            yt_video_scrape, 'extract_proxy_ip',
            return_value='1.2.3.4',
        ), patch.object(
            yt_video_scrape, '_proxy_network',
            return_value='1.2.3.0/24',
        ), patch.object(
            yt_video_scrape, 'proxy_file_label',
            return_value='test.proxies.lst',
        ), patch.object(
            yt_video_scrape, '_retire_failed_source',
            new=AsyncMock(),
        ), patch.object(
            yt_video_scrape, 'METRIC_SCRAPE_FAILURES',
        ) as failures:
            await yt_video_scrape._scrape_and_save(
                entry='video-min-X.json',
                video_id='X',
                channel_handle='Test',
                download_client=None,
                settings=settings,
                video_fm=MagicMock(),
                proxy='http://1.2.3.4:8080',
                sleep=0,
            )
        return failures

    async def test_innertube_label_default(self) -> None:
        failures: MagicMock = await self._drive(
            video_use_yt_dlp=False,
        )
        failures.labels.assert_called_once()
        self.assertEqual(
            failures.labels.call_args.kwargs['api'],
            'innertube',
        )

    async def test_ytdlp_label_when_enabled(self) -> None:
        failures: MagicMock = await self._drive(
            video_use_yt_dlp=True,
        )
        failures.labels.assert_called_once()
        self.assertEqual(
            failures.labels.call_args.kwargs['api'],
            'ytdlp',
        )


class TestHandleScrapeFailureApiLabel(
    unittest.IsolatedAsyncioTestCase,
):
    '''_handle_scrape_failure now takes the api label as an
    explicit parameter; it must thread it through to
    METRIC_SCRAPE_FAILURES.'''

    async def test_uses_passed_api_label(self) -> None:
        with patch.object(
            yt_video_scrape, '_classify_scrape_error',
            return_value='other',
        ), patch.object(
            yt_video_scrape, '_proxy_network',
            return_value='1.2.3.0/24',
        ), patch.object(
            yt_video_scrape, 'extract_proxy_ip',
            return_value='1.2.3.4',
        ), patch.object(
            yt_video_scrape, 'proxy_file_label',
            return_value='test.proxies.lst',
        ), patch.object(
            yt_video_scrape, 'METRIC_SCRAPE_FAILURES',
        ) as failures:
            await yt_video_scrape._handle_scrape_failure(
                exc=RuntimeError('boom'),
                proxy='http://1.2.3.4:8080',
                video_id='X',
                entry='video-min-X.json',
                video_fm=MagicMock(),
                sleep=0,
                api='innertube',
            )
        failures.labels.assert_called_once()
        self.assertEqual(
            failures.labels.call_args.kwargs['api'],
            'innertube',
        )


if __name__ == '__main__':
    unittest.main()
