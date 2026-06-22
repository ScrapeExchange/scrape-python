'''
Unit tests for the video-min upload pipeline added to
``tools/yt_video_upload.py``.

The video scraper now sweeps both ``video-min-*`` (RSS-discovered)
and ``video-dlp-*`` (yt-dlp-enriched) files through its bulk and
watch uploaders.  These tests verify three slices of that wiring:

* The bulk uploader's file enumeration includes both prefixes.
* The watch uploader's ``_is_video_upload_file`` filter accepts both
  prefixes.
* ``enqueue_upload_video`` builds the on-disk filename from its
  ``filename_prefix`` argument so callers can ship video-min records
  via the single-record POST path without renaming the file first.
'''

import importlib.util
import sys
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

from tools.yt_video_upload import (
    FILE_EXTENSION,
    VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_PREFIX,
    _is_video_upload_file,
    enqueue_upload_video,
    main as video_upload_main,
    upload_videos,
)


def _load_yt_rss_scrape() -> ModuleType:
    '''Load ``tools/yt_rss_scrape.py`` under the bare module name
    ``yt_rss_scrape`` so its top-level Prometheus metrics are
    registered exactly once across the suite.  The other RSS tests
    (test_rss_settings, test_yt_rss_scrape_handle, ...) use the same
    cache key — using ``tools.yt_rss_scrape`` here would trigger a
    duplicate-registration error during full discovery.
    '''
    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']

    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_rss_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


def _permissive_validator() -> MagicMock:
    '''A SchemaValidator stand-in that always passes.'''
    validator: MagicMock = MagicMock()
    validator.validate = MagicMock(return_value=None)
    return validator


class TestIsVideoUploadFile(unittest.TestCase):
    '''The watch uploader's filter must trigger on both
    video-min and video-dlp files and ignore anything else.'''

    def test_video_dlp_accepted(self) -> None:
        self.assertTrue(_is_video_upload_file(
            f'{VIDEO_YTDLP_PREFIX}abc{FILE_EXTENSION}',
        ))

    def test_video_min_accepted(self) -> None:
        self.assertTrue(_is_video_upload_file(
            f'{VIDEO_MIN_PREFIX}abc{FILE_EXTENSION}',
        ))

    def test_other_prefix_rejected(self) -> None:
        self.assertFalse(_is_video_upload_file(
            f'channel-detail-abc{FILE_EXTENSION}',
        ))

    def test_wrong_suffix_rejected(self) -> None:
        self.assertFalse(_is_video_upload_file(
            f'{VIDEO_MIN_PREFIX}abc.json',
        ))


class TestUploadVideosEnumeratesBothPrefixes(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    ``upload_videos`` must list video-min and video-dlp files in
    base_dir and enumerate them all when building bulk batches.  We
    short-circuit by returning an empty file list (so the function
    exits before actually POSTing) and inspect the calls made on the
    file-management mock.
    '''

    async def test_lists_both_prefixes(self) -> None:
        settings: MagicMock = MagicMock()
        settings.video_data_directory = '/tmp/test'
        settings.proxies = ''
        settings.bulk_batch_size = 100
        settings.bulk_max_batch_bytes = 200_000_000
        settings.video_upload_concurrency = 4

        client: MagicMock = MagicMock()
        video_fm: MagicMock = MagicMock()
        # Both lists empty so upload_videos returns early — we only
        # care about which prefixes it queried.
        video_fm.list_base.return_value = []

        creator_map: MagicMock = MagicMock()
        validator: MagicMock = _permissive_validator()

        await upload_videos(
            settings, client, video_fm, creator_map, validator,
            AsyncMock(),
        )

        called_prefixes: set[str] = {
            kwargs.get('prefix')
            for _args, kwargs in video_fm.list_base.call_args_list
        }
        self.assertIn(VIDEO_YTDLP_PREFIX, called_prefixes)
        self.assertIn(VIDEO_MIN_PREFIX, called_prefixes)


class TestEnqueueUploadVideoFilenamePrefix(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    ``enqueue_upload_video`` must build the on-disk filename from its
    ``filename_prefix`` keyword argument so an upload-only worker can
    ship a video-min file (instead of inventing a video-dlp filename
    that does not exist on disk).
    '''

    def _setup_video_and_settings(
        self,
    ) -> tuple[MagicMock, MagicMock, MagicMock]:
        video: MagicMock = MagicMock()
        video.video_id = 'abcXYZ'
        video.url = 'https://youtu.be/abcXYZ'
        video.to_dict = MagicMock(
            return_value={'video_id': 'abcXYZ'},
        )

        client: MagicMock = MagicMock()
        client.enqueue_upload = MagicMock(return_value=True)

        settings: MagicMock = MagicMock()
        settings.exchange_url = 'http://test'
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        return video, client, settings

    async def test_default_prefix_is_video_dlp(self) -> None:
        '''Backwards-compatible default — existing call sites stay
        on ``video-dlp-`` without passing ``filename_prefix``.'''
        video, client, settings = self._setup_video_and_settings()
        video_fm: AsyncMock = AsyncMock()

        ok: bool = await enqueue_upload_video(
            client, settings, video_fm, 'handle', video,
            _permissive_validator(),
        )

        self.assertTrue(ok)
        client.enqueue_upload.assert_called_once()
        kwargs: dict = client.enqueue_upload.call_args.kwargs
        self.assertEqual(
            kwargs['filename'],
            f'{VIDEO_YTDLP_PREFIX}{video.video_id}{FILE_EXTENSION}',
        )

    async def test_video_min_prefix_propagated(self) -> None:
        '''Passing ``filename_prefix=video-min-`` must produce a
        ``video-min-{id}.json.br`` filename so the post-upload
        ``mark_uploaded`` lines up with the actual on-disk file.'''
        video, client, settings = self._setup_video_and_settings()
        video_fm: AsyncMock = AsyncMock()

        ok: bool = await enqueue_upload_video(
            client, settings, video_fm, 'handle', video,
            _permissive_validator(),
            filename_prefix=VIDEO_MIN_PREFIX,
        )

        self.assertTrue(ok)
        kwargs: dict = client.enqueue_upload.call_args.kwargs
        self.assertEqual(
            kwargs['filename'],
            f'{VIDEO_MIN_PREFIX}{video.video_id}{FILE_EXTENSION}',
        )


class TestRssEnrichDoesNotUpload(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    The RSS scraper enqueues video ids onto the Redis-backed scrape
    queue but no longer enqueues them for upload — that
    responsibility moved to the video scraper.  This test asserts
    the RSS path stops short of enqueuing for upload so we don't
    regress into duplicate POSTs.
    '''

    async def test_no_enqueue_upload_call(self) -> None:
        video: MagicMock = MagicMock()
        video.video_id = 'abc'
        video.from_innertube = AsyncMock()
        video.to_file = AsyncMock(
            return_value='video-min-abc.json.br',
        )

        video_queue: AsyncMock = AsyncMock()
        video_queue.enqueue = AsyncMock()

        # ``enqueue_upload_video`` no longer exists on yt_rss_scrape;
        # patching it would error.  Instead, monkey-patch the
        # ``ExchangeClient.enqueue_upload`` symbol so that any
        # accidental upload call would be observable, then assert
        # it was never invoked.
        with patch(
            'scrape_exchange.exchange_client.'
            'ExchangeClient.enqueue_upload',
            new=MagicMock(return_value=True),
        ) as upload_mock:
            filename: str | None = (
                await yt_rss_scrape._queue_video_for_scrape(
                    video,
                    channel_handle='display-name',
                    video_queue=video_queue,
                )
            )

        self.assertEqual(filename, 'abc')
        upload_mock.assert_not_called()
        video_queue.enqueue.assert_awaited_once_with(
            'abc',
            source='rss',
            channel_id=None,
            channel_handle='display-name',
            channel_url=None,
            channel_is_verified=None,
        )
        video.from_innertube.assert_not_awaited()
        video.to_file.assert_not_awaited()
        self.assertFalse(
            hasattr(yt_rss_scrape, 'enqueue_upload_video'),
        )


class TestVideoUploadMain(unittest.TestCase):
    def test_main_always_runs_single_process(self) -> None:
        settings: MagicMock = MagicMock()
        settings.video_upload_concurrency = 3
        settings.proxies = ['http://proxy-1', 'http://proxy-2']
        settings.metrics_port = 9399
        settings.video_upload_log_file = '/dev/stdout'
        settings.video_upload_log_level = 'INFO'

        runner: MagicMock = MagicMock()
        runner.run_sync.return_value = 0

        with (
            patch('tools.yt_video_upload.VideoUploadSettings',
                  return_value=settings),
            patch('tools.yt_video_upload.ScraperRunner',
                  return_value=runner) as runner_cls,
            patch('tools.yt_video_upload.sys.exit') as exit_mock,
        ):
            video_upload_main()

        runner_cls.assert_called_once()
        self.assertEqual(
            runner_cls.call_args.kwargs['num_processes'],
            1,
        )
        exit_mock.assert_called_once_with(0)


if __name__ == '__main__':
    unittest.main()
