'''
Unit tests for the video-min upload pipeline added to
``tools/yt_video_scrape.py``.

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

from tools.yt_video_scrape import (
    FILE_EXTENSION,
    VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_PREFIX,
    _is_video_upload_file,
    enqueue_upload_video,
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
        settings.video_concurrency = 4

        client: MagicMock = MagicMock()
        video_fm: MagicMock = MagicMock()
        # Both lists empty so upload_videos returns early — we only
        # care about which prefixes it queried.
        video_fm.list_base.return_value = []

        creator_map: MagicMock = MagicMock()
        validator: MagicMock = _permissive_validator()

        await upload_videos(
            settings, client, video_fm, creator_map, validator,
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
    The RSS scraper writes ``video-min-`` files but no longer
    enqueues them for upload — that responsibility moved to the
    video scraper's bulk and watch uploaders.  This test asserts
    the RSS path stops short of enqueuing so we don't regress into
    duplicate POSTs.
    '''

    async def test_no_enqueue_upload_call(self) -> None:
        video: MagicMock = MagicMock()
        video.video_id = 'abc'
        video.from_innertube = AsyncMock()
        video.to_file = AsyncMock(
            return_value='video-min-abc.json.br',
        )

        innertube: MagicMock = MagicMock()
        settings: MagicMock = MagicMock()
        settings.video_data_directory = '/tmp/test'

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
                await yt_rss_scrape._enrich_and_store_video(
                    video, innertube, None,
                    channel_handle='display-name',
                    settings=settings,
                )
            )

        self.assertEqual(filename, 'video-min-abc.json.br')
        upload_mock.assert_not_called()
        self.assertFalse(
            hasattr(yt_rss_scrape, 'enqueue_upload_video'),
        )


if __name__ == '__main__':
    unittest.main()
