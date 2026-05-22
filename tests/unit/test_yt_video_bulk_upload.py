'''
Unit tests for the bulk-upload glue in the video scraper.

The reconciliation/WS helpers themselves are exercised in
``test_yt_channel_bulk_upload.py``; here we cover the
video-side bits — record collection (filename parsing, handle
resolution, video_id presence) and the orchestration that
batches files and dispatches to the shared bulk pipeline.
'''

import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_thumbnail import YouTubeThumbnail
from scrape_exchange.youtube.youtube_video import YouTubeVideo


def _permissive_validator() -> MagicMock:
    '''A SchemaValidator stand-in that always passes, so existing
    test cases that don't care about validation can ignore it.'''
    validator = MagicMock()
    validator.validate = MagicMock(return_value=None)
    return validator


def _thumbnail(
    url: str | None = 'https://i.ytimg.com/vi/abc/hqdefault.jpg',
) -> YouTubeThumbnail:
    return YouTubeThumbnail({
        'url': url,
        'width': 480,
        'height': 360,
    })


class TestCollectVideoRecord(unittest.IsolatedAsyncioTestCase):

    async def test_returns_record_on_happy_path(self) -> None:
        '''A well-formed video-dlp file yields
        ``(video_id, record_dict)``.'''
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = 'abc123XYZ'
        video.to_dict = MagicMock(return_value={
            'video_id': 'abc123XYZ',
            'channel_handle': 'old',
            'url': 'https://youtu.be/abc123XYZ',
        })
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        creator_map = AsyncMock()

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='canonical_handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, creator_map, None,
                _permissive_validator(),
            )

        self.assertIsNotNone(result)
        video_id, record = result
        self.assertEqual(video_id, 'abc123XYZ')
        self.assertIs(record, video.to_dict.return_value)
        # Canonical handle must be written back so the bulk
        # worker's marker resolution sees it.
        self.assertEqual(video.channel_handle, 'canonical_handle')

    async def test_missing_channel_url_uses_resolved_handle(self) -> None:
        '''When a cached video lacks ``channel_url``, collection
        fills it from the resolved handle before serialization.'''
        from tools.yt_video_upload import _collect_video_record

        channel_handle: str = 'canonical_handle'
        expected_url: str = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
            channel_handle=channel_handle,
        )
        video = YouTubeVideo(video_id='abc123XYZ')
        video.channel_url = None
        video.url = 'https://youtu.be/abc123XYZ'
        video.thumbnails = {'high': _thumbnail()}
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        creator_map = AsyncMock()

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value=channel_handle),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, creator_map, None,
                _permissive_validator(),
            )

        self.assertIsNotNone(result)
        _video_id, record = result
        self.assertEqual(record['channel_url'], expected_url)
        self.assertEqual(video.channel_url, expected_url)

    async def test_missing_url_is_derived_from_video_id(self) -> None:
        '''Older cached video files may have ``video_id`` but no
        ``url``.  The upload schema requires a string URL, so record
        collection must produce the canonical watch URL before
        validation.'''
        from tools.yt_video_upload import _collect_video_record

        video = YouTubeVideo(video_id='upoNT5xvPvg')
        video.url = None
        video.thumbnails = {'high': _thumbnail()}
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        creator_map = AsyncMock()

        validator = MagicMock()
        validator.validate = MagicMock(
            side_effect=lambda record: (
                None if isinstance(record.get('url'), str)
                else "/url: None is not of type 'string'"
            ),
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='canonical_handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-upoNT5xvPvg.json.br',
                settings, video_fm, creator_map, None,
                validator,
            )

        self.assertIsNotNone(result)
        _video_id, record = result
        self.assertEqual(
            record['url'],
            'https://www.youtube.com/watch?v=upoNT5xvPvg',
        )
        video_fm.mark_invalid.assert_not_called()

    async def test_unrecognised_filename_skipped(self) -> None:
        '''Filenames not matching video-min-/video-dlp- prefixes
        are skipped without touching disk.'''
        from tools.yt_video_upload import _collect_video_record

        with patch(
            'tools.yt_video_upload._load_video_file',
        ) as load_mock:
            result = await _collect_video_record(
                'random-channels-batch.jsonl',
                MagicMock(), AsyncMock(), AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)
        load_mock.assert_not_called()

    async def test_load_failure_skipped(self) -> None:
        '''When ``_load_video_file`` returns ``None`` (read
        error, corrupt brotli) the record is skipped.'''
        from tools.yt_video_upload import _collect_video_record

        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=None),
        ):
            result = await _collect_video_record(
                'video-dlp-abc.json.br',
                settings, AsyncMock(), AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)

    async def test_unresolved_handle_marked_invalid(self) -> None:
        '''Handle resolution failure marks the source file invalid
        instead of retrying a record that cannot get a
        ``platform_creator_id``.'''
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = 'abc123XYZ'
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abc123XYZ.json.br.invalid',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value=None),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )

    async def test_handle_with_spaces_marked_invalid(self) -> None:
        '''Resolved handles with spaces cannot be valid
        platform_creator_id values, so the source file is marked
        invalid instead of uploaded.'''
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = 'abc123XYZ'
        video.to_dict = MagicMock(return_value={
            'video_id': 'abc123XYZ',
            'channel_handle': 'bad handle',
            'url': 'https://youtu.be/abc123XYZ',
        })
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abc123XYZ.json.br.invalid',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='bad handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, AsyncMock(), None,
                _permissive_validator(),
            )

        self.assertIsNone(result)
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )
        video.to_dict.assert_not_called()

    async def test_whitespace_only_fallback_handle_marked_invalid(
        self,
    ) -> None:
        '''A whitespace-only channel_handle should not let
        fallback_handle raise out of collection; the file should be
        marked invalid through the normal no-handle path.'''
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = 'abc123XYZ'
        video.channel_id = None
        video.channel_handle = ' ...'
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abc123XYZ.json.br.invalid',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, AsyncMock(), None,
                _permissive_validator(),
            )

        self.assertIsNone(result)
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )

    async def test_missing_thumbnails_marked_invalid(self) -> None:
        '''A video with no thumbnails is not uploadable and its
        source file is marked invalid.'''
        from tools.yt_video_upload import _collect_video_record

        video = YouTubeVideo(video_id='abc123XYZ')
        video.url = 'https://youtu.be/abc123XYZ'
        video.thumbnails = {}
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abc123XYZ.json.br.invalid',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='canonical_handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, AsyncMock(), None,
                _permissive_validator(),
            )

        self.assertIsNone(result)
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )

    async def test_thumbnail_without_url_marked_invalid(self) -> None:
        '''Every thumbnail must carry a URL before the video can be
        uploaded.'''
        from tools.yt_video_upload import _collect_video_record

        video = YouTubeVideo(video_id='abc123XYZ')
        video.url = 'https://youtu.be/abc123XYZ'
        video.thumbnails = {
            'high': _thumbnail(),
            'broken': _thumbnail(url=None),
        }
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abc123XYZ.json.br.invalid',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='canonical_handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, video_fm, AsyncMock(), None,
                _permissive_validator(),
            )

        self.assertIsNone(result)
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )

    async def test_missing_video_id_skipped(self) -> None:
        '''A loaded video with no video_id can't be tracked
        through bulk results; it must be skipped.'''
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = ''
        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='handle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abc.json.br',
                settings, AsyncMock(), AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)


class TestCollectVideoRecordValidator(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_invalid_record_marked_invalid_and_skipped(
        self,
    ) -> None:
        from tools.yt_video_upload import _collect_video_record

        video = MagicMock()
        video.video_id = 'abcXYZ'
        video.to_dict = MagicMock(
            return_value={'video_id': 'abcXYZ'},
        )

        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abcXYZ.json.br.invalid',
        )

        validator = MagicMock()
        validator.validate = MagicMock(
            return_value='/url: required field missing',
        )

        with patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_upload.resolve_video_upload_handle',
            new=AsyncMock(return_value='somehandle'),
        ):
            result = await _collect_video_record(
                'video-dlp-abcXYZ.json.br',
                settings, video_fm, AsyncMock(),
                None, validator,
            )

        self.assertIsNone(result)
        validator.validate.assert_called_once()
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abcXYZ.json.br',
        )


class TestEnqueueUploadVideoValidator(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_invalid_record_not_enqueued_and_marked(
        self,
    ) -> None:
        from tools.yt_video_upload import enqueue_upload_video

        video = MagicMock()
        video.video_id = 'abcXYZ'
        video.url = 'https://youtu.be/abcXYZ'
        video.to_dict = MagicMock(
            return_value={'video_id': 'abcXYZ'},
        )

        client = MagicMock()
        client.enqueue_upload = MagicMock(return_value=True)

        video_fm = AsyncMock()
        video_fm.mark_invalid = AsyncMock(
            return_value='video-dlp-abcXYZ.json.br.invalid',
        )

        validator = MagicMock()
        validator.validate = MagicMock(
            return_value='/url: required field missing',
        )

        settings = MagicMock()
        settings.exchange_url = 'http://test'
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'

        ok: bool = await enqueue_upload_video(
            client, settings, video_fm, 'somehandle', video,
            validator,
        )
        self.assertFalse(ok)
        client.enqueue_upload.assert_not_called()
        video_fm.mark_invalid.assert_awaited_once_with(
            'video-dlp-abcXYZ.json.br',
        )

    async def test_success_callback_adds_uploaded_video_id(
        self,
    ) -> None:
        from tools.yt_video_upload import enqueue_upload_video

        video = MagicMock()
        video.video_id = 'abcXYZ'
        video.url = 'https://youtu.be/abcXYZ'
        video.to_dict = MagicMock(
            return_value={'video_id': 'abcXYZ'},
        )

        client = MagicMock()
        client.enqueue_upload = MagicMock(return_value=True)
        video_fm = AsyncMock()
        settings = MagicMock()
        settings.exchange_url = 'http://test'
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'

        uploaded: AsyncMock = AsyncMock()
        ok: bool = await enqueue_upload_video(
            client, settings, video_fm, 'somehandle', video,
            _permissive_validator(),
            uploaded=uploaded,
        )
        self.assertTrue(ok)
        on_success = client.enqueue_upload.call_args.kwargs[
            'on_success'
        ]
        await on_success()
        uploaded.add.assert_awaited_once_with('abcXYZ')


class TestBulkUploadedVideoIds(unittest.IsolatedAsyncioTestCase):

    async def test_bulk_success_ids_add_to_uploaded_set(
        self,
    ) -> None:
        from scrape_exchange.bulk_upload import BulkBatchOutcome
        from tools.yt_video_upload import _upload_one_video_batch

        settings = MagicMock()
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://test'
        settings.bulk_progress_timeout_seconds = 1.0

        uploaded: AsyncMock = AsyncMock()
        with patch(
            'tools.yt_video_upload.upload_bulk_batch',
            new=AsyncMock(return_value=BulkBatchOutcome(
                status='completed',
                job_id='job1',
                success=2,
                failed=0,
                missing=0,
                success_ids={'abcXYZ', 'defXYZ'},
            )),
        ):
            await _upload_one_video_batch(
                b'{}\n{}\n',
                [
                    ('abcXYZ', 'video-dlp-abcXYZ.json.br'),
                    ('defXYZ', 'video-dlp-defXYZ.json.br'),
                ],
                settings,
                MagicMock(),
                AsyncMock(),
                uploaded,
            )

        self.assertEqual(uploaded.add.await_count, 2)
        uploaded.add.assert_any_await('abcXYZ')
        uploaded.add.assert_any_await('defXYZ')


class TestVideoUploadStartup(unittest.IsolatedAsyncioTestCase):

    async def test_run_worker_uses_bulk_for_startup_files(self) -> None:
        from tools.yt_video_upload import _run_worker

        settings = MagicMock()
        settings.video_data_directory = '/tmp/videos'
        settings.redis_dsn = 'redis://test'
        settings.exchange_url = 'http://test'
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'

        ctx = MagicMock()
        ctx.settings = settings
        ctx.client = MagicMock()

        with patch(
            'tools.yt_video_upload.AssetFileManagement',
            return_value=MagicMock(),
        ), patch(
            'tools.yt_video_upload.RedisCreatorMap',
            return_value=MagicMock(),
        ), patch(
            'tools.yt_video_upload.fetch_schema_dict',
            new=AsyncMock(return_value={}),
        ), patch(
            'tools.yt_video_upload.SchemaValidator',
            return_value=MagicMock(),
        ), patch(
            'tools.yt_video_upload.UploadedVideoIds',
            return_value=MagicMock(),
        ), patch(
            'tools.yt_video_upload.resume_pending_bulk_uploads',
            new=AsyncMock(),
        ) as resume_mock, patch(
            'tools.yt_video_upload.upload_videos',
            new=AsyncMock(),
        ) as bulk_mock, patch(
            'tools.yt_video_upload.upload_worker_loop',
            new=AsyncMock(),
        ) as worker_mock:
            await _run_worker(ctx)

        resume_mock.assert_awaited_once()
        bulk_mock.assert_awaited_once()
        worker_mock.assert_awaited_once()
        self.assertFalse(
            worker_mock.await_args.kwargs['enqueue_existing'],
        )


class TestUploadedVideoIds(unittest.IsolatedAsyncioTestCase):

    async def test_prepare_line_moves_already_uploaded_video(
        self,
    ) -> None:
        from tools.yt_video_upload import _prepare_video_line

        settings = MagicMock()
        video_fm = AsyncMock()
        video_fm.mark_uploaded = AsyncMock(
            return_value=Path('/tmp/uploaded/video-dlp-abc123XYZ.json.br'),
        )

        uploaded: AsyncMock = AsyncMock()
        uploaded.contains.return_value = True
        with patch(
            'tools.yt_video_upload.video_needs_uploading',
            new=AsyncMock(return_value=True),
        ), patch(
            'tools.yt_video_upload._collect_video_record',
            new=AsyncMock(),
        ) as collect:
            result = await _prepare_video_line(
                'video-dlp-abc123XYZ.json.br',
                settings,
                video_fm,
                AsyncMock(),
                None,
                _permissive_validator(),
                uploaded,
            )

        self.assertIsNone(result)
        video_fm.mark_uploaded.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )
        collect.assert_not_awaited()

    async def test_process_upload_file_moves_already_uploaded_video(
        self,
    ) -> None:
        from tools.yt_video_upload import _process_upload_file

        settings = MagicMock()
        video_fm = AsyncMock()
        video_fm.mark_uploaded = AsyncMock(
            return_value=Path('/tmp/uploaded/video-min-abc123XYZ.json.br'),
        )

        uploaded: AsyncMock = AsyncMock()
        uploaded.contains.return_value = True
        with patch(
            'tools.yt_video_upload.video_needs_uploading',
            new=AsyncMock(return_value=True),
        ), patch(
            'tools.yt_video_upload._load_video_file',
            new=AsyncMock(),
        ) as load:
            result = await _process_upload_file(
                'video-min-abc123XYZ.json.br',
                settings,
                video_fm,
                MagicMock(),
                AsyncMock(),
                _permissive_validator(),
                None,
                uploaded,
            )

        self.assertFalse(result)
        video_fm.mark_uploaded.assert_awaited_once_with(
            'video-min-abc123XYZ.json.br',
        )
        load.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
