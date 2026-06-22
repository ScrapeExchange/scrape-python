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

import brotli

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


class TestLoadVideoFile(unittest.IsolatedAsyncioTestCase):

    async def test_unrecoverable_brotli_requeued_then_deleted(
        self,
    ) -> None:
        from tools.yt_video_upload import _load_video_file

        video_fm = AsyncMock()
        scrape_queue = AsyncMock()
        events: list[str] = []
        scrape_queue.force_enqueue.side_effect = (
            lambda *args, **kwargs: events.append('enqueue')
        )
        video_fm.delete.side_effect = (
            lambda *args, **kwargs: events.append('delete')
        )

        with patch(
            'tools.yt_video_upload.YouTubeVideo.from_file',
            new=AsyncMock(side_effect=brotli.error('corrupt')),
        ):
            result = await _load_video_file(
                'abc123XYZ', '/tmp', 'video-dlp-',
                'video-dlp-abc123XYZ.json.br', video_fm,
                scrape_queue,
            )

        self.assertIsNone(result)
        scrape_queue.force_enqueue.assert_awaited_once_with(
            'abc123XYZ', source='video_upload_corrupt_file',
        )
        video_fm.delete.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br', fail_ok=False,
        )
        self.assertEqual(events, ['enqueue', 'delete'])

    async def test_requeue_failure_preserves_corrupt_file(self) -> None:
        from tools.yt_video_upload import _load_video_file

        video_fm = AsyncMock()
        scrape_queue = AsyncMock()
        scrape_queue.force_enqueue.side_effect = RuntimeError(
            'redis unavailable',
        )

        with patch(
            'tools.yt_video_upload.YouTubeVideo.from_file',
            new=AsyncMock(side_effect=brotli.error('corrupt')),
        ):
            with self.assertRaisesRegex(
                RuntimeError, 'redis unavailable',
            ):
                await _load_video_file(
                    'abc123XYZ', '/tmp', 'video-dlp-',
                    'video-dlp-abc123XYZ.json.br', video_fm,
                    scrape_queue,
                )

        video_fm.delete.assert_not_awaited()


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
            'tools.yt_video_upload.upload_prepared_bulk_batch',
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
        scrape_queue = bulk_mock.await_args.args[6]
        self.assertIs(
            worker_mock.await_args.kwargs['scrape_queue'],
            scrape_queue,
        )
        self.assertFalse(
            worker_mock.await_args.kwargs['enqueue_existing'],
        )


class TestUploadedVideoIds(unittest.IsolatedAsyncioTestCase):

    async def test_iter_video_upload_filenames_streams_real_directory(
        self,
    ) -> None:
        from tools.yt_video_upload import _iter_video_upload_filenames

        with TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            (base_dir / 'video-dlp-one.json.br').write_bytes(b'{}')
            (base_dir / 'video-min-two.json.br').write_bytes(b'{}')
            (base_dir / 'video-dlp-three.json.br.failed').write_bytes(
                b'{}',
            )
            (base_dir / 'not-a-video.json.br').write_bytes(b'{}')

            video_fm = MagicMock()
            video_fm.base_dir = base_dir
            video_fm.list_base.side_effect = AssertionError(
                'real-directory scan should not materialise list_base',
            )

            filenames = sorted(_iter_video_upload_filenames(video_fm))

        self.assertEqual(filenames, [
            'video-dlp-one.json.br',
            'video-min-two.json.br',
        ])

    async def test_upload_worker_count_ignores_scrape_proxies(
        self,
    ) -> None:
        from tools.yt_video_upload import upload_worker_loop

        settings = MagicMock()
        settings.video_upload_concurrency = 2
        settings.proxies = ['http://p1:8080', 'http://p2:8080']
        settings.video_upload_watch = False

        video_fm = MagicMock()
        video_fm.list_base.return_value = []
        client = AsyncMock()
        worker = AsyncMock()

        with patch(
            'tools.yt_video_upload._upload_worker',
            new=worker,
        ):
            await upload_worker_loop(
                settings, video_fm, client, AsyncMock(),
                _permissive_validator(), AsyncMock(),
                enqueue_existing=False,
            )

        self.assertEqual(worker.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in worker.call_args_list],
            [None, None],
        )

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

    async def test_prepare_line_uses_prefetched_uploaded_status(
        self,
    ) -> None:
        from tools.yt_video_upload import _prepare_video_line

        settings = MagicMock()
        video_fm = AsyncMock()
        uploaded: AsyncMock = AsyncMock()
        uploaded.contains.side_effect = AssertionError(
            'single Redis lookup should not run',
        )

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
                already_uploaded=True,
            )

        self.assertIsNone(result)
        video_fm.mark_uploaded.assert_awaited_once_with(
            'video-dlp-abc123XYZ.json.br',
        )
        collect.assert_not_awaited()

    async def test_upload_videos_batches_uploaded_id_lookups(
        self,
    ) -> None:
        from contextlib import asynccontextmanager
        from scrape_exchange.bulk_upload import BulkBatchOutcome
        from tools.yt_video_upload import upload_videos

        async def fake_collect(
            filename: str,
            settings: object,
            video_fm: object,
            creator_map_backend: object,
            proxy: object,
            validator: object,
            scrape_queue: object = None,
        ) -> tuple[str, dict]:
            del (
                settings, video_fm, creator_map_backend,
                proxy, validator, scrape_queue,
            )
            video_id: str = filename.removeprefix(
                'video-dlp-',
            ).removesuffix('.json.br')
            return video_id, {'video_id': video_id}

        async def fake_post(
            batch_buf: bytes,
            batch_records: list[tuple[str, str]],
            settings: object,
            client: object,
            video_fm: object,
        ) -> tuple[str, str, BulkBatchOutcome | None]:
            del batch_buf, batch_records, settings, client, video_fm
            return 'job-1', 'batch-1', None

        async def fake_finalize_helper(*args: object) -> None:
            del args

        @asynccontextmanager
        async def _noop_slot(*args: object, **kwargs: object):
            del args, kwargs
            yield None

        settings = MagicMock()
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://test'
        settings.bulk_progress_timeout_seconds = 1.0
        settings.bulk_batch_size = 1000
        settings.bulk_max_batch_bytes = 10 * 1024 * 1024
        settings.video_upload_concurrency = 3
        settings.max_active_bulk_jobs = 1
        settings.proxies = []
        settings.video_data_directory = '/tmp/videos'

        video_fm = MagicMock()
        video_fm.list_base = MagicMock(side_effect=[
            [
                'video-dlp-one.json.br',
                'video-dlp-two.json.br',
                'video-dlp-three.json.br',
            ],
            [],
        ])
        uploaded = AsyncMock()
        uploaded.contains.side_effect = AssertionError(
            'single Redis lookup should not run',
        )
        uploaded.contains_many.return_value = {
            'one': False,
            'two': False,
            'three': False,
        }

        with patch(
            'tools.yt_video_upload.video_needs_uploading',
            new=AsyncMock(return_value=True),
        ), patch(
            'tools.yt_video_upload._collect_video_record',
            new=fake_collect,
        ), patch(
            'tools.yt_video_upload._post_one_video_batch',
            new=fake_post,
        ), patch(
            'tools.yt_video_upload._finalize_one_video_batch',
            new=fake_finalize_helper,
        ), patch(
            'tools.yt_video_upload.reserve_bulk_upload_slot',
            new=_noop_slot,
        ):
            await upload_videos(
                settings,
                MagicMock(),
                video_fm,
                AsyncMock(),
                _permissive_validator(),
                uploaded,
            )

        uploaded.contains.assert_not_awaited()
        uploaded.contains_many.assert_awaited_once_with(
            ['one', 'two', 'three'],
        )

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


class TestBulkBatchBytesReleasedBetweenPostAndFinalize(
    unittest.IsolatedAsyncioTestCase,
):
    '''The video uploader's _spawn_batch._gated must drop its
    reference to the batch bytes between the POST phase and the
    long progress-wait phase. With the default 30-minute
    bulk_progress_timeout_seconds, NOT dropping the reference
    keeps hundreds of MB of payload alive per in-flight batch.
    '''

    async def test_buf_local_in_gated_is_empty_when_finalize_runs(
        self,
    ) -> None:
        import inspect
        from contextlib import asynccontextmanager
        from scrape_exchange.bulk_upload import BulkBatchOutcome
        from tools.yt_video_upload import upload_videos

        # 1 MB payload — large enough that an accidentally-held
        # reference would be obvious in a memory profile.
        payload: bytes = b'x' * (1024 * 1024)
        buf_at_finalize: dict[str, object] = {}

        async def fake_post(
            batch_buf: bytes,
            batch_records: list[tuple[str, str]],
            settings: object,
            client: object,
            video_fm: object,
        ) -> tuple[str, str, BulkBatchOutcome | None]:
            del (
                batch_buf, batch_records, settings,
                client, video_fm,
            )
            return 'job-1', 'batch-1', None

        async def fake_finalize_helper(
            job_id: str,
            batch_id: str,
            err: BulkBatchOutcome | None,
            batch_records: list[tuple[str, str]],
            settings: object,
            client: object,
            video_fm: object,
            uploaded: object,
        ) -> None:
            del (
                job_id, batch_id, err, settings,
                client, video_fm, uploaded,
            )
            # Walk the frame stack to find _gated's frame
            # and snapshot its _buf local at the moment the
            # progress-wait phase begins.
            frame = inspect.currentframe()
            gated_frame = None
            while frame is not None:
                if frame.f_code.co_name == '_gated':
                    gated_frame = frame
                    break
                frame = frame.f_back
            assert gated_frame is not None, (
                '_gated frame not in call stack'
            )
            buf_at_finalize['value'] = (
                gated_frame.f_locals.get('_buf')
            )
            buf_at_finalize['records'] = batch_records

        @asynccontextmanager
        async def _noop_slot(*args: object, **kwargs: object):
            del args, kwargs
            yield None

        async def fake_prepare(
            filename: str,
            settings: object,
            video_fm: object,
            creator_map_backend: object,
            proxy: object,
            validator: object,
            uploaded: object,
            **kwargs: object,
        ) -> tuple[str, str, bytes]:
            del (
                settings, video_fm, creator_map_backend,
                proxy, validator, uploaded, kwargs,
            )
            video_id: str = filename.split('-')[-1].rstrip(
                '.json.br'
            )
            return video_id, filename, payload

        settings = MagicMock()
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://test'
        settings.bulk_progress_timeout_seconds = 1.0
        settings.bulk_batch_size = 1000
        settings.bulk_max_batch_bytes = 10 * 1024 * 1024
        settings.video_upload_concurrency = 1
        settings.max_active_bulk_jobs = 1
        settings.proxies = []
        settings.video_data_directory = '/tmp/videos'

        video_fm = MagicMock()
        video_fm.list_base = MagicMock(side_effect=[
            ['video-dlp-abcXYZ.json.br'], [],
        ])
        uploaded = AsyncMock()
        uploaded.contains_many.return_value = {'abcXYZ': False}

        with patch(
            'tools.yt_video_upload._post_one_video_batch',
            new=fake_post,
        ), patch(
            'tools.yt_video_upload._finalize_one_video_batch',
            new=fake_finalize_helper,
        ), patch(
            'tools.yt_video_upload._prepare_video_line',
            new=fake_prepare,
        ), patch(
            'tools.yt_video_upload.reserve_bulk_upload_slot',
            new=_noop_slot,
        ):
            await upload_videos(
                settings,
                MagicMock(),
                video_fm,
                AsyncMock(),
                _permissive_validator(),
                uploaded,
            )

        self.assertIn('value', buf_at_finalize)
        # The whole point: by the time the finalize helper is
        # invoked (which then does the long progress wait),
        # _gated's _buf local must already be empty bytes —
        # NOT the 1 MB payload.
        self.assertEqual(buf_at_finalize['value'], b'')
        # batch_records is what the finalize phase needs and
        # is small (filenames + content IDs).
        self.assertEqual(
            buf_at_finalize['records'],
            [('abcXYZ', 'video-dlp-abcXYZ.json.br')],
        )

    async def test_batch_creation_is_bounded_by_active_jobs(
        self,
    ) -> None:
        import asyncio
        from contextlib import asynccontextmanager
        from scrape_exchange.bulk_upload import BulkBatchOutcome
        from tools.yt_video_upload import upload_videos

        finalize_started = asyncio.Event()
        release_finalize = asyncio.Event()
        prepared: list[str] = []

        async def fake_post(
            batch_buf: bytes,
            batch_records: list[tuple[str, str]],
            settings: object,
            client: object,
            video_fm: object,
        ) -> tuple[str, str, BulkBatchOutcome | None]:
            del batch_buf, batch_records, settings, client, video_fm
            return 'job-1', 'batch-1', None

        async def fake_finalize_helper(
            job_id: str,
            batch_id: str,
            err: BulkBatchOutcome | None,
            batch_records: list[tuple[str, str]],
            settings: object,
            client: object,
            video_fm: object,
            uploaded: object,
        ) -> None:
            del (
                job_id, batch_id, err, batch_records,
                settings, client, video_fm, uploaded,
            )
            finalize_started.set()
            await release_finalize.wait()

        @asynccontextmanager
        async def _noop_slot(*args: object, **kwargs: object):
            del args, kwargs
            yield None

        async def fake_prepare(
            filename: str,
            settings: object,
            video_fm: object,
            creator_map_backend: object,
            proxy: object,
            validator: object,
            uploaded: object,
            **kwargs: object,
        ) -> tuple[str, str, bytes]:
            del (
                settings, video_fm, creator_map_backend,
                proxy, validator, uploaded, kwargs,
            )
            prepared.append(filename)
            video_id: str = filename.removeprefix(
                'video-dlp-',
            ).removesuffix('.json.br')
            return video_id, filename, b'{}\n'

        settings = MagicMock()
        settings.schema_owner = 'boinko'
        settings.schema_version = '0.0.2'
        settings.exchange_url = 'http://test'
        settings.bulk_progress_timeout_seconds = 1.0
        settings.bulk_batch_size = 1
        settings.bulk_max_batch_bytes = 10 * 1024 * 1024
        settings.video_upload_concurrency = 1
        settings.max_active_bulk_jobs = 1
        settings.proxies = []
        settings.video_data_directory = '/tmp/videos'

        video_fm = MagicMock()
        video_fm.list_base = MagicMock(side_effect=[
            [
                'video-dlp-one.json.br',
                'video-dlp-two.json.br',
                'video-dlp-three.json.br',
            ],
            [],
        ])
        uploaded = AsyncMock()
        uploaded.contains_many.return_value = {
            'one': False,
            'two': False,
            'three': False,
        }

        with patch(
            'tools.yt_video_upload._post_one_video_batch',
            new=fake_post,
        ), patch(
            'tools.yt_video_upload._finalize_one_video_batch',
            new=fake_finalize_helper,
        ), patch(
            'tools.yt_video_upload._prepare_video_line',
            new=fake_prepare,
        ), patch(
            'tools.yt_video_upload.reserve_bulk_upload_slot',
            new=_noop_slot,
        ):
            task = asyncio.create_task(upload_videos(
                settings,
                MagicMock(),
                video_fm,
                AsyncMock(),
                _permissive_validator(),
                uploaded,
            ))
            await asyncio.wait_for(finalize_started.wait(), timeout=1.0)
            # The second file is needed to discover that the first
            # batch is full. The third must not be prepared until
            # one active batch has finished.
            self.assertEqual(prepared, [
                'video-dlp-one.json.br',
                'video-dlp-two.json.br',
            ])
            release_finalize.set()
            await task

        self.assertEqual(prepared, [
            'video-dlp-one.json.br',
            'video-dlp-two.json.br',
            'video-dlp-three.json.br',
        ])


if __name__ == '__main__':
    unittest.main()
