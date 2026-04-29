'''
Unit tests for the bulk-upload glue in the video scraper.

The reconciliation/WS helpers themselves are exercised in
``test_yt_channel_bulk_upload.py``; here we cover the
video-side bits — record collection (filename parsing, handle
resolution, video_id presence) and the orchestration that
batches files and dispatches to the shared bulk pipeline.
'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch


def _permissive_validator() -> MagicMock:
    '''A SchemaValidator stand-in that always passes, so existing
    test cases that don't care about validation can ignore it.'''
    validator = MagicMock()
    validator.validate = MagicMock(return_value=None)
    return validator


class TestCollectVideoRecord(unittest.IsolatedAsyncioTestCase):

    async def test_returns_record_on_happy_path(self) -> None:
        '''A well-formed video-dlp file yields
        ``(video_id, record_dict)``.'''
        from tools.yt_video_scrape import _collect_video_record

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
            'tools.yt_video_scrape._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.resolve_video_upload_handle',
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

    async def test_unrecognised_filename_skipped(self) -> None:
        '''Filenames not matching video-min-/video-dlp- prefixes
        are skipped without touching disk.'''
        from tools.yt_video_scrape import _collect_video_record

        with patch(
            'tools.yt_video_scrape._load_video_file',
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
        from tools.yt_video_scrape import _collect_video_record

        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        with patch(
            'tools.yt_video_scrape._load_video_file',
            new=AsyncMock(return_value=None),
        ):
            result = await _collect_video_record(
                'video-dlp-abc.json.br',
                settings, AsyncMock(), AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)

    async def test_unresolved_handle_skipped(self) -> None:
        '''Handle resolution failure leaves the file for retry —
        the helper returns ``None`` instead of returning a record
        with no ``platform_creator_id``.'''
        from tools.yt_video_scrape import _collect_video_record

        video = MagicMock()
        video.video_id = 'abc123XYZ'
        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        with patch(
            'tools.yt_video_scrape._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.resolve_video_upload_handle',
            new=AsyncMock(return_value=None),
        ):
            result = await _collect_video_record(
                'video-dlp-abc123XYZ.json.br',
                settings, AsyncMock(), AsyncMock(), None,
                _permissive_validator(),
            )
        self.assertIsNone(result)

    async def test_missing_video_id_skipped(self) -> None:
        '''A loaded video with no video_id can't be tracked
        through bulk results; it must be skipped.'''
        from tools.yt_video_scrape import _collect_video_record

        video = MagicMock()
        video.video_id = ''
        settings = MagicMock()
        settings.video_data_directory = '/tmp'

        with patch(
            'tools.yt_video_scrape._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.resolve_video_upload_handle',
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
        from tools.yt_video_scrape import _collect_video_record

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
            'tools.yt_video_scrape._load_video_file',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.resolve_video_upload_handle',
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
        from tools.yt_video_scrape import enqueue_upload_video

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


if __name__ == '__main__':
    unittest.main()
