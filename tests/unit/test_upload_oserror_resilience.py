'''A per-file OSError (e.g. ENOSPC from the ext4 single-directory limit
when a video dir holds millions of files) must not crash the whole
upload drain. The failing file is logged and skipped so the rest keep
uploading and the directory drains.
'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class TestVideoPrepareOSErrorSkips(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_enospc_on_mark_uploaded_skips_file(self) -> None:
        from tools.yt_video_upload import _prepare_video_line
        video_fm = MagicMock()
        video_fm.mark_uploaded = AsyncMock(
            side_effect=OSError(28, 'No space left on device'),
        )
        with patch(
            'tools.yt_video_upload.video_needs_uploading',
            new=AsyncMock(return_value=True),
        ), patch(
            'tools.yt_video_upload._parse_entry',
            new=MagicMock(return_value=('vidABC', 'x', False)),
        ):
            result = await _prepare_video_line(
                'video-min-vidABC.json.br',
                MagicMock(),          # settings
                video_fm,
                MagicMock(),          # creator_map_backend
                None,                 # proxy
                MagicMock(),          # validator
                MagicMock(),          # uploaded
                already_uploaded=True,
            )
        # Skipped (None), not raised.
        self.assertIsNone(result)
        video_fm.mark_uploaded.assert_awaited_once()

    async def test_worker_path_enospc_skips_file(self) -> None:
        from tools.yt_video_upload import _process_upload_file
        video_fm = MagicMock()
        video_fm.mark_uploaded = AsyncMock(
            side_effect=OSError(28, 'No space left on device'),
        )
        with patch(
            'tools.yt_video_upload.video_needs_uploading',
            new=AsyncMock(return_value=True),
        ), patch(
            'tools.yt_video_upload._parse_entry',
            new=MagicMock(return_value=('vidABC', 'video-min-', False)),
        ), patch(
            'tools.yt_video_upload._contains_uploaded_video_id',
            new=AsyncMock(return_value=True),
        ):
            result = await _process_upload_file(
                'video-min-vidABC.json.br',
                MagicMock(),          # settings
                video_fm,
                MagicMock(),          # client
                MagicMock(),          # creator_map_backend
                MagicMock(),          # validator
                None,                 # proxy
                MagicMock(),          # uploaded
            )
        self.assertFalse(result)
        video_fm.mark_uploaded.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
