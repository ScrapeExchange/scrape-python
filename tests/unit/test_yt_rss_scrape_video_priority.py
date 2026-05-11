'''Tests for the RSS scraper's video write target selection.

When ``settings.video_priority_directory`` is set, newly-
discovered videos are written there so the
``--video-upload-only`` container processes them ahead of
the bulk-archive backlog. When unset, files go to
``video_data_directory`` as before.
'''

import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


def _load_yt_rss_scrape() -> ModuleType:
    cached: ModuleType | None = sys.modules.get(
        'yt_rss_scrape',
    )
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape',
        '/home/steven/src/scrape-python/tools/'
        'yt_rss_scrape.py',
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


class TestEnrichAndStoreVideoTarget(
    unittest.IsolatedAsyncioTestCase,
):
    '''``_enrich_and_store_video`` writes the video file to
    the priority directory when configured, otherwise to
    the legacy ``video_data_directory``.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.data_dir: str = os.path.join(self.tmp, 'data')
        self.priority_dir: str = (
            os.path.join(self.tmp, 'priority')
        )
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.priority_dir, exist_ok=True)
        self.module: ModuleType = _load_yt_rss_scrape()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _video(self, video_id: str) -> mock.AsyncMock:
        v: mock.AsyncMock = mock.AsyncMock()
        v.video_id = video_id

        async def _to_file(
            target_dir: str,
            filename_prefix: str = '',
            overwrite: bool = False,
        ) -> str:
            filename: str = (
                f'{filename_prefix}{video_id}.json.br'
            )
            Path(target_dir, filename).write_bytes(b'x')
            return filename

        v.to_file = mock.AsyncMock(side_effect=_to_file)
        v.from_innertube = mock.AsyncMock()
        return v

    def _settings(
        self, priority: str | None,
    ) -> mock.MagicMock:
        s: mock.MagicMock = mock.MagicMock()
        s.video_data_directory = self.data_dir
        s.video_priority_directory = priority
        return s

    async def test_writes_to_priority_when_set(
        self,
    ) -> None:
        result: str | None = (
            await self.module._enrich_and_store_video(
                self._video('vid_a'),
                innertube=mock.MagicMock(),
                proxy=None,
                channel_handle='hh',
                settings=self._settings(self.priority_dir),
            )
        )
        self.assertEqual(result, 'video-min-vid_a.json.br')
        self.assertTrue(
            Path(
                self.priority_dir,
                'video-min-vid_a.json.br',
            ).exists(),
        )
        self.assertFalse(
            Path(
                self.data_dir,
                'video-min-vid_a.json.br',
            ).exists(),
        )

    async def test_writes_to_data_dir_when_priority_unset(
        self,
    ) -> None:
        result: str | None = (
            await self.module._enrich_and_store_video(
                self._video('vid_b'),
                innertube=mock.MagicMock(),
                proxy=None,
                channel_handle='hh',
                settings=self._settings(None),
            )
        )
        self.assertEqual(result, 'video-min-vid_b.json.br')
        self.assertTrue(
            Path(
                self.data_dir,
                'video-min-vid_b.json.br',
            ).exists(),
        )
        self.assertFalse(
            Path(
                self.priority_dir,
                'video-min-vid_b.json.br',
            ).exists(),
        )

    async def test_writes_to_data_dir_when_priority_empty(
        self,
    ) -> None:
        '''Empty-string env var resolves to "" and should
        fall back to data_dir just like ``None``.'''
        result: str | None = (
            await self.module._enrich_and_store_video(
                self._video('vid_c'),
                innertube=mock.MagicMock(),
                proxy=None,
                channel_handle='hh',
                settings=self._settings(''),
            )
        )
        self.assertEqual(result, 'video-min-vid_c.json.br')
        self.assertTrue(
            Path(
                self.data_dir,
                'video-min-vid_c.json.br',
            ).exists(),
        )


if __name__ == '__main__':
    unittest.main()
