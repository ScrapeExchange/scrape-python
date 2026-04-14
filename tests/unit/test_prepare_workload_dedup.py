'''
Unit tests for the dedup logic inside ``prepare_workload`` in
tools/yt_video_scrape.py.

When both ``video-min-{id}.json.br`` and ``video-dlp-{id}.json.br``
exist for the same video ID the video-min file is stale — it must be
deleted and only the video-dlp entry queued for upload. When only one
variant exists it passes through unchanged.
'''

import importlib.util
import sys
import unittest

from asyncio import Queue
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, call


def _load_yt_video_scrape() -> ModuleType:
    '''
    Load tools/yt_video_scrape.py as a module without importing it
    as a package (``tools/`` has no ``__init__.py``).
    '''
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_video_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_video_scrape', module_path,
    )
    assert spec is not None and spec.loader is not None
    mod: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod: ModuleType = _load_yt_video_scrape()
prepare_workload = _mod.prepare_workload

VIDEO_MIN_PREFIX: str = _mod.VIDEO_MIN_PREFIX
VIDEO_YTDLP_PREFIX: str = _mod.VIDEO_YTDLP_PREFIX
FILE_EXTENSION: str = _mod.FILE_EXTENSION


def _min(vid: str) -> str:
    '''Return the bare filename for a video-min entry.'''
    return f'{VIDEO_MIN_PREFIX}{vid}{FILE_EXTENSION}'


def _dlp(vid: str) -> str:
    '''Return the bare filename for a video-dlp entry.'''
    return f'{VIDEO_YTDLP_PREFIX}{vid}{FILE_EXTENSION}'


def _make_mocks(
    min_files: list[str],
    dlp_files: list[str],
) -> tuple[MagicMock, MagicMock]:
    '''
    Build mock ``settings`` and ``video_fm`` objects for a single
    test scenario.

    ``video_fm.list_base`` is configured to return ``min_files`` when
    called with ``prefix=VIDEO_MIN_PREFIX`` and ``dlp_files`` when
    called with ``prefix=VIDEO_YTDLP_PREFIX``.

    ``video_fm.is_superseded`` always returns ``False`` so that every
    candidate file passes through ``video_needs_uploading`` and lands
    in the queue (no upload-supersession filtering during dedup
    tests).

    ``video_fm.delete`` is an ``AsyncMock`` so awaiting it works.
    '''
    settings: MagicMock = MagicMock()
    settings.video_data_directory = '/tmp/test'

    video_fm: MagicMock = MagicMock()
    video_fm.delete = AsyncMock()
    video_fm.is_superseded.return_value = False

    def _list_base(
        prefix: str = '',
        suffix: str = '',
    ) -> list[str]:
        if prefix == VIDEO_MIN_PREFIX:
            return list(min_files)
        if prefix == VIDEO_YTDLP_PREFIX:
            return list(dlp_files)
        return []

    video_fm.list_base.side_effect = _list_base
    return settings, video_fm


class TestPrepareWorkloadDedup(unittest.IsolatedAsyncioTestCase):
    '''
    Tests covering the video-min / video-dlp deduplication step
    inside ``prepare_workload``.
    '''

    async def test_both_present_min_deleted_dlp_queued(
        self,
    ) -> None:
        '''
        When video-min-ABC and video-dlp-ABC both exist the video-min
        file must be deleted and only video-dlp-ABC ends up in the
        queue.
        '''
        vid: str = 'ABC'
        settings, video_fm = _make_mocks(
            min_files=[_min(vid)],
            dlp_files=[_dlp(vid)],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        # The video-min file was deleted exactly once.
        video_fm.delete.assert_awaited_once_with(
            _min(vid), fail_ok=False,
        )

        # Only the dlp entry is queued.
        items: list[str] = []
        while not queue.empty():
            items.append(await queue.get())
        self.assertEqual(items, [_dlp(vid)])

    async def test_only_min_present_kept_in_queue(self) -> None:
        '''
        When only video-min-ABC exists (no video-dlp counterpart) it
        must NOT be deleted and must appear in the queue.
        '''
        vid: str = 'ABC'
        settings, video_fm = _make_mocks(
            min_files=[_min(vid)],
            dlp_files=[],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        video_fm.delete.assert_not_awaited()

        items: list[str] = []
        while not queue.empty():
            items.append(await queue.get())
        self.assertEqual(items, [_min(vid)])

    async def test_only_dlp_present_queued_normally(self) -> None:
        '''
        When only video-dlp-ABC exists (no video-min counterpart) it
        must appear in the queue with no delete calls.
        '''
        vid: str = 'ABC'
        settings, video_fm = _make_mocks(
            min_files=[],
            dlp_files=[_dlp(vid)],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        video_fm.delete.assert_not_awaited()

        items: list[str] = []
        while not queue.empty():
            items.append(await queue.get())
        self.assertEqual(items, [_dlp(vid)])

    async def test_mixed_overlap_multiple_ids(self) -> None:
        '''
        With three video IDs:
          - ID1: both min and dlp exist  → min deleted, dlp queued
          - ID2: only min exists          → min kept, no delete
          - ID3: only dlp exists          → dlp queued, no delete

        Exactly one delete call (for ID1's min file) and exactly two
        entries in the queue (ID2-min and ID3-dlp) plus ID1-dlp.
        '''
        settings, video_fm = _make_mocks(
            min_files=[_min('ID1'), _min('ID2')],
            dlp_files=[_dlp('ID1'), _dlp('ID3')],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        # Only the overlapping video-min is deleted.
        video_fm.delete.assert_awaited_once_with(
            _min('ID1'), fail_ok=False,
        )

        items: set[str] = set()
        while not queue.empty():
            items.add(await queue.get())

        expected: set[str] = {
            _dlp('ID1'),
            _min('ID2'),
            _dlp('ID3'),
        }
        self.assertEqual(items, expected)

    async def test_empty_file_lists_no_errors(self) -> None:
        '''
        When both lists are empty no delete calls are made and the
        returned queue is empty.
        '''
        settings, video_fm = _make_mocks(
            min_files=[],
            dlp_files=[],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        video_fm.delete.assert_not_awaited()
        self.assertTrue(queue.empty())

    async def test_multiple_overlapping_ids_all_deleted(
        self,
    ) -> None:
        '''
        When every video-min has a matching video-dlp each video-min
        is deleted individually and none appear in the queue.
        '''
        vids: list[str] = ['X1', 'X2', 'X3']
        settings, video_fm = _make_mocks(
            min_files=[_min(v) for v in vids],
            dlp_files=[_dlp(v) for v in vids],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        expected_calls: list[call] = [
            call(_min(v), fail_ok=False) for v in vids
        ]
        video_fm.delete.assert_awaited()
        self.assertEqual(
            video_fm.delete.await_count, len(vids),
        )
        # Each expected call appears in the actual call list.
        actual_calls: list[call] = (
            video_fm.delete.await_args_list
        )
        for expected_call in expected_calls:
            self.assertIn(expected_call, actual_calls)

        # All queued entries are dlp files only.
        items: set[str] = set()
        while not queue.empty():
            items.add(await queue.get())
        self.assertEqual(items, {_dlp(v) for v in vids})


if __name__ == '__main__':
    unittest.main()
