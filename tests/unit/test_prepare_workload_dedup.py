'''
Unit tests for the dedup logic inside ``prepare_workload`` in
tools/yt_video_scrape.py.

When both ``video-min-{id}.json.br`` and ``video-dlp-{id}.json.br``
exist for the same video ID the video-min file is stale — it must be
deleted and only the video-dlp entry queued for upload. When only one
variant exists it passes through unchanged.

The dedup applies symmetrically across ``base_dir`` and
``uploaded_dir``: a ``video-dlp-{id}`` in either directory makes any
``video-min-{id}`` in either directory stale.
'''

import unittest

from asyncio import Queue
from unittest.mock import AsyncMock, MagicMock, call

from tools.yt_video_scrape import (
    FILE_EXTENSION,
    VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_PREFIX,
    WorkItem,
    prepare_workload,
)


def _min(vid: str) -> str:
    '''Return the bare filename for a video-min entry.'''
    return f'{VIDEO_MIN_PREFIX}{vid}{FILE_EXTENSION}'


def _dlp(vid: str) -> str:
    '''Return the bare filename for a video-dlp entry.'''
    return f'{VIDEO_YTDLP_PREFIX}{vid}{FILE_EXTENSION}'


def _make_mocks(
    min_files: list[str],
    dlp_files: list[str],
    *,
    min_uploaded: list[str] | None = None,
    dlp_uploaded: list[str] | None = None,
    upload_only: bool = False,
) -> tuple[MagicMock, MagicMock]:
    '''
    Build mock ``settings`` and ``video_fm`` objects for a single
    test scenario.

    ``video_fm.list_base`` and ``video_fm.list_uploaded`` are
    configured to return the appropriate files for each prefix.
    ``video_fm.is_superseded`` always returns ``False`` so every
    base-directory candidate passes through
    ``video_needs_uploading`` and lands in the queue (no
    upload-supersession filtering during dedup tests).
    ``video_fm.delete`` and ``video_fm.delete_uploaded`` are
    ``AsyncMock``\\ s so awaiting them works.
    '''
    min_uploaded = min_uploaded or []
    dlp_uploaded = dlp_uploaded or []

    settings: MagicMock = MagicMock()
    settings.video_data_directory = '/tmp/test'
    settings.video_upload_only = upload_only

    video_fm: MagicMock = MagicMock()
    video_fm.delete = AsyncMock()
    video_fm.delete_uploaded = AsyncMock()
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

    def _list_uploaded(
        prefix: str = '',
        suffix: str = '',
    ) -> list[str]:
        if prefix == VIDEO_MIN_PREFIX:
            return list(min_uploaded)
        if prefix == VIDEO_YTDLP_PREFIX:
            return list(dlp_uploaded)
        return []

    video_fm.list_base.side_effect = _list_base
    video_fm.list_uploaded.side_effect = _list_uploaded
    return settings, video_fm


async def _drain(queue: Queue) -> list[WorkItem]:
    '''Empty *queue* and return the accumulated items.'''
    items: list[WorkItem] = []
    while not queue.empty():
        items.append(await queue.get())
    return items


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
        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(
            items, [WorkItem(_dlp(vid), False)],
        )

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

        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(
            items, [WorkItem(_min(vid), False)],
        )

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

        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(
            items, [WorkItem(_dlp(vid), False)],
        )

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

        items: set[WorkItem] = set(await _drain(queue))
        expected: set[WorkItem] = {
            WorkItem(_dlp('ID1'), False),
            WorkItem(_min('ID2'), False),
            WorkItem(_dlp('ID3'), False),
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
        items: set[WorkItem] = set(await _drain(queue))
        self.assertEqual(
            items, {WorkItem(_dlp(v), False) for v in vids},
        )


class TestPrepareWorkloadUploadedDir(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    Tests covering the new behaviour where prepare_workload also
    looks for ``video-min-*`` files in ``uploaded_dir`` (only
    relevant in non-upload-only mode, where they will be picked up
    for yt-dlp scraping).
    '''

    async def test_min_in_uploaded_queued_with_from_uploaded_flag(
        self,
    ) -> None:
        '''
        A ``video-min-XYZ`` that lives in ``uploaded_dir`` and has
        no ``video-dlp-XYZ`` counterpart anywhere is queued with
        ``from_uploaded=True`` so the worker reads it from the
        right directory.
        '''
        vid: str = 'XYZ'
        settings, video_fm = _make_mocks(
            min_files=[],
            dlp_files=[],
            min_uploaded=[_min(vid)],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(items, [WorkItem(_min(vid), True)])
        video_fm.delete.assert_not_awaited()
        video_fm.delete_uploaded.assert_not_awaited()

    async def test_uploaded_min_dropped_when_dlp_in_base_exists(
        self,
    ) -> None:
        '''
        A ``video-min`` in ``uploaded_dir`` with a ``video-dlp``
        in ``base_dir`` is stale — the upgrade has already happened.
        It must be removed via ``delete_uploaded`` (not ``delete``)
        and must NOT appear in the queue.
        '''
        vid: str = 'XYZ'
        settings, video_fm = _make_mocks(
            min_files=[],
            dlp_files=[_dlp(vid)],
            min_uploaded=[_min(vid)],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        video_fm.delete_uploaded.assert_awaited_once_with(
            _min(vid), fail_ok=False,
        )
        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(items, [WorkItem(_dlp(vid), False)])

    async def test_uploaded_min_dropped_when_dlp_in_uploaded_exists(
        self,
    ) -> None:
        '''
        A ``video-min`` in ``uploaded_dir`` with a ``video-dlp``
        also in ``uploaded_dir`` is stale and must be removed via
        ``delete_uploaded``.
        '''
        vid: str = 'XYZ'
        settings, video_fm = _make_mocks(
            min_files=[],
            dlp_files=[],
            min_uploaded=[_min(vid)],
            dlp_uploaded=[_dlp(vid)],
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        video_fm.delete_uploaded.assert_awaited_once_with(
            _min(vid), fail_ok=False,
        )
        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(items, [])

    async def test_upload_only_skips_uploaded_min(self) -> None:
        '''
        In upload-only mode ``prepare_workload`` does not seed
        ``uploaded_dir`` files: they are already on the API server
        and the watch uploader only handles new arrivals in
        ``base_dir``.  The watcher uploader filter still triggers
        on video-min events, so a video-min that lands in base_dir
        is the only path that needs queueing.
        '''
        vid: str = 'XYZ'
        settings, video_fm = _make_mocks(
            min_files=[_min(vid)],
            dlp_files=[],
            min_uploaded=[_min('LEFT-OVER')],
            upload_only=True,
        )

        queue: Queue = await prepare_workload(settings, video_fm)

        items: list[WorkItem] = await _drain(queue)
        self.assertEqual(items, [WorkItem(_min(vid), False)])
        video_fm.delete_uploaded.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
