'''
Unit tests for the workload preparation and producer logic in
tools/yt_video_scrape.py.

``prepare_workload`` enumerates candidate files and returns
``(priority_items, shuffled_items)`` without any filtering.
``_produce_workload`` streams those items onto a bounded queue,
skipping stale video-min files and already-superseded base
entries as it goes.

The video-min / video-dlp deduplication formerly done upfront by
``_dedup_video_min`` (now deleted) is now performed lazily inside
``_produce_workload`` via ``_video_min_superseded_by_dlp``.
'''

import tempfile
import unittest

from asyncio import Queue
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from tools.yt_video_scrape import (
    FILE_EXTENSION,
    VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_PREFIX,
    WorkItem,
    _FROM_UPLOADED_SENTINEL,
    _decode_shuffled_entry,
    _encode_shuffled_entry,
    _produce_workload,
    _promote_bare_id_files,
    _video_min_superseded_by_dlp,
    prepare_workload,
)


def _enc(filename: str, from_uploaded: bool = False) -> str:
    '''Encode a candidate the way ``prepare_workload`` does.'''
    return (
        _FROM_UPLOADED_SENTINEL + filename
        if from_uploaded else filename
    )


def _min(vid: str) -> str:
    '''Return the bare filename for a video-min entry.'''
    return f'{VIDEO_MIN_PREFIX}{vid}{FILE_EXTENSION}'


def _dlp(vid: str) -> str:
    '''Return the bare filename for a video-dlp entry.'''
    return f'{VIDEO_YTDLP_PREFIX}{vid}{FILE_EXTENSION}'


def _make_settings(
    *,
    upload_only: bool = False,
    priority_dir: str | None = None,
) -> MagicMock:
    '''Build a minimal mock VideoSettings object.'''
    settings: MagicMock = MagicMock()
    settings.video_upload_only = upload_only
    settings.video_priority_directory = priority_dir
    return settings


def _make_listing_fm(
    min_files: list[str],
    dlp_files: list[str],
    *,
    min_uploaded: list[str] | None = None,
    dlp_uploaded: list[str] | None = None,
    base_dir: Path,
    uploaded_dir: Path,
) -> MagicMock:
    '''
    Build a mock ``video_fm`` whose ``list_base`` /
    ``list_uploaded`` return the provided filename lists.
    ``base_dir`` and ``uploaded_dir`` are real :class:`Path`
    objects so :func:`_promote_bare_id_files` (called from
    ``prepare_workload``) can ``os.scandir`` them.
    ``is_superseded`` defaults to ``False`` so base-dir items
    pass through ``video_needs_uploading``.
    ``delete`` and ``delete_uploaded`` are ``AsyncMock`` s.
    '''
    min_uploaded = min_uploaded or []
    dlp_uploaded = dlp_uploaded or []

    video_fm: MagicMock = MagicMock()
    video_fm.delete = AsyncMock()
    video_fm.delete_uploaded = AsyncMock()
    video_fm.is_superseded.return_value = False
    video_fm.base_dir = base_dir
    video_fm.uploaded_dir = uploaded_dir

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
    return video_fm


def _make_producer_fm(
    *,
    base_dlp_present: set[str] | None = None,
    uploaded_dlp_present: set[str] | None = None,
    is_superseded_returns: bool = False,
) -> MagicMock:
    '''
    Build a mock ``video_fm`` suitable for producer / helper tests.

    ``base_dir / name`` and ``uploaded_dir / name`` support
    ``.exists()`` and return ``True`` only for names listed in
    *base_dlp_present* / *uploaded_dlp_present* respectively.
    ``is_superseded`` returns the fixed value provided.
    ``delete`` and ``delete_uploaded`` are ``AsyncMock`` s.
    '''
    base_dlp_present = base_dlp_present or set()
    uploaded_dlp_present = uploaded_dlp_present or set()

    video_fm: MagicMock = MagicMock()
    video_fm.delete = AsyncMock()
    video_fm.delete_uploaded = AsyncMock()
    video_fm.is_superseded.return_value = is_superseded_returns

    def _make_path(present: set[str]) -> MagicMock:
        dir_mock: MagicMock = MagicMock()

        def _truediv(name: str) -> MagicMock:
            entry: MagicMock = MagicMock()
            entry.exists.return_value = name in present
            return entry

        dir_mock.__truediv__.side_effect = _truediv
        return dir_mock

    video_fm.base_dir = _make_path(base_dlp_present)
    video_fm.uploaded_dir = _make_path(uploaded_dlp_present)
    return video_fm


def _drain(queue: Queue) -> list[WorkItem]:
    '''Empty *queue* and return the accumulated items.'''
    items: list[WorkItem] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


# ---------------------------------------------------------------------------
# Class 1: prepare_workload — pure enumeration + shuffle
# ---------------------------------------------------------------------------

class TestPrepareWorkload(unittest.IsolatedAsyncioTestCase):
    '''
    Tests for the new ``prepare_workload`` signature.  It no longer
    dedups or filters; it only enumerates and shuffles, returning
    ``(priority_items, shuffled_items)``.
    '''

    async def asyncSetUp(self) -> None:
        # ``prepare_workload`` calls ``_promote_bare_id_files``
        # which ``os.scandir``s ``video_fm.base_dir``; provide
        # real (empty) tempdirs so that walk is a no-op.
        self._tmp_base: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self._tmp_uploaded: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.base_dir: Path = Path(self._tmp_base.name)
        self.uploaded_dir: Path = Path(self._tmp_uploaded.name)

    async def asyncTearDown(self) -> None:
        self._tmp_base.cleanup()
        self._tmp_uploaded.cleanup()

    def _fm(
        self,
        min_files: list[str],
        dlp_files: list[str],
        *,
        min_uploaded: list[str] | None = None,
        dlp_uploaded: list[str] | None = None,
    ) -> MagicMock:
        '''Wrapper around :func:`_make_listing_fm` that supplies
        the per-test tempdirs.'''
        return _make_listing_fm(
            min_files=min_files,
            dlp_files=dlp_files,
            min_uploaded=min_uploaded,
            dlp_uploaded=dlp_uploaded,
            base_dir=self.base_dir,
            uploaded_dir=self.uploaded_dir,
        )

    async def test_returns_two_lists(self) -> None:
        '''Empty dirs → returns ``([], [])``.'''
        settings: MagicMock = _make_settings()
        video_fm: MagicMock = self._fm(
            min_files=[], dlp_files=[],
        )
        priority: list[WorkItem]
        shuffled: list[str]
        priority, shuffled = await prepare_workload(
            settings, video_fm,
        )
        self.assertEqual(priority, [])
        self.assertEqual(shuffled, [])

    async def test_non_upload_only_includes_all_three_sources(
        self,
    ) -> None:
        '''
        In normal mode all three sources appear in shuffled_items
        encoded with the correct ``from_uploaded`` flags.
        '''
        settings: MagicMock = _make_settings(upload_only=False)
        video_fm: MagicMock = self._fm(
            min_files=[_min('A')],
            dlp_files=[_dlp('B')],
            min_uploaded=[_min('C')],
        )
        _priority: list[WorkItem]
        shuffled: list[str]
        _priority, shuffled = await prepare_workload(
            settings, video_fm,
        )
        result: set[str] = set(shuffled)
        expected: set[str] = {
            _enc(_min('A'), False),
            _enc(_dlp('B'), False),
            _enc(_min('C'), True),
        }
        self.assertEqual(result, expected)

    async def test_upload_only_excludes_uploaded_min(
        self,
    ) -> None:
        '''
        In upload-only mode only base-dir items appear, all with
        ``from_uploaded=False`` (no sentinel prefix).
        '''
        settings: MagicMock = _make_settings(upload_only=True)
        video_fm: MagicMock = self._fm(
            min_files=[_min('A')],
            dlp_files=[_dlp('B')],
            min_uploaded=[_min('LEFT-OVER')],
        )
        _priority: list[WorkItem]
        shuffled: list[str]
        _priority, shuffled = await prepare_workload(
            settings, video_fm,
        )
        result: set[str] = set(shuffled)
        expected: set[str] = {
            _enc(_min('A'), False),
            _enc(_dlp('B'), False),
        }
        self.assertEqual(result, expected)
        for entry in shuffled:
            self.assertFalse(
                entry.startswith(_FROM_UPLOADED_SENTINEL),
            )

    async def test_priority_items_returned_separately(
        self,
    ) -> None:
        '''
        Priority items appear in the first list only, not in
        shuffled_items.
        '''
        settings: MagicMock = _make_settings()
        video_fm: MagicMock = self._fm(
            min_files=[], dlp_files=[],
        )
        priority_item: WorkItem = WorkItem(
            'priority.json.br', False, True,
        )
        with patch(
            'tools.yt_video_scrape._list_priority_items',
            return_value=[priority_item],
        ):
            priority: list[WorkItem]
            shuffled: list[str]
            priority, shuffled = await prepare_workload(
                settings, video_fm,
            )
        self.assertEqual(priority, [priority_item])
        self.assertNotIn(priority_item.filename, shuffled)
        self.assertNotIn(
            _enc(priority_item.filename, True), shuffled,
        )

    async def test_shuffle_is_called(self) -> None:
        '''``random.shuffle`` is called once with the items list.'''
        settings: MagicMock = _make_settings()
        video_fm: MagicMock = self._fm(
            min_files=[_min('A')],
            dlp_files=[],
        )
        with patch(
            'tools.yt_video_scrape.shuffle',
        ) as mock_shuffle:
            await prepare_workload(settings, video_fm)
        mock_shuffle.assert_called_once()


# ---------------------------------------------------------------------------
# Class 2: _produce_workload — dedup + supersede filtering
# ---------------------------------------------------------------------------

class TestProduceWorkload(unittest.IsolatedAsyncioTestCase):
    '''
    Tests for ``_produce_workload``.  This covers the dedup and
    supersede filtering that used to live in ``prepare_workload`` /
    ``_dedup_video_min``.
    '''

    async def test_priority_items_enqueued_first_unfiltered(
        self,
    ) -> None:
        '''
        Priority items are placed at the head of the queue in
        insertion order without any filtering or deletion.
        '''
        p1: WorkItem = WorkItem(_min('P1'), False, True)
        p2: WorkItem = WorkItem(_min('P2'), False, True)
        priority_items: list[WorkItem] = [p1, p2]
        shuffled_items: list[str] = []
        video_fm: MagicMock = _make_producer_fm()
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload(
            priority_items, shuffled_items, video_fm, queue,
        )

        items: list[WorkItem] = _drain(queue)
        self.assertEqual(items, [p1, p2])
        video_fm.delete.assert_not_awaited()
        video_fm.delete_uploaded.assert_not_awaited()

    async def test_only_min_present_kept(self) -> None:
        '''
        A video-min with no dlp counterpart anywhere is kept.
        '''
        shuffled_items: list[str] = [_enc(_min('ABC'))]
        video_fm: MagicMock = _make_producer_fm()
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        items: list[WorkItem] = _drain(queue)
        self.assertEqual(items, [WorkItem(_min('ABC'), False)])
        video_fm.delete.assert_not_awaited()

    async def test_only_dlp_present_kept(self) -> None:
        '''A video-dlp with no min counterpart is kept.'''
        shuffled_items: list[str] = [_enc(_dlp('ABC'))]
        video_fm: MagicMock = _make_producer_fm()
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        items: list[WorkItem] = _drain(queue)
        self.assertEqual(items, [WorkItem(_dlp('ABC'), False)])
        video_fm.delete.assert_not_awaited()

    async def test_video_min_superseded_by_base_dlp_skipped(
        self,
    ) -> None:
        '''
        A base video-min with a dlp counterpart in base_dir is
        deleted via ``delete`` and not enqueued.
        '''
        shuffled_items: list[str] = [_enc(_min('ABC'))]
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp('ABC')},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        video_fm.delete.assert_awaited_once_with(
            _min('ABC'), fail_ok=False,
        )
        self.assertTrue(queue.empty())

    async def test_video_min_superseded_by_uploaded_dlp_skipped(
        self,
    ) -> None:
        '''
        A base video-min with a dlp counterpart in uploaded_dir is
        deleted via ``delete`` (not ``delete_uploaded``) and not
        enqueued.
        '''
        shuffled_items: list[str] = [_enc(_min('ABC'))]
        video_fm: MagicMock = _make_producer_fm(
            uploaded_dlp_present={_dlp('ABC')},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        video_fm.delete.assert_awaited_once_with(
            _min('ABC'), fail_ok=False,
        )
        video_fm.delete_uploaded.assert_not_awaited()
        self.assertTrue(queue.empty())

    async def test_uploaded_min_superseded_by_base_dlp_skipped(
        self,
    ) -> None:
        '''
        An uploaded video-min with a dlp in base_dir is removed via
        ``delete_uploaded`` and not enqueued.
        '''
        shuffled_items: list[str] = [
            _enc(_min('XYZ'), from_uploaded=True),
        ]
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp('XYZ')},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        video_fm.delete_uploaded.assert_awaited_once_with(
            _min('XYZ'), fail_ok=False,
        )
        video_fm.delete.assert_not_awaited()
        self.assertTrue(queue.empty())

    async def test_uploaded_min_superseded_by_uploaded_dlp_skipped(
        self,
    ) -> None:
        '''
        An uploaded video-min with a dlp in uploaded_dir is removed
        via ``delete_uploaded`` and not enqueued.
        '''
        shuffled_items: list[str] = [
            _enc(_min('XYZ'), from_uploaded=True),
        ]
        video_fm: MagicMock = _make_producer_fm(
            uploaded_dlp_present={_dlp('XYZ')},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        video_fm.delete_uploaded.assert_awaited_once_with(
            _min('XYZ'), fail_ok=False,
        )
        self.assertTrue(queue.empty())

    async def test_uploaded_min_no_dlp_kept(self) -> None:
        '''
        An uploaded video-min with no dlp counterpart anywhere is
        kept in the queue.
        '''
        shuffled_items: list[str] = [
            _enc(_min('XYZ'), from_uploaded=True),
        ]
        video_fm: MagicMock = _make_producer_fm()
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        items: list[WorkItem] = _drain(queue)
        self.assertEqual(items, [WorkItem(_min('XYZ'), True)])
        video_fm.delete.assert_not_awaited()
        video_fm.delete_uploaded.assert_not_awaited()

    async def test_from_uploaded_skips_supersede_check(
        self,
    ) -> None:
        '''
        A ``from_uploaded=True`` dlp item is enqueued without
        consulting ``video_fm.is_superseded``.
        '''
        shuffled_items: list[str] = [
            _enc(_dlp('ABC'), from_uploaded=True),
        ]
        video_fm: MagicMock = _make_producer_fm(
            is_superseded_returns=True,
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        items: list[WorkItem] = _drain(queue)
        self.assertEqual(items, [WorkItem(_dlp('ABC'), True)])
        video_fm.is_superseded.assert_not_called()

    async def test_base_supersede_filters_item(self) -> None:
        '''
        A base-dir dlp item that ``is_superseded`` returns True for
        is filtered out by ``video_needs_uploading``.
        '''
        shuffled_items: list[str] = [_enc(_dlp('ABC'))]
        video_fm: MagicMock = _make_producer_fm(
            is_superseded_returns=True,
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        self.assertTrue(queue.empty())

    async def test_mixed_overlap_multiple_ids(self) -> None:
        '''
        Three IDs with overlapping min/dlp files:

        - ID1: min + dlp both present → min deleted, dlp queued
        - ID2: only min → kept
        - ID3: only dlp → kept

        Exactly one ``delete`` call (for ID1 min), queue contains
        dlp1, min2, dlp3.
        '''
        shuffled_items: list[str] = [
            _enc(_min('ID1')),
            _enc(_dlp('ID1')),
            _enc(_min('ID2')),
            _enc(_dlp('ID3')),
        ]
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp('ID1')},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        video_fm.delete.assert_awaited_once_with(
            _min('ID1'), fail_ok=False,
        )
        items: set[WorkItem] = set(_drain(queue))
        expected: set[WorkItem] = {
            WorkItem(_dlp('ID1'), False),
            WorkItem(_min('ID2'), False),
            WorkItem(_dlp('ID3'), False),
        }
        self.assertEqual(items, expected)

    async def test_multiple_overlapping_ids_all_min_deleted(
        self,
    ) -> None:
        '''
        When every video-min has a matching video-dlp in base_dir,
        each video-min is deleted and none appear in the queue;
        only dlp items are enqueued.
        '''
        vids: list[str] = ['X1', 'X2', 'X3']
        shuffled_items: list[str] = (
            [_enc(_min(v)) for v in vids]
            + [_enc(_dlp(v)) for v in vids]
        )
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp(v) for v in vids},
        )
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], shuffled_items, video_fm, queue)

        self.assertEqual(
            video_fm.delete.await_count, len(vids),
        )
        actual_calls: list = video_fm.delete.await_args_list
        for v in vids:
            import unittest.mock as _mock
            self.assertIn(
                _mock.call(_min(v), fail_ok=False), actual_calls,
            )

        items: set[WorkItem] = set(_drain(queue))
        self.assertEqual(
            items, {WorkItem(_dlp(v), False) for v in vids},
        )

    async def test_empty_lists_no_errors(self) -> None:
        '''
        Empty priority and shuffled lists produce an empty queue
        with no deletion calls.
        '''
        video_fm: MagicMock = _make_producer_fm()
        queue: Queue = Queue(maxsize=1000)

        await _produce_workload([], [], video_fm, queue)

        video_fm.delete.assert_not_awaited()
        self.assertTrue(queue.empty())


# ---------------------------------------------------------------------------
# Class 3: _video_min_superseded_by_dlp — direct helper tests
# ---------------------------------------------------------------------------

class TestVideoMinSupersededByDlp(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    Direct unit tests for ``_video_min_superseded_by_dlp``.
    These supplement the producer tests with explicit
    return-value and deletion-method assertions.
    '''

    async def test_no_dlp_anywhere_returns_false(self) -> None:
        '''Returns ``False`` and makes no deletions.'''
        item: WorkItem = WorkItem(_min('ABC'), False)
        video_fm: MagicMock = _make_producer_fm()

        result: bool = await _video_min_superseded_by_dlp(
            video_fm, item,
        )

        self.assertFalse(result)
        video_fm.delete.assert_not_awaited()
        video_fm.delete_uploaded.assert_not_awaited()

    async def test_base_dlp_present_returns_true_deletes(
        self,
    ) -> None:
        '''
        When base_dir has the dlp counterpart the function returns
        ``True`` and calls ``delete``.
        '''
        item: WorkItem = WorkItem(_min('ABC'), False)
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp('ABC')},
        )

        result: bool = await _video_min_superseded_by_dlp(
            video_fm, item,
        )

        self.assertTrue(result)
        video_fm.delete.assert_awaited_once_with(
            _min('ABC'), fail_ok=False,
        )

    async def test_uploaded_dlp_present_returns_true_deletes(
        self,
    ) -> None:
        '''
        When uploaded_dir has the dlp counterpart the function
        returns ``True`` and calls ``delete``.
        '''
        item: WorkItem = WorkItem(_min('ABC'), False)
        video_fm: MagicMock = _make_producer_fm(
            uploaded_dlp_present={_dlp('ABC')},
        )

        result: bool = await _video_min_superseded_by_dlp(
            video_fm, item,
        )

        self.assertTrue(result)
        video_fm.delete.assert_awaited_once_with(
            _min('ABC'), fail_ok=False,
        )

    async def test_uploaded_item_uses_delete_uploaded(
        self,
    ) -> None:
        '''
        When ``item.from_uploaded=True`` and a dlp counterpart
        exists, ``delete_uploaded`` is called (not ``delete``).
        '''
        item: WorkItem = WorkItem(_min('XYZ'), True)
        video_fm: MagicMock = _make_producer_fm(
            base_dlp_present={_dlp('XYZ')},
        )

        result: bool = await _video_min_superseded_by_dlp(
            video_fm, item,
        )

        self.assertTrue(result)
        video_fm.delete_uploaded.assert_awaited_once_with(
            _min('XYZ'), fail_ok=False,
        )
        video_fm.delete.assert_not_awaited()


# ---------------------------------------------------------------------------
# Class 4: bare-id marker promotion
# ---------------------------------------------------------------------------


class TestPromoteBareIdFiles(unittest.IsolatedAsyncioTestCase):
    '''
    Tests for ``_promote_bare_id_files``.  Operators drop a file
    whose name is exactly an 11-char video ID into the priority
    or base directory; the helper turns it into a proper
    ``video-min-{id}.json.br`` record (or deletes it as redundant
    when an existing record already exists).
    '''

    async def asyncSetUp(self) -> None:
        self._tmp_base: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self._tmp_uploaded: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self._tmp_priority: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.base_dir: Path = Path(self._tmp_base.name)
        self.uploaded_dir: Path = Path(self._tmp_uploaded.name)
        self.priority_dir: Path = Path(self._tmp_priority.name)
        self.video_fm: MagicMock = MagicMock()
        self.video_fm.base_dir = self.base_dir
        self.video_fm.uploaded_dir = self.uploaded_dir

    async def asyncTearDown(self) -> None:
        self._tmp_base.cleanup()
        self._tmp_uploaded.cleanup()
        self._tmp_priority.cleanup()

    async def test_bare_id_in_base_promoted_to_video_min(
        self,
    ) -> None:
        '''
        A bare-id file in ``base_dir`` with no existing record is
        promoted: a ``video-min-{id}.json.br`` file is written and
        the marker is deleted.
        '''
        vid: str = 'dQw4w9WgXcQ'
        marker: Path = self.base_dir / vid
        marker.touch()

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        produced: Path = self.base_dir / _min(vid)
        self.assertTrue(produced.exists())

    async def test_bare_id_in_priority_promoted_to_video_min(
        self,
    ) -> None:
        '''Same as above but for the priority directory.'''
        vid: str = 'abcDEF12345'
        marker: Path = self.priority_dir / vid
        marker.touch()

        await _promote_bare_id_files(
            self.priority_dir, self.video_fm,
        )

        self.assertFalse(marker.exists())
        produced: Path = self.priority_dir / _min(vid)
        self.assertTrue(produced.exists())

    async def test_marker_redundant_when_video_min_in_base(
        self,
    ) -> None:
        '''
        Existing ``video-min-{id}`` in ``base_dir`` makes the
        marker redundant: it is deleted, no new record is written.
        '''
        vid: str = 'abcDEF12345'
        marker: Path = self.base_dir / vid
        marker.touch()
        existing: Path = self.base_dir / _min(vid)
        existing.write_bytes(b'pre-existing')

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        self.assertEqual(existing.read_bytes(), b'pre-existing')

    async def test_marker_redundant_when_video_dlp_in_base(
        self,
    ) -> None:
        '''Existing ``video-dlp-{id}`` in ``base_dir`` is enough.'''
        vid: str = 'abcDEF12345'
        marker: Path = self.base_dir / vid
        marker.touch()
        existing: Path = self.base_dir / _dlp(vid)
        existing.write_bytes(b'dlp')

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        # No video-min created.
        self.assertFalse((self.base_dir / _min(vid)).exists())

    async def test_marker_redundant_when_video_min_in_uploaded(
        self,
    ) -> None:
        '''Existing ``video-min-{id}`` in ``uploaded_dir`` counts.'''
        vid: str = 'abcDEF12345'
        marker: Path = self.base_dir / vid
        marker.touch()
        (self.uploaded_dir / _min(vid)).write_bytes(b'min-up')

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        self.assertFalse((self.base_dir / _min(vid)).exists())

    async def test_marker_redundant_when_video_dlp_in_uploaded(
        self,
    ) -> None:
        '''Existing ``video-dlp-{id}`` in ``uploaded_dir`` counts.'''
        vid: str = 'abcDEF12345'
        marker: Path = self.base_dir / vid
        marker.touch()
        (self.uploaded_dir / _dlp(vid)).write_bytes(b'dlp-up')

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        self.assertFalse((self.base_dir / _min(vid)).exists())

    async def test_non_matching_names_ignored(self) -> None:
        '''
        Files whose name is not exactly 11 chars from the video-ID
        alphabet are left untouched.
        '''
        kept_short: Path = self.base_dir / 'README'
        kept_long: Path = self.base_dir / 'thisistoolong12'
        kept_dotted: Path = self.base_dir / 'abcDEF12345.txt'
        for f in (kept_short, kept_long, kept_dotted):
            f.write_bytes(b'x')

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        for f in (kept_short, kept_long, kept_dotted):
            self.assertTrue(f.exists(), f'{f.name} should be kept')

    async def test_missing_directory_is_noop(self) -> None:
        '''A non-existent directory returns without error.'''
        await _promote_bare_id_files(
            self.base_dir / 'does-not-exist', self.video_fm,
        )

    async def test_empty_marker_file_still_promoted(self) -> None:
        '''Marker file contents are ignored — empty is fine.'''
        vid: str = 'abcDEF12345'
        marker: Path = self.base_dir / vid
        marker.touch()
        self.assertEqual(marker.stat().st_size, 0)

        await _promote_bare_id_files(self.base_dir, self.video_fm)

        self.assertFalse(marker.exists())
        self.assertTrue((self.base_dir / _min(vid)).exists())


# ---------------------------------------------------------------------------
# Class 5: shuffled-buffer encoding helpers
# ---------------------------------------------------------------------------


class TestShuffledEntryEncoding(unittest.TestCase):
    '''
    Round-trip tests for ``_encode_shuffled_entry`` /
    ``_decode_shuffled_entry``.  The shuffled candidate buffer
    in ``prepare_workload`` stores plain ``str`` entries to avoid
    ~80 bytes of ``WorkItem`` tuple overhead per item; these
    helpers carry the single ``from_uploaded`` bit alongside the
    filename via a sentinel prefix.
    '''

    def test_base_entry_has_no_prefix(self) -> None:
        '''A ``from_uploaded=False`` entry is the bare filename.'''
        encoded: str = _encode_shuffled_entry(
            _min('A'), from_uploaded=False,
        )
        self.assertEqual(encoded, _min('A'))

    def test_uploaded_entry_has_sentinel_prefix(self) -> None:
        '''A ``from_uploaded=True`` entry starts with the sentinel.'''
        encoded: str = _encode_shuffled_entry(
            _min('A'), from_uploaded=True,
        )
        self.assertTrue(encoded.startswith(_FROM_UPLOADED_SENTINEL))
        self.assertEqual(
            encoded[len(_FROM_UPLOADED_SENTINEL):], _min('A'),
        )

    def test_decode_base(self) -> None:
        filename: str
        from_uploaded: bool
        filename, from_uploaded = _decode_shuffled_entry(
            _enc(_min('A'), from_uploaded=False),
        )
        self.assertEqual(filename, _min('A'))
        self.assertFalse(from_uploaded)

    def test_decode_uploaded(self) -> None:
        filename: str
        from_uploaded: bool
        filename, from_uploaded = _decode_shuffled_entry(
            _enc(_min('A'), from_uploaded=True),
        )
        self.assertEqual(filename, _min('A'))
        self.assertTrue(from_uploaded)

    def test_round_trip_base(self) -> None:
        filename: str
        from_uploaded: bool
        filename, from_uploaded = _decode_shuffled_entry(
            _encode_shuffled_entry(_dlp('XYZ'), False),
        )
        self.assertEqual((filename, from_uploaded), (_dlp('XYZ'), False))

    def test_round_trip_uploaded(self) -> None:
        filename: str
        from_uploaded: bool
        filename, from_uploaded = _decode_shuffled_entry(
            _encode_shuffled_entry(_min('XYZ'), True),
        )
        self.assertEqual((filename, from_uploaded), (_min('XYZ'), True))


if __name__ == '__main__':
    unittest.main()
