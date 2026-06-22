'''
Unit tests for tools.scrape_queue — the thin platform/entity-agnostic
operator CLI. The adapter is mocked; dispatch + arg parsing are
under test.
'''

import contextlib
import io
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from tools import scrape_queue as tool
from scrape_exchange.queue_admin import ImportReport


async def _agen(items: list[dict]):
    for item in items:
        yield item


def _adapter() -> MagicMock:
    a: MagicMock = MagicMock()
    a.platform = 'tiktok'
    a.entity = 'creator'
    a.member_label = 'username'
    a.states = MagicMock(
        return_value=['queued', 'claimed', 'removed'],
    )
    a.count_by_state = AsyncMock(
        return_value={'queued': 2, 'claimed': 0, 'removed': 1},
    )
    a.show = AsyncMock(
        return_value={
            'creator_id': 'x', 'name': 'x', 'state': 'queued',
            'tier': 1, 'score': 1.0,
        },
    )
    a.search = AsyncMock(return_value=[])
    a.add = AsyncMock(return_value=2)
    a.remove = AsyncMock(return_value=True)
    a.rescrape = AsyncMock(return_value=1)
    a.import_members = AsyncMock(
        return_value=ImportReport(
            total_lines=9,
            added=3,
            duplicates=2,
            invalid=1,
            blank=2,
            comments=1,
        ),
    )
    a.close = AsyncMock()
    return a


def _video_adapter() -> MagicMock:
    a: MagicMock = _adapter()
    a.entity = 'video'
    a.member_label = 'video_id'
    a.search = AsyncMock(
        return_value=[
            {
                'video_id': '7000000000000000001',
                'state': 'queued',
            },
        ],
    )
    return a


class TestQueueCli(unittest.IsolatedAsyncioTestCase):

    async def _run(
        self, argv: list[str], adapter: MagicMock,
    ) -> int:
        with patch.object(
            tool, 'get_adapter', return_value=adapter,
        ), patch.object(tool, 'QueueToolSettings'):
            return await tool.main_async(argv)

    async def test_stats_dispatches_and_closes(self) -> None:
        a: MagicMock = _adapter()
        rc: int = await self._run(['stats'], a)
        self.assertEqual(rc, 0)
        a.count_by_state.assert_awaited_once()
        a.close.assert_awaited_once()

    async def test_count_dispatches(self) -> None:
        a: MagicMock = _adapter()
        rc: int = await self._run(['count'], a)
        self.assertEqual(rc, 0)
        a.count_by_state.assert_awaited_once()

    async def test_show_passes_member_id(self) -> None:
        a: MagicMock = _adapter()
        await self._run(['show', 'alice'], a)
        a.show.assert_awaited_once_with('alice')

    async def test_search_passes_term_and_limit(self) -> None:
        a: MagicMock = _adapter()
        await self._run(['search', 'char', '--limit', '5'], a)
        a.search.assert_awaited_once_with('char', 5)

    async def test_video_search_uses_video_id_label(self) -> None:
        a: MagicMock = _video_adapter()
        rc: int = await self._run(
            ['--platform', 'tiktok', '--entity', 'video',
             'search', '7000'],
            a,
        )
        self.assertEqual(rc, 0)
        a.search.assert_awaited_once_with('7000', 20)

    async def test_add_builds_weighted_pairs(self) -> None:
        a: MagicMock = _adapter()
        await self._run(
            ['add', 'alice', 'bob', '--weight', '100'], a,
        )
        a.add.assert_awaited_once_with(
            [('alice', 100), ('bob', 100)],
        )

    async def test_add_reads_members_from_stdin(self) -> None:
        a: MagicMock = _adapter()
        stdin: io.StringIO = io.StringIO(
            '@alice\n\n# ignored\nbob\n',
        )
        with patch.object(sys, 'stdin', stdin):
            await self._run(['add', '--weight', '100'], a)
        a.add.assert_awaited_once_with(
            [('alice', 100), ('bob', 100)],
        )

    async def test_add_dash_splices_members_from_stdin(self) -> None:
        a: MagicMock = _adapter()
        stdin: io.StringIO = io.StringIO('@bob\n@carol\n')
        with patch.object(sys, 'stdin', stdin):
            await self._run(
                ['add', 'alice', '-', '--weight', '100'], a,
            )
        a.add.assert_awaited_once_with(
            [('alice', 100), ('bob', 100), ('carol', 100)],
        )

    async def test_add_accepts_short_url(self) -> None:
        a: MagicMock = _adapter()
        await self._run(
            ['add', 'https://vm.tiktok.com/ZGJEytV2E/?x=y',
             '--weight', '0'],
            a,
        )
        a.add.assert_awaited_once_with(
            [('https://vm.tiktok.com/ZGJEytV2E', 0)],
        )

    async def test_add_normalizes_at_prefix(self) -> None:
        a: MagicMock = _adapter()
        await self._run(
            ['add', '@alice', '--weight', '0'], a,
        )
        a.add.assert_awaited_once_with(
            [('alice', 0)],
        )

    async def test_add_normalizes_url(self) -> None:
        a: MagicMock = _adapter()
        await self._run(
            [
                'add',
                'https://www.tiktok.com/@alice?x=y',
                '--weight', '0',
            ], a,
        )
        a.add.assert_awaited_once_with(
            [('alice', 0)],
        )

    async def test_add_skips_invalid(self) -> None:
        a: MagicMock = _adapter()
        await self._run(
            ['add', 'good', 'bad@handle', 'url'], a,
        )
        a.add.assert_awaited_once_with([('good', 0), ('url', 0)])

    async def test_video_add_does_not_use_creator_normalizer(
        self,
    ) -> None:
        a: MagicMock = _video_adapter()
        await self._run(
            [
                '--platform', 'tiktok', '--entity', 'video',
                'add',
                'https://www.tiktok.com/@alice/video/'
                '7000000000000000001?x=y',
            ],
            a,
        )
        a.add.assert_awaited_once_with(
            [
                (
                    'https://www.tiktok.com/@alice/video/'
                    '7000000000000000001?x=y',
                    0,
                ),
            ],
        )

    async def test_remove_each_id(self) -> None:
        a: MagicMock = _adapter()
        await self._run(['remove', 'alice', 'bob'], a)
        self.assertEqual(a.remove.await_count, 2)

    async def test_rescrape_passes_ids(self) -> None:
        a: MagicMock = _adapter()
        await self._run(['rescrape', 'alice', 'bob'], a)
        a.rescrape.assert_awaited_once_with(['alice', 'bob'])

    async def test_import_passes_path(self) -> None:
        a: MagicMock = _adapter()
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('alice\nbob\n')
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                await self._run(['import', path], a)
        a.import_members.assert_awaited_once_with(path)
        self.assertEqual(
            stdout.getvalue(),
            'lines=9 added=3 duplicates=2 invalid=1 '
            'blank=2 comments=1\n',
        )

    async def test_video_import_delegates_to_adapter(self) -> None:
        a: MagicMock = _video_adapter()
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'videos.jsonl')
            with open(path, 'w') as fd:
                fd.write('{"video_id":"7000000000000000001"}\n')
            await self._run(
                ['--platform', 'tiktok', '--entity', 'video',
                 'import', path],
                a,
            )
        a.import_members.assert_awaited_once_with(path)
        a.add.assert_not_awaited()

    async def test_import_normalization_lives_in_adapter(self) -> None:
        a: MagicMock = _adapter()
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('@charlidamelio\n@addisonre\n')
            await self._run(['import', path], a)
        a.import_members.assert_awaited_once_with(path)
        a.add.assert_not_awaited()

    async def test_import_url_normalization_lives_in_adapter(
        self,
    ) -> None:
        a: MagicMock = _adapter()
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('https://www.tiktok.com/@charlidamelio?x=y\n')
            await self._run(['import', path], a)
        a.import_members.assert_awaited_once_with(path)
        a.add.assert_not_awaited()

    async def test_non_tiktok_import_delegates_to_adapter(self) -> None:
        a: MagicMock = _adapter()
        a.platform = 'youtube'
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('@not-normalized-by-cli\n')
            await self._run(
                ['--platform', 'youtube', '--entity', 'creator',
                 'import', path],
                a,
            )
        a.import_members.assert_awaited_once_with(path)
        a.add.assert_not_awaited()

    async def test_non_creator_tiktok_add_is_not_normalized(
        self,
    ) -> None:
        a: MagicMock = _video_adapter()
        await self._run(
            ['--platform', 'tiktok', '--entity', 'video',
             'add', '@alice'],
            a,
        )
        a.add.assert_awaited_once_with([('@alice', 0)])

    async def test_export_csv_default(self) -> None:
        a: MagicMock = _adapter()
        data: list[dict] = [
            {
                'creator_id': 'alice', 'name': 'alice',
                'state': 'queued', 'tier': 1, 'score': 1.0,
            },
            {
                'creator_id': 'bob', 'name': 'bob',
                'state': 'removed', 'tier': 3, 'score': None,
            },
        ]
        a.export = lambda: _agen(data)
        buf: io.StringIO = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc: int = await self._run(['export'], a)
        self.assertEqual(rc, 0)
        lines: list[str] = [
            ln for ln in buf.getvalue().splitlines() if ln.strip()
        ]
        self.assertEqual(lines, ['alice,queued,1', 'bob,removed,3'])

    async def test_export_jsonl(self) -> None:
        a: MagicMock = _adapter()
        data: list[dict] = [
            {
                'creator_id': 'alice', 'name': 'alice',
                'state': 'queued', 'tier': 1, 'score': 1.0,
            },
            {
                'creator_id': 'bob', 'name': 'bob',
                'state': 'removed', 'tier': 3, 'score': None,
            },
        ]
        a.export = lambda: _agen(data)
        buf: io.StringIO = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc: int = await self._run(['export', '--jsonl'], a)
        self.assertEqual(rc, 0)
        lines: list[str] = [
            ln for ln in buf.getvalue().splitlines() if ln.strip()
        ]
        first: dict = json.loads(lines[0])
        self.assertEqual(first['creator_id'], 'alice')
        self.assertEqual(first['state'], 'queued')

    async def test_export_output_appends_csv(self) -> None:
        a: MagicMock = _adapter()
        data: list[dict] = [
            {
                'creator_id': 'alice', 'name': 'alice',
                'state': 'queued', 'tier': 1, 'score': 1.0,
            },
        ]
        a.export = lambda: _agen(data)
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'queue.csv')
            Path(path).write_text(
                'existing,removed,3\n',
                encoding='utf-8',
            )
            rc: int = await self._run(['export', '--output', path], a)
            output: str = Path(path).read_text(encoding='utf-8')

        self.assertEqual(rc, 0)
        self.assertEqual(output, 'existing,removed,3\nalice,queued,1\n')

    async def test_export_output_appends_jsonl(self) -> None:
        a: MagicMock = _adapter()
        data: list[dict] = [
            {
                'creator_id': 'alice', 'name': 'alice',
                'state': 'queued', 'tier': 1, 'score': 1.0,
            },
        ]
        a.export = lambda: _agen(data)
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path: str = _os.path.join(d, 'queue.jsonl')
            Path(path).write_text(
                '{"creator_id":"existing"}\n',
                encoding='utf-8',
            )
            rc: int = await self._run(
                ['export', '--jsonl', '--output', path],
                a,
            )
            lines: list[str] = Path(path).read_text(
                encoding='utf-8',
            ).splitlines()

        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(lines[0])['creator_id'], 'existing')
        self.assertEqual(json.loads(lines[1])['creator_id'], 'alice')

    async def test_export_state_filter(self) -> None:
        a: MagicMock = _adapter()
        data: list[dict] = [
            {
                'creator_id': 'alice', 'name': 'alice',
                'state': 'queued', 'tier': 1, 'score': 1.0,
            },
            {
                'creator_id': 'bob', 'name': 'bob',
                'state': 'removed', 'tier': 3, 'score': None,
            },
        ]
        a.export = lambda: _agen(data)
        buf: io.StringIO = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc: int = await self._run(
                ['export', '--state', 'removed'], a,
            )
        self.assertEqual(rc, 0)
        lines: list[str] = [
            ln for ln in buf.getvalue().splitlines() if ln.strip()
        ]
        self.assertEqual(lines, ['bob,removed,3'])

    async def test_export_invalid_state_exits_2(self) -> None:
        a: MagicMock = _adapter()
        a.export = lambda: _agen([])
        rc: int = await self._run(
            ['export', '--state', 'bogus'], a,
        )
        self.assertEqual(rc, 2)

    async def test_export_video_uses_video_id(self) -> None:
        a: MagicMock = _video_adapter()
        a.export = lambda: _agen([
            {'video_id': '7000000000000000001', 'state': 'queued'},
        ])
        buf: io.StringIO = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc: int = await self._run(
                [
                    '--platform', 'tiktok', '--entity', 'video',
                    'export',
                ],
                a,
            )
        self.assertEqual(rc, 0)
        lines: list[str] = [
            ln for ln in buf.getvalue().splitlines() if ln.strip()
        ]
        self.assertEqual(lines, ['7000000000000000001,queued,'])

    async def test_unknown_platform_exits_nonzero(self) -> None:
        with patch.object(
            tool, 'get_adapter',
            side_effect=ValueError('no adapter'),
        ), patch.object(tool, 'QueueToolSettings'):
            rc: int = await tool.main_async(
                ['--platform', 'myspace', 'count'],
            )
        self.assertNotEqual(rc, 0)


class TestQueueCommandExecution(unittest.TestCase):

    def test_console_script_reaches_cli_error_path(self) -> None:
        root: Path = Path(__file__).parents[2]
        proc: subprocess.CompletedProcess[str] = subprocess.run(
            [
                'scrape-queue',
                '--platform',
                'nope',
                'count',
            ],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn('no queue adapter', proc.stderr)
        self.assertNotIn('ImportError', proc.stderr)
        self.assertNotIn("from 'queue'", proc.stderr)


if __name__ == '__main__':
    unittest.main()
