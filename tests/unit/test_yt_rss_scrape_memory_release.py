'''Source-level assertion that ``tools.yt_rss_scrape.worker_loop``
releases the startup-only data structures (``channel_map_data``,
``known_ids``, ``subscriber_counts``) before spawning the
long-running streamer tasks.

The streamers run for the lifetime of the process; if the dict
of ~500k channel_id->handle entries stays in the function's
local frame, it stays in RSS for hours of normal operation
even though no streamer reads it.

A runtime test of ``worker_loop`` would require extensive
mocking of populate / cleanup_stale_claims / streamers /
background tasks. Instead, an AST scan locks in the cheaper
structural invariant: every del statement is present, and
each appears AFTER the populate call and BEFORE the streamer
spawn.
'''

from __future__ import annotations

import ast
import inspect
import unittest

from tools import yt_rss_scrape


class TestWorkerLoopReleasesStartupDicts(unittest.TestCase):

    def _worker_loop_ast(self) -> ast.AsyncFunctionDef:
        src: str = inspect.getsource(
            yt_rss_scrape.worker_loop,
        )
        module: ast.Module = ast.parse(src)
        node: ast.AST = module.body[0]
        assert isinstance(node, ast.AsyncFunctionDef)
        return node

    def _line_of_call(
        self,
        node: ast.AsyncFunctionDef,
        attr_name: str,
    ) -> int | None:
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == attr_name
            ):
                return child.lineno
        return None

    def _line_of_del(
        self,
        node: ast.AsyncFunctionDef,
        target_name: str,
    ) -> int | None:
        for child in ast.walk(node):
            if isinstance(child, ast.Delete):
                for tgt in child.targets:
                    if (
                        isinstance(tgt, ast.Name)
                        and tgt.id == target_name
                    ):
                        return child.lineno
        return None

    def test_del_channel_map_data_after_populate(
        self,
    ) -> None:
        node: ast.AsyncFunctionDef = self._worker_loop_ast()
        populate_ln: int | None = self._line_of_call(
            node, 'populate',
        )
        del_ln: int | None = self._line_of_del(
            node, 'channel_map_data',
        )
        self.assertIsNotNone(populate_ln)
        self.assertIsNotNone(del_ln)
        self.assertGreater(del_ln, populate_ln)

    def test_del_known_ids_after_populate(self) -> None:
        node: ast.AsyncFunctionDef = self._worker_loop_ast()
        populate_ln: int | None = self._line_of_call(
            node, 'populate',
        )
        del_ln: int | None = self._line_of_del(
            node, 'known_ids',
        )
        self.assertIsNotNone(populate_ln)
        self.assertIsNotNone(del_ln)
        self.assertGreater(del_ln, populate_ln)

    def test_del_subscriber_counts_after_populate(
        self,
    ) -> None:
        node: ast.AsyncFunctionDef = self._worker_loop_ast()
        populate_ln: int | None = self._line_of_call(
            node, 'populate',
        )
        del_ln: int | None = self._line_of_del(
            node, 'subscriber_counts',
        )
        self.assertIsNotNone(populate_ln)
        self.assertIsNotNone(del_ln)
        self.assertGreater(del_ln, populate_ln)

    def test_dels_before_streamer_spawn(self) -> None:
        '''All three releases must happen before
        ``_stream_processor`` tasks are spawned, otherwise
        the streamers' lifetime extends the dicts' lifetime.
        '''
        node: ast.AsyncFunctionDef = self._worker_loop_ast()
        # The streamer is referenced by name inside the
        # asyncio.create_task call. Find the line of the
        # first such reference.
        streamer_ln: int | None = None
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Name)
                and child.id == '_stream_processor'
            ):
                streamer_ln = child.lineno
                break
        self.assertIsNotNone(streamer_ln)
        for name in (
            'channel_map_data',
            'known_ids',
            'subscriber_counts',
        ):
            del_ln: int | None = self._line_of_del(
                node, name,
            )
            self.assertIsNotNone(del_ln)
            self.assertLess(del_ln, streamer_ln)


if __name__ == '__main__':
    unittest.main()
