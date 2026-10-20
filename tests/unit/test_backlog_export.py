'''Unit tests for tools.backlog_export.'''

import unittest
from typing import Any

from tools.backlog_export import (
    ExportCounts,
    format_queued_line,
    format_terminal_line,
    route_document,
)

VID: str = 'dQw4w9WgXcQ'


class TestFormatLines(unittest.TestCase):

    def test_queued_document(self) -> None:
        doc: dict[str, Any] = {
            '_id': VID, 'state': 'queued', 'enqueued_at': 1700000000,
        }
        self.assertEqual(format_queued_line(doc), f'{VID}\t1700000000')

    def test_hot_document(self) -> None:
        doc: dict[str, Any] = {'_id': VID, 'state': 'hot', 'enqueued_at': 5}
        self.assertEqual(format_queued_line(doc), f'{VID}\t5')

    def test_missing_enqueued_at_is_zero(self) -> None:
        doc: dict[str, Any] = {'_id': VID, 'state': 'queued'}
        self.assertEqual(format_queued_line(doc), f'{VID}\t0')

    def test_failed_with_record(self) -> None:
        doc: dict[str, Any] = {
            '_id': VID, 'state': 'failed', 'enqueued_at': 10,
            'record': {'last_error': 'bot_detection', 'ts': 99},
        }
        self.assertEqual(
            format_terminal_line(doc), f'{VID}\tfailed\tbot_detection\t99',
        )

    def test_retries_exhausted_reason(self) -> None:
        doc: dict[str, Any] = {
            '_id': VID, 'state': 'failed', 'enqueued_at': 10,
            'record': {
                'last_error': 'transient retries exhausted: rate_limit',
                'ts': 12.5,
            },
        }
        self.assertEqual(
            format_terminal_line(doc),
            f'{VID}\tfailed\tretries_exhausted\t12',
        )

    def test_unavailable_without_record(self) -> None:
        doc: dict[str, Any] = {
            '_id': VID, 'state': 'unavailable', 'enqueued_at': 10,
        }
        self.assertEqual(
            format_terminal_line(doc), f'{VID}\tunavailable\tnone\t10',
        )

    def test_terminal_line_rejects_queued(self) -> None:
        self.assertIsNone(format_terminal_line({'_id': VID, 'state': 'hot'}))


class TestRouteDocument(unittest.TestCase):

    def test_routes_and_counts(self) -> None:
        counts: ExportCounts = ExportCounts()
        docs: list[dict[str, Any]] = [
            {'_id': VID, 'state': 'queued', 'enqueued_at': 1},
            {'_id': VID, 'state': 'removed', 'enqueued_at': 2},
            {'_id': VID, 'state': 'weird'},
            {'_id': None, 'state': 'queued'},
        ]
        routed: list[tuple[str, str] | None] = [
            route_document(d, counts) for d in docs
        ]
        self.assertEqual(routed[0], ('queued', f'{VID}\t1'))
        self.assertEqual(routed[1], ('terminal', f'{VID}\tremoved\tnone\t2'))
        self.assertIsNone(routed[2])
        self.assertIsNone(routed[3])
        self.assertEqual(
            (counts.queued, counts.terminal, counts.skipped), (1, 1, 2),
        )


if __name__ == '__main__':
    unittest.main()
