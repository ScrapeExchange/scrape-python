'''
Unit tests for tools/yt_rss_dropped_channels_report.

Only the pure helpers are exercised — the Elasticsearch
query path is integration-ish (needs a live ES) and is out
of scope for the unit suite.
'''

import io
import json
import unittest

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from tools.yt_rss_dropped_channels_report import (
    _parse_when,
    render_csv,
    render_json,
    render_text,
    summarise,
)


class TestParseWhen(unittest.TestCase):
    NOW: datetime = datetime(
        2026, 4, 22, 12, 0, 0, tzinfo=UTC,
    )

    def test_literal_now(self) -> None:
        self.assertEqual(
            _parse_when('now', self.NOW), self.NOW,
        )

    def test_hours_relative(self) -> None:
        self.assertEqual(
            _parse_when('6h', self.NOW),
            self.NOW - timedelta(hours=6),
        )

    def test_minutes_relative(self) -> None:
        self.assertEqual(
            _parse_when('15m', self.NOW),
            self.NOW - timedelta(minutes=15),
        )

    def test_seconds_relative(self) -> None:
        self.assertEqual(
            _parse_when('30s', self.NOW),
            self.NOW - timedelta(seconds=30),
        )

    def test_days_relative(self) -> None:
        self.assertEqual(
            _parse_when('2d', self.NOW),
            self.NOW - timedelta(days=2),
        )

    def test_iso_timestamp(self) -> None:
        self.assertEqual(
            _parse_when(
                '2026-04-22T06:00:00+00:00', self.NOW,
            ),
            datetime(
                2026, 4, 22, 6, 0, 0, tzinfo=UTC,
            ),
        )

    def test_iso_timestamp_z_suffix(self) -> None:
        self.assertEqual(
            _parse_when(
                '2026-04-22T06:00:00Z', self.NOW,
            ),
            datetime(
                2026, 4, 22, 6, 0, 0, tzinfo=UTC,
            ),
        )


def _hit(
    cid: str,
    ts: str,
    fail_count: int = 50,
    threshold: int = 50,
    name: str = '',
) -> dict:
    return {
        '_source': {
            'channel_id': cid,
            'channel_name': name or f'ch-{cid}',
            'fail_count': fail_count,
            'threshold': threshold,
            'proxy_ip': '192.168.1.16',
            '@timestamp': ts,
        },
    }


class TestSummarise(unittest.TestCase):
    def test_empty_hits_returns_empty(self) -> None:
        self.assertEqual(summarise([]), [])

    def test_single_hit_produces_single_row(self) -> None:
        rows = summarise([
            _hit('UCa', '2026-04-22T12:00:00Z'),
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['channel_id'], 'UCa')
        self.assertEqual(rows[0]['drop_events'], 1)

    def test_duplicate_hits_collapse_with_count(
        self,
    ) -> None:
        rows = summarise([
            _hit('UCa', '2026-04-22T12:00:00Z'),
            _hit('UCa', '2026-04-22T11:00:00Z'),
            _hit('UCa', '2026-04-22T10:00:00Z'),
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['drop_events'], 3)
        # Most recent timestamp wins for last_seen_at.
        self.assertEqual(
            rows[0]['last_seen_at'],
            '2026-04-22T12:00:00Z',
        )

    def test_rows_sorted_by_last_seen_desc(self) -> None:
        rows = summarise([
            _hit('UCa', '2026-04-22T10:00:00Z'),
            _hit('UCb', '2026-04-22T12:00:00Z'),
            _hit('UCc', '2026-04-22T11:00:00Z'),
        ])
        self.assertEqual(
            [r['channel_id'] for r in rows],
            ['UCb', 'UCc', 'UCa'],
        )

    def test_hit_without_channel_id_skipped(self) -> None:
        rows = summarise([
            {
                '_source': {
                    '@timestamp': '2026-04-22T12:00:00Z',
                },
            },
            _hit('UCa', '2026-04-22T11:00:00Z'),
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['channel_id'], 'UCa')


class TestRenderers(unittest.TestCase):
    def test_render_json_shape(self) -> None:
        rows = summarise([
            _hit(
                'UCa', '2026-04-22T12:00:00Z',
                fail_count=55, threshold=50,
            ),
        ])
        buf = io.StringIO()
        with patch('sys.stdout', buf):
            render_json(
                rows, total=1,
                start=datetime(
                    2026, 4, 22, 0, 0,
                    tzinfo=UTC,
                ),
                end=datetime(
                    2026, 4, 22, 12, 0,
                    tzinfo=UTC,
                ),
            )
        payload = json.loads(buf.getvalue())
        self.assertEqual(
            payload['distinct_channels_dropped'], 1,
        )
        self.assertEqual(
            payload['total_drop_events'], 1,
        )
        self.assertEqual(
            payload['channels'][0]['channel_id'], 'UCa',
        )
        self.assertEqual(
            payload['channels'][0]['fail_count'], 55,
        )

    def test_render_csv_has_header_and_rows(self) -> None:
        rows = summarise([
            _hit('UCa', '2026-04-22T12:00:00Z'),
            _hit('UCb', '2026-04-22T11:00:00Z'),
        ])
        buf = io.StringIO()
        with patch('sys.stdout', buf):
            render_csv(rows)
        lines: list[str] = (
            buf.getvalue().strip().splitlines()
        )
        self.assertEqual(len(lines), 3)
        self.assertIn('channel_id', lines[0])
        self.assertIn('UCa', lines[1])
        self.assertIn('UCb', lines[2])

    def test_render_text_includes_summary(self) -> None:
        rows = summarise([
            _hit('UCa', '2026-04-22T12:00:00Z'),
        ])
        out: list[str] = []
        render_text(
            rows, total=1,
            start=datetime(
                2026, 4, 22, 0, 0, tzinfo=UTC,
            ),
            end=datetime(
                2026, 4, 22, 12, 0, tzinfo=UTC,
            ),
            out=out,
        )
        joined: str = '\n'.join(out)
        self.assertIn(
            'Distinct channels dropped: 1', joined,
        )
        self.assertIn(
            'Total drop events in window: 1', joined,
        )
        self.assertIn('UCa', joined)


if __name__ == '__main__':
    unittest.main()
