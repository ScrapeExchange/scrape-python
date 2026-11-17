'''
Re-check of channels parked in the ``low_subs`` state: selecting the
channels that are due and deciding what happens to each.
'''

import unittest
from unittest.mock import MagicMock

from scrape_exchange.youtube.low_subs_recheck import (
    LowSubsEntry,
    RecheckAction,
    decide,
    effective_count,
    parse_low_subs_entries,
    select_due,
)

NOW: float = 1_000_000.0
DAY: float = 86400.0


def _settings() -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.channel_min_subscribers = 100
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    return settings


def _entry(
    channel_id: str,
    subscriber_count: int | None,
    checked_at: float | None,
) -> LowSubsEntry:
    return LowSubsEntry(
        member=f'i:{channel_id}',
        channel_id=channel_id,
        subscriber_count=subscriber_count,
        view_count=None,
        checked_at=checked_at,
        record={},
    )


class TestSelection(unittest.TestCase):

    def test_parse_skips_handle_members(self) -> None:
        entries: list[LowSubsEntry] = parse_low_subs_entries([
            ('h:@x', '{}'),
            ('i:UCa', '{"subscriber_count": "50"}'),
        ])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].channel_id, 'UCa')
        self.assertEqual(entries[0].member, 'i:UCa')
        self.assertEqual(entries[0].subscriber_count, 50)

    def test_parse_bad_records(self) -> None:
        raw: str | None
        for raw in (
            'not json', None, '[]', '{"checked_at": "soon"}',
        ):
            entries: list[LowSubsEntry] = parse_low_subs_entries(
                [('i:UCb', raw)],
            )
            self.assertEqual(len(entries), 1, raw)
            self.assertIsNone(entries[0].subscriber_count, raw)
            self.assertIsNone(entries[0].checked_at, raw)

    def test_parse_reads_views_and_checked_at(self) -> None:
        entries: list[LowSubsEntry] = parse_low_subs_entries([(
            'i:UCc',
            '{"view_count": "1200", "checked_at": 900000}',
        )])
        self.assertEqual(entries[0].view_count, 1200)
        self.assertEqual(entries[0].checked_at, 900000.0)

    def test_select_skips_recent(self) -> None:
        recent: LowSubsEntry = _entry('UCa', 10, NOW - 10 * DAY)
        old: LowSubsEntry = _entry('UCb', 10, NOW - 31 * DAY)
        due: list[LowSubsEntry]
        skipped: int
        due, skipped = select_due(
            [recent, old], now=NOW, recheck_after_days=30, limit=None,
        )
        self.assertEqual(due, [old])
        self.assertEqual(skipped, 1)

    def test_select_order(self) -> None:
        checked: LowSubsEntry = _entry('UCa', 90, NOW - 40 * DAY)
        n20: LowSubsEntry = _entry('UCb', 20, None)
        unknown: LowSubsEntry = _entry('UCc', None, None)
        n80: LowSubsEntry = _entry('UCd', 80, None)
        due: list[LowSubsEntry]
        due, _ = select_due(
            [checked, n20, unknown, n80],
            now=NOW, recheck_after_days=30, limit=None,
        )
        self.assertEqual(due, [n80, n20, unknown, checked])

    def test_select_limit(self) -> None:
        n20: LowSubsEntry = _entry('UCb', 20, None)
        n80: LowSubsEntry = _entry('UCd', 80, None)
        n50: LowSubsEntry = _entry('UCe', 50, None)
        due: list[LowSubsEntry]
        due, _ = select_due(
            [n20, n80, n50], now=NOW, recheck_after_days=30, limit=2,
        )
        self.assertEqual(due, [n80, n50])


class TestDecide(unittest.TestCase):

    def test_effective_count(self) -> None:
        self.assertEqual(effective_count(None, 500), 500)
        self.assertEqual(effective_count(60, 500), 60)
        self.assertIsNone(effective_count(None, None))

    def test_decide(self) -> None:
        cases: list[tuple[int | None, int | None, bool, RecheckAction]] = [
            (150, None, False, RecheckAction.REQUEUE),
            (99, None, False, RecheckAction.STILL_TINY),
            (None, 10_000, False, RecheckAction.REQUEUE),
            (None, 9_999, False, RecheckAction.STILL_TINY),
            (None, None, False, RecheckAction.STILL_TINY),
            (5, None, True, RecheckAction.TOPIC),
        ]
        subs: int | None
        views: int | None
        topic: bool
        expected: RecheckAction
        for subs, views, topic, expected in cases:
            self.assertEqual(
                decide(subs, views, topic, _settings()), expected,
                (subs, views, topic),
            )


if __name__ == '__main__':
    unittest.main()
