'''Unit tests for scrape_exchange.backlog_server_queue.'''

import unittest
from typing import Any

from scrape_exchange.backlog_server_queue import (
    BacklogServerUnsupported,
    BacklogServerVideoScrapeQueue,
    normalize_reason,
)
from scrape_exchange.video_scrape_queue import (
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
)

QUEUE: str = 'youtube:video'


class FakeRedis:
    '''Records execute_command calls and returns canned replies.'''

    def __init__(self, replies: list[Any] | None = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.replies: list[Any] = list(replies or [])

    async def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return self.replies.pop(0) if self.replies else None


def make(
    replies: list[Any] | None = None, *, supports_uploaded: bool = True,
) -> tuple[BacklogServerVideoScrapeQueue, FakeRedis]:
    fake: FakeRedis = FakeRedis(replies)
    queue: BacklogServerVideoScrapeQueue = BacklogServerVideoScrapeQueue(
        fake, VideoScrapeQueueSettings(),  # type: ignore[arg-type]
        supports_uploaded=supports_uploaded,
    )
    return queue, fake


class TestBacklogServerQueue(unittest.IsolatedAsyncioTestCase):

    async def test_pop_entries_carries_force(self) -> None:
        queue, fake = make([[['aaaaaaaaaaA', 0], ['bbbbbbbbbbA', 1]]])
        entries: list[VideoScrapeQueueEntry] = await queue.pop_entries(2)
        self.assertEqual(fake.calls, [('BACKLOG.POP', QUEUE, 2)])
        self.assertEqual(
            [e.video_id for e in entries], ['aaaaaaaaaaA', 'bbbbbbbbbbA'],
        )
        self.assertEqual(entries[0].meta, {})
        self.assertEqual(entries[1].meta, {'force': '1'})
        self.assertIsNone(entries[1].source)
        self.assertIsNone(entries[1].channel.channel_id)
        self.assertTrue(await queue.consume_force('bbbbbbbbbbA'))
        self.assertFalse(await queue.consume_force('bbbbbbbbbbA'))
        self.assertFalse(await queue.consume_force('aaaaaaaaaaA'))

    async def test_pop_returns_ids(self) -> None:
        queue, _ = make([[['aaaaaaaaaaA', 0]]])
        self.assertEqual(await queue.pop(5), ['aaaaaaaaaaA'])

    async def test_pop_empty(self) -> None:
        queue, _ = make([[]])
        self.assertEqual(await queue.pop_entries(5), [])

    async def test_enqueue_single(self) -> None:
        queue, fake = make([[1], [0]])
        self.assertTrue(await queue.enqueue('dQw4w9WgXcQ', source='rss'))
        self.assertFalse(await queue.enqueue('dQw4w9WgXcQ', source='rss'))
        self.assertEqual(
            fake.calls[0], ('BACKLOG.ADD', QUEUE, 'dQw4w9WgXcQ'),
        )

    async def test_enqueue_invalid_raises(self) -> None:
        queue, _ = make([[-1]])
        with self.assertRaises(ValueError):
            await queue.enqueue('nope', source='rss')

    async def test_enqueue_many_counts_invalid(self) -> None:
        queue, fake = make([[1, -1, 0]])
        with self.assertLogs(
            'scrape_exchange.backlog_server_queue', level='WARNING',
        ) as logs:
            added: int = await queue.enqueue_many(
                ['a' * 11, 'bad', 'b' * 11], source='channel',
            )
        self.assertEqual(added, 1)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(fake.calls[0][:2], ('BACKLOG.ADD', QUEUE))

    async def test_enqueue_many_chunks(self) -> None:
        queue, fake = make([[1, 1], [1]])
        added: int = await queue.enqueue_many(
            ['a' * 11, 'b' * 11, 'c' * 11], source='x', chunk_size=2,
        )
        self.assertEqual(added, 3)
        self.assertEqual(len(fake.calls), 2)

    async def test_mark_normalizes_reason(self) -> None:
        queue, fake = make(['OK'])
        await queue.mark(
            'dQw4w9WgXcQ', state=VideoState.FAILED,
            last_error='transient retries exhausted: rate_limit',
        )
        self.assertEqual(fake.calls[0], (
            'BACKLOG.MARK', QUEUE, 'dQw4w9WgXcQ', 'failed',
            'retries_exhausted',
        ))

    async def test_mark_rejects_queued(self) -> None:
        queue, _ = make()
        with self.assertRaises(ValueError):
            await queue.mark('dQw4w9WgXcQ', state=VideoState.QUEUED)

    async def test_simple_commands(self) -> None:
        queue, fake = make([1, 'revived', 1, 1])
        await queue.complete('dQw4w9WgXcQ')
        self.assertEqual(
            await queue.force_enqueue('dQw4w9WgXcQ', source='op'),
            'revived',
        )
        await queue.unmark('dQw4w9WgXcQ')
        self.assertEqual(
            await queue.bump_attempts('dQw4w9WgXcQ', last_error='x'), 0,
        )
        self.assertEqual([c[0] for c in fake.calls], [
            'BACKLOG.COMPLETE', 'BACKLOG.FORCE', 'BACKLOG.REQUEUE',
            'BACKLOG.TOUCH',
        ])
        await queue.flush()

    async def test_scraped_state_is_none(self) -> None:
        queue, _ = make([['scraped'], [None], ['failed']])
        self.assertIsNone(await queue.get_state('dQw4w9WgXcQ'))
        self.assertIsNone(await queue.get_state('dQw4w9WgXcQ'))
        self.assertEqual(
            await queue.get_state('dQw4w9WgXcQ'), VideoState.FAILED,
        )

    async def test_get_states_maps_each(self) -> None:
        queue, fake = make([['queued', 'scraped', None]])
        states: dict[str, VideoState | None] = await queue.get_states(
            ['a' * 11, 'b' * 11, 'c' * 11],
        )
        self.assertEqual(states, {
            'a' * 11: VideoState.QUEUED, 'b' * 11: None, 'c' * 11: None,
        })
        self.assertEqual(await queue.get_states([]), {})
        self.assertEqual(len(fake.calls), 1)

    async def test_get_state_names_keeps_scraped(self) -> None:
        queue, fake = make([['queued', 'scraped', None, 'failed']])
        names: dict[str, str | None] = await queue.get_state_names(
            ['a' * 11, 'b' * 11, 'c' * 11, 'd' * 11],
        )
        self.assertEqual(names, {
            'a' * 11: 'queued', 'b' * 11: 'scraped',
            'c' * 11: None, 'd' * 11: 'failed',
        })
        self.assertEqual(await queue.get_state_names([]), {})
        self.assertEqual(
            fake.calls,
            [('BACKLOG.STATE', QUEUE, 'a' * 11, 'b' * 11, 'c' * 11,
              'd' * 11)],
        )

    async def test_count_by_state_maps_info(self) -> None:
        queue, _ = make([[
            'queued', 7, 'scraped', 9, 'failed', 2, 'unavailable', 1,
            'removed', 0, 'leased', 3,
        ]])
        counts: dict[VideoState, int] = await queue.count_by_state()
        self.assertEqual(counts, {
            VideoState.QUEUED: 7, VideoState.FAILED: 2,
            VideoState.UNAVAILABLE: 1, VideoState.REMOVED: 0,
        })

    async def test_count_by_state_name_includes_done_states(
        self,
    ) -> None:
        queue, _ = make([[
            'queued', 7, 'scraped', 9, 'failed', 2, 'unavailable', 1,
            'removed', 0, 'uploaded', 5, 'leased', 3,
        ]])
        self.assertEqual(await queue.count_by_state_name(), {
            'queued': 7, 'unavailable': 1, 'failed': 2, 'removed': 0,
            'scraped': 9, 'uploaded': 5,
        })

    async def test_count_by_state_name_without_upload_state(
        self,
    ) -> None:
        queue, _ = make(
            [['queued', 1, 'scraped', 2, 'uploaded', 0]],
            supports_uploaded=False,
        )
        counts: dict[str, int] = await queue.count_by_state_name()
        self.assertEqual(counts['scraped'], 2)
        self.assertNotIn('uploaded', counts)

    async def test_get_meta(self) -> None:
        queue, _ = make([['failed'], ['scraped']])
        self.assertEqual(
            await queue.get_meta('dQw4w9WgXcQ'), {'state': 'failed'},
        )
        self.assertEqual(await queue.get_meta('dQw4w9WgXcQ'), {})

    async def test_unsupported_methods_raise(self) -> None:
        queue, _ = make()
        with self.assertRaises(BacklogServerUnsupported):
            await queue.set_meta('dQw4w9WgXcQ', note='x')
        with self.assertRaises(BacklogServerUnsupported):
            await queue.search_meta('*x*')
        with self.assertRaises(BacklogServerUnsupported):
            async for _ in queue.iter_members():
                pass
        self.assertTrue(issubclass(
            BacklogServerUnsupported, NotImplementedError,
        ))


    async def test_mark_uploaded_sends_batch_command(self) -> None:
        queue, fake = make([2])
        await queue.mark_uploaded(['a' * 11, 'b' * 11])
        self.assertEqual(fake.calls, [
            ('BACKLOG.UPLOADED', QUEUE, 'a' * 11, 'b' * 11),
        ])

    async def test_uploaded_and_done_read_states(self) -> None:
        ids: list[str] = ['a' * 11, 'b' * 11, 'c' * 11, 'd' * 11]
        queue, fake = make([['uploaded', 'scraped', None, 'queued']] * 2)
        self.assertEqual(
            await queue.uploaded_many(ids),
            dict(zip(ids, [True, False, False, False], strict=True)),
        )
        self.assertEqual(
            await queue.done_many(ids),
            dict(zip(ids, [True, True, False, False], strict=True)),
        )
        self.assertEqual(fake.calls, [
            ('BACKLOG.STATE', QUEUE, *ids),
            ('BACKLOG.STATE', QUEUE, *ids),
        ])

    async def test_get_states_maps_done_states_to_none(self) -> None:
        queue, _ = make([['scraped', 'uploaded']])
        self.assertEqual(
            await queue.get_states(['a' * 11, 'b' * 11]),
            {'a' * 11: None, 'b' * 11: None},
        )

    async def test_unknown_state_name_raises(self) -> None:
        queue, _ = make([['bogus']])
        with self.assertRaises(ValueError):
            await queue.get_states(['a' * 11])

    async def test_unsupported_queue_makes_no_calls(self) -> None:
        ids: list[str] = ['a' * 11]
        queue, fake = make([], supports_uploaded=False)
        self.assertIsNone(await queue.mark_uploaded(ids))
        self.assertEqual(await queue.uploaded_many(ids), {'a' * 11: False})
        self.assertEqual(await queue.done_many(ids), {'a' * 11: False})
        self.assertEqual(fake.calls, [])

    async def test_state_names_include_done_states(self) -> None:
        queue, _ = make([], supports_uploaded=True)
        self.assertEqual(queue.state_names(), [
            'queued', 'unavailable', 'failed', 'removed',
            'scraped', 'uploaded',
        ])
        plain, _ = make([], supports_uploaded=False)
        self.assertNotIn('uploaded', plain.state_names())


class TestNormalizeReason(unittest.TestCase):

    def test_normalize_reason_table(self) -> None:
        cases: dict[str | None, str] = {
            None: 'none',
            '': 'none',
            'bot_detection': 'bot_detection',
            'rate_limit_wait': 'rate_limit_wait',
            'premiere': 'premiere',
            'transient retries exhausted: bind_failed': 'retries_exhausted',
            'weird': 'other',
        }
        for raw, want in cases.items():
            self.assertEqual(normalize_reason(raw), want, raw)


if __name__ == '__main__':
    unittest.main()
