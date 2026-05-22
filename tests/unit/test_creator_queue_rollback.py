'''Tests for CreatorQueue.rollback_no_feeds() — both backends.'''

import asyncio
import tempfile
import unittest

from scrape_exchange.creator_queue import FileCreatorQueue


def _run(coro):
    loop: asyncio.AbstractEventLoop = (
        asyncio.new_event_loop()
    )
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestFileRollback(unittest.TestCase):

    def test_decrement_and_clear_when_zero(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            q = FileCreatorQueue(
                queue_file=f'{d}/q.lst',
                no_feeds_file=f'{d}/nf.lst',
                had_feed_file=f'{d}/hf.lst',
            )

            async def run() -> None:
                # Build up to count=2 via two
                # set_no_feeds calls.
                await q.set_no_feeds(
                    'UC1',
                    'http://example/rss',
                    'n',
                    1,
                )
                await q.set_no_feeds(
                    'UC1',
                    'http://example/rss',
                    'n',
                    1,
                )
                entry = await q.get_no_feeds('UC1')
                self.assertIsNotNone(entry)
                self.assertEqual(entry[2], 2)

                # Rollback decrements by 1.
                await q.rollback_no_feeds('UC1')
                entry = await q.get_no_feeds('UC1')
                self.assertIsNotNone(entry)
                self.assertEqual(entry[2], 1)

                # Rollback to 0 clears the entry entirely.
                await q.rollback_no_feeds('UC1')
                entry = await q.get_no_feeds('UC1')
                self.assertIsNone(entry)

            _run(run())

    def test_rollback_unknown_channel_is_noop(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            q = FileCreatorQueue(
                queue_file=f'{d}/q.lst',
                no_feeds_file=f'{d}/nf.lst',
                had_feed_file=f'{d}/hf.lst',
            )

            async def run() -> None:
                # No-op (defensive — another worker may
                # have already cleared the entry).
                await q.rollback_no_feeds('UNKNOWN')
                self.assertIsNone(
                    await q.get_no_feeds('UNKNOWN'),
                )

            _run(run())


if __name__ == '__main__':
    unittest.main()
