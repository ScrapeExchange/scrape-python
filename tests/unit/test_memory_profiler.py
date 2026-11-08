import asyncio
import tracemalloc
import unittest

from scrape_exchange import memory_profiler as mod


class TestRunMemoryProfiler(unittest.IsolatedAsyncioTestCase):

    async def test_disabled_does_not_trace(self) -> None:
        await mod.run_memory_profiler(0)
        self.assertFalse(tracemalloc.is_tracing())

    async def test_reports_growth_since_baseline(self) -> None:
        leak: list[bytes] = []

        async def _leak() -> None:
            while True:
                leak.append(bytes(10_000))
                await asyncio.sleep(0.005)

        leaker: asyncio.Task[None] = asyncio.create_task(_leak())
        profiler: asyncio.Task[None] = asyncio.create_task(
            mod.run_memory_profiler(1, frames=4),
        )
        try:
            with self.assertLogs(mod.__name__, level='INFO') as logs:
                await asyncio.sleep(2.6)
        finally:
            profiler.cancel()
            leaker.cancel()
            await asyncio.gather(
                profiler, leaker, return_exceptions=True,
            )

        messages: list[str] = [r.getMessage() for r in logs.records]
        self.assertIn('Memory profile', messages)
        self.assertIn('Memory growth by line', messages)
        lines = [
            r for r in logs.records
            if r.getMessage() == 'Memory growth by line'
        ]
        self.assertGreater(lines[0].size_diff_bytes, 0)
        self.assertIn('test_memory_profiler', lines[0].location)
        self.assertFalse(tracemalloc.is_tracing())
