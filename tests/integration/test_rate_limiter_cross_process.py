'''
Cross-process integration test for the shared file-backed rate limiter.

Spawns multiple subprocesses that all point at the same state
directory and race to acquire tokens as fast as they can. Asserts
that the **aggregate** acquisition count across all processes
stays within a small margin of the configured refill rate, which
demonstrates that fcntl.flock + the file backend are correctly
serialising token consumption across process boundaries. If the
limiter silently fell back to per-process buckets, the aggregate
rate would be ``N_processes`` times too high and the test would
fail loudly.
'''

import json
import multiprocessing
import os
import tempfile
import time
import unittest

from scrape_exchange.rate_limiter import (
    _SharedFileBackend,
    _BucketConfig,
)


# A deliberately synthetic call-type enum so we don't depend on the
# real YouTube configs (whose refill rates would make the test
# either too slow or too loose to be diagnostic).
from enum import Enum


class _CT(str, Enum):
    ONE = 'one'


_CONFIGS: dict[_CT, _BucketConfig] = {
    _CT.ONE: _BucketConfig(
        burst=5,
        refill_rate=10.0,
        jitter_min=0.0,
        jitter_max=0.0,
    ),
}
_GLOBAL: _BucketConfig = _BucketConfig(
    burst=5,
    refill_rate=10.0,
    jitter_min=0.0,
    jitter_max=0.0,
)

_PROXY: str = 'http://probe:0'


def _child_worker(
    state_dir: str, duration_seconds: float, result_path: str,
) -> None:
    '''
    Run a tight acquire loop for *duration_seconds*, writing the
    number of tokens successfully acquired to *result_path*.
    '''
    import asyncio as _asyncio
    # Must import here so the child creates its own module-level
    # instance of the backend (simulates a fresh process).
    from scrape_exchange.rate_limiter import (
        _SharedFileBackend as SFB,
    )

    backend = SFB(_CONFIGS, _GLOBAL, state_dir)

    async def run() -> int:
        count: int = 0
        deadline: float = time.time() + duration_seconds
        while time.time() < deadline:
            wait, _, _ = await backend.try_acquire(_CT.ONE, _PROXY)
            if wait <= 0:
                count += 1
            else:
                await _asyncio.sleep(min(wait, 0.05))
        return count

    acquired: int = _asyncio.run(run())
    with open(result_path, 'w') as f:
        json.dump({'acquired': acquired}, f)


class TestCrossProcessRateLimit(unittest.TestCase):
    '''
    Spawn N child processes against the same state dir and verify
    their combined acquisition rate matches the configured refill
    rate — not N× it.
    '''

    def test_aggregate_rate_matches_configured(self) -> None:
        num_children: int = 4
        duration: float = 3.0
        configured_refill: float = _CONFIGS[_CT.ONE].refill_rate
        burst: int = _CONFIGS[_CT.ONE].burst

        with tempfile.TemporaryDirectory() as state_dir:
            ctx = multiprocessing.get_context('spawn')
            result_paths: list[str] = [
                os.path.join(state_dir, f'result_{i}.json')
                for i in range(num_children)
            ]
            procs: list[multiprocessing.Process] = []
            for rp in result_paths:
                p = ctx.Process(
                    target=_child_worker,
                    args=(state_dir, duration, rp),
                )
                p.start()
                procs.append(p)
            for p in procs:
                p.join(timeout=duration + 15.0)
                self.assertFalse(
                    p.is_alive(),
                    'child worker did not exit in time',
                )
                self.assertEqual(p.exitcode, 0)

            totals: list[int] = []
            for rp in result_paths:
                with open(rp) as f:
                    totals.append(json.load(f)['acquired'])
            aggregate: int = sum(totals)

            # Expected: roughly refill_rate * duration + initial burst.
            # Allow +50%/-20% slack to absorb scheduling jitter and
            # flock latency but still catch a missing-lock regression
            # (which would yield ~num_children × this value).
            expected: float = configured_refill * duration + burst
            upper_bound: float = expected * 1.5
            lower_bound: float = max(expected * 0.8, burst + 1)
            self.assertLessEqual(
                aggregate, upper_bound,
                f'aggregate {aggregate} exceeds upper bound '
                f'{upper_bound:.1f} — flock is not serialising?',
            )
            self.assertGreaterEqual(
                aggregate, lower_bound,
                f'aggregate {aggregate} below lower bound '
                f'{lower_bound:.1f} — workers not progressing',
            )

    def test_in_process_per_child_would_multiply(self) -> None:
        '''
        Sanity check / regression guardrail: a single child running
        alone should acquire ``expected`` tokens in *duration*
        seconds. Two children should acquire *roughly* the same
        total because they share state — not 2×.
        '''
        duration: float = 2.0
        configured_refill: float = _CONFIGS[_CT.ONE].refill_rate
        burst: int = _CONFIGS[_CT.ONE].burst

        with tempfile.TemporaryDirectory() as state_dir:
            ctx = multiprocessing.get_context('spawn')

            # Single-child run.
            rp_solo: str = os.path.join(state_dir, 'solo.json')
            p1 = ctx.Process(
                target=_child_worker,
                args=(state_dir, duration, rp_solo),
            )
            p1.start()
            p1.join(timeout=duration + 10.0)
            with open(rp_solo) as f:
                solo: int = json.load(f)['acquired']

            os.unlink(rp_solo)

        # Pair run in a fresh dir.
        with tempfile.TemporaryDirectory() as state_dir2:
            ctx = multiprocessing.get_context('spawn')
            rp1: str = os.path.join(state_dir2, 'a.json')
            rp2: str = os.path.join(state_dir2, 'b.json')
            p1 = ctx.Process(
                target=_child_worker,
                args=(state_dir2, duration, rp1),
            )
            p2 = ctx.Process(
                target=_child_worker,
                args=(state_dir2, duration, rp2),
            )
            p1.start()
            p2.start()
            p1.join(timeout=duration + 10.0)
            p2.join(timeout=duration + 10.0)
            with open(rp1) as f:
                a: int = json.load(f)['acquired']
            with open(rp2) as f:
                b: int = json.load(f)['acquired']
            pair_total: int = a + b

        # Pair total must not exceed solo * 1.8 — would need to be
        # ~2× if state were per-process instead of shared.
        self.assertLessEqual(
            pair_total, int(solo * 1.8),
            f'pair total {pair_total} is much larger than solo '
            f'{solo} — state is not being shared',
        )
        # And the pair total should still be within a reasonable
        # band of the refill expectation, to catch total starvation.
        expected: float = configured_refill * duration + burst
        self.assertGreaterEqual(
            pair_total, int(expected * 0.7),
        )


if __name__ == '__main__':
    unittest.main()
