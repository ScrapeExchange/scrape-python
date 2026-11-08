'''Opt-in tracemalloc reporting for long-running tools.

``tracemalloc`` slows a process by a few times and adds memory
overhead, so it is off unless an interval is configured. The first
snapshot, taken one interval after start, is the baseline so start-up
allocations are not reported; every later snapshot is compared to it
and the biggest growers are logged. A steady leak shows up as the same
source lines growing on every report.
'''

import asyncio
import logging
import tracemalloc

_LOGGER: logging.Logger = logging.getLogger(__name__)

TOP_LINES: int = 25
TOP_TRACEBACKS: int = 3


def _report(
    baseline: tracemalloc.Snapshot, label: str,
) -> None:
    '''Log the allocations that grew most since *baseline*.'''
    snapshot: tracemalloc.Snapshot = tracemalloc.take_snapshot()
    current: int
    peak: int
    current, peak = tracemalloc.get_traced_memory()
    _LOGGER.info(
        'Memory profile',
        extra={
            'report': label,
            'traced_bytes': current,
            'traced_peak_bytes': peak,
        },
    )
    for stat in snapshot.compare_to(baseline, 'lineno')[:TOP_LINES]:
        _LOGGER.info(
            'Memory growth by line',
            extra={
                'report': label,
                'location': str(stat.traceback),
                'size_diff_bytes': stat.size_diff,
                'count_diff': stat.count_diff,
                'size_bytes': stat.size,
            },
        )
    for stat in snapshot.compare_to(
        baseline, 'traceback',
    )[:TOP_TRACEBACKS]:
        _LOGGER.info(
            'Memory growth by traceback',
            extra={
                'report': label,
                'size_diff_bytes': stat.size_diff,
                'count_diff': stat.count_diff,
                'traceback': ' <- '.join(
                    line.strip().replace('\n', ' ')
                    for line in stat.traceback.format()
                ),
            },
        )


async def run_memory_profiler(
    interval_seconds: int, frames: int = 12,
) -> None:
    '''Trace allocations and log growth every *interval_seconds*.

    Runs until cancelled. Does nothing when *interval_seconds* is not
    positive.
    '''
    if interval_seconds <= 0:
        return
    tracemalloc.start(frames)
    _LOGGER.warning(
        'Memory profiling enabled; expect a slower process',
        extra={'interval_seconds': interval_seconds, 'frames': frames},
    )
    try:
        await asyncio.sleep(interval_seconds)
        baseline: tracemalloc.Snapshot = await asyncio.to_thread(
            tracemalloc.take_snapshot,
        )
        report_number: int = 0
        while True:
            await asyncio.sleep(interval_seconds)
            report_number += 1
            await asyncio.to_thread(
                _report, baseline, f'{report_number}',
            )
    finally:
        tracemalloc.stop()
