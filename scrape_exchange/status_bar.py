'''Progress status line for long-running maintenance tools.

Stats objects are dataclasses with a ``scanned`` counter; every
other field is shown as a counter.'''

import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import Any, TextIO


class StatusBar:
    '''Progress status line on stderr: bar, percentage, rate, ETA and
    the step's counters. On a terminal it redraws one line every
    *tty_interval* seconds; otherwise (e.g. redirected to a log) it
    writes a full line every *log_interval* seconds. *inplace*
    overrides the terminal detection.

    :param label: step name shown at the start of the line
    :param total: expected number of items; 0 if unknown
    :param estimated: mark the total as an estimate
    '''

    BAR_WIDTH: int = 24

    def __init__(
        self, label: str, total: int, *, estimated: bool = False,
        stream: TextIO = sys.stderr,
        clock: Callable[[], float] = time.monotonic,
        tty_interval: float = 1.0, log_interval: float = 30.0,
        inplace: bool | None = None,
    ) -> None:
        self._label: str = label
        self._total: int = total
        self._estimated: bool = estimated
        self._stream: TextIO = stream
        self._clock: Callable[[], float] = clock
        self._tty: bool = (
            stream.isatty() if inplace is None else inplace
        )
        self._interval: float = (
            tty_interval if self._tty else log_interval
        )
        self._start: float = clock()
        self._last: float | None = None

    def render(self, stats: Any) -> str:
        '''One status line for *stats* (no trailing newline).'''
        done: int = stats.scanned
        elapsed: float = max(self._clock() - self._start, 1e-9)
        rate: float = done / elapsed
        total: str = f'{self._total:,}' if self._total else '?'
        if self._estimated and self._total:
            total = f'~{total}'
        line: str = f'{self._label} '
        if self._total:
            frac: float = min(done / self._total, 1.0)
            filled: int = int(frac * self.BAR_WIDTH)
            bar: str = '#' * filled + '-' * (self.BAR_WIDTH - filled)
            line += f'[{bar}] {frac * 100:5.1f}% '
        line += f'{done:,}/{total} {rate:,.0f}/s'
        if self._total and rate > 0 and done < self._total:
            line += f' ETA {_hms((self._total - done) / rate)}'
        line += f' elapsed {_hms(elapsed)} | {_counters(stats)}'
        return line

    def update(self, stats: Any) -> None:
        '''Write the status line if the refresh interval has passed.'''
        now: float = self._clock()
        if self._last is not None and now - self._last < self._interval:
            return
        self._last = now
        self._write(stats)

    def finish(self, stats: Any) -> None:
        '''Write the final status line and end it.'''
        self._write(stats)
        if self._tty:
            self._stream.write('\n')
            self._stream.flush()

    def _write(self, stats: Any) -> None:
        line: str = self.render(stats)
        if self._tty:
            self._stream.write(f'\r\x1b[K{line}')
        else:
            self._stream.write(f'{line}\n')
        self._stream.flush()


def _hms(seconds: float) -> str:
    total: int = int(seconds)
    return f'{total // 3600}:{total // 60 % 60:02d}:{total % 60:02d}'


def _counters(stats: Any) -> str:
    fields: dict[str, int | bool] = asdict(stats)
    fields.pop('scanned', None)
    return ' '.join(f'{name}={value:,}' if not isinstance(value, bool)
                    else f'{name}={value}'
                    for name, value in fields.items())
