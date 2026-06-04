'''Fleet-wide RSS circuit breaker — backend-agnostic façade and
its in-process, Redis, and (stubbed) file backends.

See ``docs/superpowers/specs/2026-05-20-rss-circuit-breaker-design.md``
for the design rationale.
'''

from __future__ import annotations

import abc
import asyncio
import logging
import random
import time
from typing import ClassVar

from ._rss_circuit_state import (
    CircuitParams,
    CircuitReport,
    CircuitState,
    apply_outcome,
)
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.watchdog import Watchdog


_LOGGER: logging.Logger = logging.getLogger(__name__)

# Cap on a single open-circuit sleep. A park can legitimately last up to
# ``rss_circuit_max_open_seconds`` (default 7200s), which dwarfs the
# watchdog work timeout. Sleeping in bounded chunks and touching the
# watchdog each chunk keeps an intentional wait from looking like a hang.
_CIRCUIT_WAIT_CHUNK_SECONDS: float = 30.0


class _CircuitBackend(abc.ABC):
    '''Storage + atomic state transitions.

    Each backend implements the same state-machine rules
    (defined once in ``apply_outcome``) under its own
    atomicity primitive: asyncio.Lock for in-process, a Lua
    script for Redis, a file lock for the future file backend.
    '''

    def __init__(
        self,
        params: CircuitParams,
        wait_jitter_seconds: float = 30.0,
    ) -> None:
        self._params: CircuitParams = params
        self._wait_jitter_seconds: float = wait_jitter_seconds

    @abc.abstractmethod
    async def read_state(self) -> CircuitState:
        ...

    @abc.abstractmethod
    async def record_outcome(
        self, *, channel_id: str, was_not_found: bool,
    ) -> CircuitReport:
        ...

    async def wait_until_closed(self) -> float:
        '''Sleep until the circuit closes (or return immediately).

        Returns the number of seconds the call blocked.

        Every open-state sleep gains random jitter. This spreads
        worker wake-ups after a cooldown while still allowing the
        first worker that re-reads unchanged stale-open state to
        return and probe.
        '''
        slept: float = 0.0
        while True:
            state: CircuitState = await self.read_state()
            if not state.is_open:
                return slept
            wait: float = max(
                state.open_until_ts - time.time(), 0.0,
            )
            if wait <= 0 and slept > 0:
                return slept
            jitter: float = random.uniform(
                0.0, self._wait_jitter_seconds,
            )
            total_sleep: float = wait + jitter
            if total_sleep <= 0:
                return slept
            # Sleep in bounded chunks so a long open-circuit park does
            # not look like a hang: touch the watchdog after each chunk.
            chunk: float = min(
                total_sleep, _CIRCUIT_WAIT_CHUNK_SECONDS,
            )
            await asyncio.sleep(chunk)
            slept += chunk
            Watchdog.get().touch_work()


class _InProcessCircuitBackend(_CircuitBackend):
    '''asyncio.Lock + in-memory dict.

    Used in unit tests and when neither Redis nor a state dir
    is configured.
    '''

    def __init__(
        self,
        params: CircuitParams,
        wait_jitter_seconds: float = 30.0,
    ) -> None:
        super().__init__(params, wait_jitter_seconds)
        self._lock: asyncio.Lock = asyncio.Lock()
        self._state: CircuitState = CircuitState(
            mode='regular',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=params.initial_open_seconds,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        self._window: list[tuple[str, bool]] = []

    async def read_state(self) -> CircuitState:
        async with self._lock:
            return CircuitState(
                mode=self._state.mode,
                is_open=self._state.is_open,
                open_until_ts=self._state.open_until_ts,
                current_cooldown_s=self._state.current_cooldown_s,
                consecutive_404s=self._state.consecutive_404s,
                consecutive_successes=(
                    self._state.consecutive_successes
                ),
            )

    async def record_outcome(
        self, *, channel_id: str, was_not_found: bool,
    ) -> CircuitReport:
        async with self._lock:
            new_state, new_window, report = apply_outcome(
                self._state, self._window,
                channel_id=channel_id,
                was_not_found=was_not_found,
                now=time.time(),
                params=self._params,
            )
            self._state = new_state
            self._window = new_window
            return report


class RssCircuitBreaker:
    '''Fleet-wide RSS circuit breaker. Backend-agnostic façade.'''

    _singletons: ClassVar[
        dict[tuple, 'RssCircuitBreaker']
    ] = {}

    def __init__(self, backend: _CircuitBackend) -> None:
        self._backend: _CircuitBackend = backend

    @classmethod
    def get(
        cls,
        *,
        redis_dsn: str | None,
        state_dir: str | None,
        fail_threshold: int,
        window_size: int,
        initial_open_seconds: int,
        max_open_seconds: int,
        impaired_reopen_threshold: int,
        recovery_threshold: int,
        wait_jitter_seconds: float = 30.0,
    ) -> 'RssCircuitBreaker':
        '''Return a process-wide singleton keyed on the storage
        target plus all state-machine and wait tunables. The
        breaker picks a backend in the same order as
        ``RateLimiter``: redis_dsn → state_dir → in-process.
        '''
        key: tuple = (
            redis_dsn,
            state_dir,
            fail_threshold,
            window_size,
            initial_open_seconds,
            max_open_seconds,
            impaired_reopen_threshold,
            recovery_threshold,
            wait_jitter_seconds,
        )
        existing: RssCircuitBreaker | None = cls._singletons.get(
            key,
        )
        if existing is not None:
            return existing
        params: CircuitParams = CircuitParams(
            fail_threshold=fail_threshold,
            window_size=window_size,
            initial_open_seconds=initial_open_seconds,
            max_open_seconds=max_open_seconds,
            impaired_reopen_threshold=impaired_reopen_threshold,
            recovery_threshold=recovery_threshold,
        )
        backend: _CircuitBackend
        if redis_dsn:
            # Imported lazily so unit tests of the in-process
            # path don't drag in the Redis client.
            from . import rss_circuit_breaker as _self
            backend = _self._RedisCircuitBackend(
                params, redis_dsn,
                wait_jitter_seconds=wait_jitter_seconds,
            )
        elif state_dir:
            backend = _FileCircuitBackend(
                params, state_dir,
                wait_jitter_seconds=wait_jitter_seconds,
            )
        else:
            backend = _InProcessCircuitBackend(
                params, wait_jitter_seconds=wait_jitter_seconds,
            )
        breaker: RssCircuitBreaker = cls(backend)
        cls._singletons[key] = breaker
        return breaker

    async def acquire(self) -> float:
        '''Block until the circuit is closed.

        Returns seconds slept (for metrics).
        '''
        return await self._backend.wait_until_closed()

    async def report(
        self,
        *,
        channel_id: str,
        has_had_feed: bool,
        was_not_found: bool,
    ) -> CircuitReport:
        '''Apply one outcome to the breaker.

        Only previously-scraped channels (``has_had_feed=True``)
        contribute to the state machine; never-scraped channels
        return an empty report and do not enter the window.
        '''
        if not has_had_feed:
            return CircuitReport(
                transition=None,
                suppress_channel_failure=False,
                rollback_channel_ids=[],
                state_after=await self._backend.read_state(),
            )
        return await self._backend.record_outcome(
            channel_id=channel_id,
            was_not_found=was_not_found,
        )


_LUA_SCRIPT: str = '''
redis.replicate_commands()

local key       = KEYS[1]
local cid       = ARGV[1]
local was_404   = tonumber(ARGV[2])  -- 0 or 1
local f_thresh  = tonumber(ARGV[3])
local t_size    = tonumber(ARGV[4])
local s_init    = tonumber(ARGV[5])
local s_max     = tonumber(ARGV[6])
local c_thresh  = tonumber(ARGV[7])
local n_thresh  = tonumber(ARGV[8])

-- Use Redis server clock so all hosts share one time reference.
local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1000000

-- Load state.
local mode       = redis.call('HGET', key, 'mode')          or 'regular'
local is_open    = redis.call('HGET', key, 'is_open')       or '0'
local open_until = tonumber(
    redis.call('HGET', key, 'open_until_ts') or '0'
)
local cooldown   = tonumber(
    redis.call('HGET', key, 'current_cooldown') or tostring(s_init)
)
local cons404    = tonumber(
    redis.call('HGET', key, 'cons_404') or '0'
)
local conssucc   = tonumber(
    redis.call('HGET', key, 'cons_succ') or '0'
)
local window     = redis.call('LRANGE', key .. ':window', 0, -1)

local function clamp(v, hi)
    if v > hi then return hi else return v end
end
local function double_cap(v)
    return clamp(v * 2, s_max)
end
local function label(is_open_str, mode_str)
    if is_open_str == '1' then return 'open-' .. mode_str end
    return 'closed-' .. mode_str
end

local before_label        = label(is_open, mode)
local rollback            = {}
local transition_from     = ''
local transition_to       = ''
local transition_cooldown = 0
local suppress            = 0

-- Time-driven open->closed.
if is_open == '1' and now >= open_until then
    cooldown   = double_cap(cooldown)
    is_open    = '0'
    mode       = 'impaired'
    open_until = 0
    cons404    = 0
    conssucc   = 0
    redis.call('DEL', key .. ':window')
    window           = {}
    transition_from  = before_label
    transition_to    = 'closed-impaired'
    suppress         = 1
else
    if is_open == '1' then
        suppress = 1
    else
        -- Append outcome; trim to T.
        local entry = cjson.encode({cid = cid, f = was_404})
        redis.call('RPUSH', key .. ':window', entry)
        redis.call('LTRIM', key .. ':window', -t_size, -1)
        window = redis.call('LRANGE', key .. ':window', 0, -1)

        if mode == 'regular' then
            local n404 = 0
            for _, raw in ipairs(window) do
                local rec = cjson.decode(raw)
                if rec.f == 1 then n404 = n404 + 1 end
            end
            if n404 >= f_thresh then
                -- TRIP: roll back all 404s except the trigger.
                for i = 1, (#window - 1) do
                    local rec = cjson.decode(window[i])
                    if rec.f == 1 then
                        table.insert(rollback, rec.cid)
                    end
                end
                is_open    = '1'
                open_until = now + cooldown
                cons404    = 0
                conssucc   = 0
                redis.call('DEL', key .. ':window')
                transition_from     = before_label
                transition_to       = 'open-regular'
                transition_cooldown = cooldown
                suppress            = 1
            else
                suppress = 0
            end
        else  -- mode == 'impaired'
            if was_404 == 1 then
                cons404  = cons404 + 1
                conssucc = 0
            else
                cons404  = 0
                conssucc = conssucc + 1
            end
            if cons404 >= c_thresh then
                is_open    = '1'
                open_until = now + cooldown
                cons404    = 0
                conssucc   = 0
                redis.call('DEL', key .. ':window')
                transition_from     = before_label
                transition_to       = 'open-impaired'
                transition_cooldown = cooldown
                suppress            = 1
            elseif conssucc >= n_thresh then
                mode     = 'regular'
                cooldown = s_init
                cons404  = 0
                conssucc = 0
                redis.call('DEL', key .. ':window')
                transition_from = before_label
                transition_to   = 'closed-regular'
                suppress        = 0
            else
                suppress = 1
            end
        end
    end
end

-- Persist state.
redis.call('HSET', key,
    'mode',             mode,
    'is_open',          is_open,
    'open_until_ts',    tostring(open_until),
    'current_cooldown', tostring(cooldown),
    'cons_404',         tostring(cons404),
    'cons_succ',        tostring(conssucc)
)

-- Pack response.
return {
    mode, is_open, tostring(open_until), tostring(cooldown),
    tostring(cons404), tostring(conssucc),
    transition_from, transition_to, tostring(transition_cooldown),
    tostring(suppress),
    rollback,
}
'''


class _RedisCircuitBackend(_CircuitBackend):
    '''One Lua script per ``record_outcome`` call; state lives in
    one Redis hash + one list (the sliding window).

    The Lua script is loaded once per backend instance via
    ``SCRIPT LOAD`` and called via ``EVALSHA``; if the script
    is evicted from the Redis script cache, ``NoScriptError``
    triggers a reload.
    '''

    _STATE_KEY: ClassVar[str] = 'youtube:rss:circuit:state'

    def __init__(
        self,
        params: CircuitParams,
        redis_dsn: str,
        wait_jitter_seconds: float = 30.0,
    ) -> None:
        super().__init__(params, wait_jitter_seconds)
        self._client = redis_from_url(
            redis_dsn,
            component='youtube-rss-circuit-breaker',
            decode_responses=True,
        )
        self._script_sha: str | None = None

    async def _ensure_script(self) -> str:
        if self._script_sha is None:
            self._script_sha = await self._client.script_load(
                _LUA_SCRIPT,
            )
        return self._script_sha

    async def read_state(self) -> CircuitState:
        h: dict[str, str] = await self._client.hgetall(
            self._STATE_KEY,
        )
        return CircuitState(
            mode=h.get('mode', 'regular'),
            is_open=h.get('is_open', '0') == '1',
            open_until_ts=float(h.get('open_until_ts', '0')),
            current_cooldown_s=int(
                h.get(
                    'current_cooldown',
                    str(self._params.initial_open_seconds),
                ),
            ),
            consecutive_404s=int(h.get('cons_404', '0')),
            consecutive_successes=int(h.get('cons_succ', '0')),
        )

    async def record_outcome(
        self, *, channel_id: str, was_not_found: bool,
    ) -> CircuitReport:
        from redis.exceptions import NoScriptError  # lazy import
        sha: str = await self._ensure_script()
        args: list[str] = [
            channel_id,
            '1' if was_not_found else '0',
            str(self._params.fail_threshold),
            str(self._params.window_size),
            str(self._params.initial_open_seconds),
            str(self._params.max_open_seconds),
            str(self._params.impaired_reopen_threshold),
            str(self._params.recovery_threshold),
        ]
        try:
            raw = await self._client.evalsha(
                sha, 1, self._STATE_KEY, *args,
            )
        except NoScriptError:
            self._script_sha = None
            sha = await self._ensure_script()
            raw = await self._client.evalsha(
                sha, 1, self._STATE_KEY, *args,
            )
        return _parse_lua_reply(raw)


def _parse_lua_reply(raw: list) -> CircuitReport:
    '''Convert the Lua ``return`` block into a ``CircuitReport``.

    The Lua script returns a flat list:
      [0] mode, [1] is_open, [2] open_until_ts, [3] cooldown,
      [4] cons404, [5] conssucc,
      [6] transition_from, [7] transition_to,
      [8] transition_cooldown, [9] suppress,
      [10] rollback channel-id list
    '''
    from ._rss_circuit_state import CircuitTransition
    mode: str = raw[0]
    is_open: bool = raw[1] == '1'
    open_until_ts: float = float(raw[2])
    cooldown: int = int(float(raw[3]))
    cons404: int = int(raw[4])
    conssucc: int = int(raw[5])
    transition_from: str = raw[6]
    transition_to: str = raw[7]
    transition_cooldown: int = int(float(raw[8]))
    suppress: bool = raw[9] == '1'
    rollback: list[str] = list(raw[10])

    state_after: CircuitState = CircuitState(
        mode=mode,
        is_open=is_open,
        open_until_ts=open_until_ts,
        current_cooldown_s=cooldown,
        consecutive_404s=cons404,
        consecutive_successes=conssucc,
    )
    transition: CircuitTransition | None = None
    if transition_from and transition_to:
        transition = CircuitTransition(
            from_state=transition_from,
            to_state=transition_to,
            cooldown_seconds=transition_cooldown,
        )
    return CircuitReport(
        transition=transition,
        suppress_channel_failure=suppress,
        rollback_channel_ids=rollback,
        state_after=state_after,
    )


class _FileCircuitBackend(_CircuitBackend):
    '''Future. Raises ``NotImplementedError`` for every method
    so the factory branch is wired without a working impl.'''

    def __init__(
        self,
        params: CircuitParams,
        state_dir: str,
        wait_jitter_seconds: float = 30.0,
    ) -> None:
        super().__init__(params, wait_jitter_seconds)
        self._state_dir: str = state_dir

    async def read_state(self) -> CircuitState:
        raise NotImplementedError(
            'File-backed RSS circuit breaker not implemented yet',
        )

    async def record_outcome(
        self, *, channel_id: str, was_not_found: bool,
    ) -> CircuitReport:
        raise NotImplementedError(
            'File-backed RSS circuit breaker not implemented yet',
        )
