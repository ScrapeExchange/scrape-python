'''
Load-aware proxy selection with fleet-wide proxy health.

Selection uses "power of two choices": sample two available proxies at
random and take the one with the lower score, where

    score = recent selections by this process (decaying)
            + FAIL_SCORE_WEIGHT * failure EWMA
            + (1 - fraction of the call type's token bucket left)

Sampling keeps concurrent selectors from herding onto the same proxy
(the previous "most tokens" rule sent every task to the one proxy that
looked richest), and the score spreads load while steering away from
failing and drained proxies.

Health is tracked per proxy from reported outcomes: a transport-level
failure raises an exponentially weighted failure rate; three failures
in a row, or a failure rate above FAIL_EWMA_THRESHOLD, puts the proxy
in cooldown for 30 s, doubling per consecutive opening up to 10 min.
With Redis the state is shared by every process and scraper on every
host (a ``proxy_health:<hash>`` hash per proxy, updated atomically by a
Lua script) and each process refreshes its snapshot every few seconds;
without Redis it is per process.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import hashlib
import logging
import math
import random
import time

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from prometheus_client import Counter, Gauge

from scrape_exchange.worker_id import get_worker_id


_LOGGER: logging.Logger = logging.getLogger(__name__)

# Weight of each new outcome in the failure EWMA.
FAILURE_ALPHA: float = 0.2
# Consecutive failures that open a proxy's cooldown.
FAIL_THRESHOLD_CONSECUTIVE: int = 3
# Failure EWMA above which a failure opens the cooldown.
FAIL_EWMA_THRESHOLD: float = 0.5
BASE_COOLDOWN_SECONDS: float = 30.0
MAX_COOLDOWN_SECONDS: float = 600.0
# How often a process refreshes its view of fleet-wide health.
REFRESH_INTERVAL_SECONDS: float = 5.0
# Half-life of the per-process recent-selection load counter.
LOAD_HALF_LIFE_SECONDS: float = 10.0
FAIL_SCORE_WEIGHT: float = 4.0
# Failure EWMA below which a success is not written to Redis.
CLEAN_EWMA: float = 0.01
KEY_PREFIX: str = 'proxy_health'
KEY_TTL_SECONDS: int = 86400

METRIC_PROXY_SELECTED: Counter = Counter(
    'proxy_selected_total',
    'Proxies chosen by the rate limiter proxy selection.',
    ['platform', 'proxy', 'worker_id'],
)
METRIC_PROXY_COOLDOWNS: Counter = Counter(
    'proxy_health_cooldowns_total',
    'Times a proxy was put in cooldown after transport failures.',
    ['platform', 'proxy', 'worker_id'],
)
METRIC_PROXY_FAIL_EWMA: Gauge = Gauge(
    'proxy_health_fail_ewma',
    'Fleet-wide exponentially weighted transport failure rate '
    'of a proxy, as last seen by this process.',
    ['platform', 'proxy', 'worker_id'],
)
METRIC_PROXY_IN_COOLDOWN: Gauge = Gauge(
    'proxy_health_in_cooldown',
    '1 while a proxy is in cooldown, as last seen by this process.',
    ['platform', 'proxy', 'worker_id'],
)

# KEYS[1] = health hash. ARGV: ok, now, alpha, consecutive threshold,
# ewma threshold, base cooldown, max cooldown, key ttl.
# Mirrors apply_outcome().
_REPORT_LUA: str = '''
local key = KEYS[1]
local ok = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local alpha = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local ewma_threshold = tonumber(ARGV[5])
local base = tonumber(ARGV[6])
local max_cooldown = tonumber(ARGV[7])
local v = redis.call(
    'HMGET', key,
    'fail_ewma', 'consecutive_failures', 'open_count', 'cooldown_until'
)
local ewma = tonumber(v[1]) or 0
local consecutive = tonumber(v[2]) or 0
local opens = tonumber(v[3]) or 0
local cooldown_until = tonumber(v[4]) or 0
local opened = 0
if ok == 1 then
    ewma = ewma * (1 - alpha)
    consecutive = 0
    if ewma < ewma_threshold / 2 and now >= cooldown_until then
        opens = 0
    end
else
    ewma = ewma * (1 - alpha) + alpha
    consecutive = consecutive + 1
    if now >= cooldown_until
            and (consecutive >= threshold or ewma > ewma_threshold) then
        cooldown_until = now + math.min(base * 2 ^ opens, max_cooldown)
        opens = opens + 1
        consecutive = 0
        opened = 1
    end
end
redis.call(
    'HSET', key,
    'fail_ewma', tostring(ewma),
    'consecutive_failures', tostring(consecutive),
    'open_count', tostring(opens),
    'cooldown_until', tostring(cooldown_until)
)
redis.call('EXPIRE', key, tonumber(ARGV[8]))
return {
    tostring(ewma), tostring(consecutive), tostring(opens),
    tostring(cooldown_until), opened
}
'''


@dataclass
class ProxyHealthState:
    '''Health of one proxy.'''

    fail_ewma: float = 0.0
    consecutive_failures: int = 0
    open_count: int = 0
    cooldown_until: float = 0.0

    def is_clean(self) -> bool:
        return (
            self.fail_ewma < CLEAN_EWMA
            and self.consecutive_failures == 0
            and self.open_count == 0
        )


def apply_outcome(state: ProxyHealthState, ok: bool, now: float) -> bool:
    '''
    Update *state* with one request outcome; mirrors ``_REPORT_LUA``.

    :param state: the proxy's health, updated in place
    :param ok: True for success, False for a transport failure
    :param now: wall-clock time of the outcome
    :returns: True when this failure put the proxy in cooldown
    :raises: (none)
    '''

    if ok:
        state.fail_ewma *= 1 - FAILURE_ALPHA
        state.consecutive_failures = 0
        if (
            state.fail_ewma < FAIL_EWMA_THRESHOLD / 2
            and now >= state.cooldown_until
        ):
            state.open_count = 0
        return False
    state.fail_ewma = state.fail_ewma * (1 - FAILURE_ALPHA) + FAILURE_ALPHA
    state.consecutive_failures += 1
    if now < state.cooldown_until:
        return False
    if (
        state.consecutive_failures >= FAIL_THRESHOLD_CONSECUTIVE
        or state.fail_ewma > FAIL_EWMA_THRESHOLD
    ):
        state.cooldown_until = now + min(
            BASE_COOLDOWN_SECONDS * 2 ** state.open_count,
            MAX_COOLDOWN_SECONDS,
        )
        state.open_count += 1
        state.consecutive_failures = 0
        return True
    return False


def _health_key(proxy: str) -> str:
    # Hash the proxy URL so credentials never end up in Redis keys.
    digest: str = hashlib.sha1(proxy.encode('utf-8')).hexdigest()[:16]
    return f'{KEY_PREFIX}:{digest}'


class ProxyHealth:
    '''
    Proxy selection and health for one rate limiter.

    :param redis: async Redis client for fleet-wide health, or None
        for per-process health
    :param platform: platform label for metrics
    :param rng: random source (tests)
    '''

    def __init__(
        self,
        redis: Any | None,
        platform: str,
        rng: random.Random | None = None,
    ) -> None:
        self._redis: Any | None = redis
        self._platform: str = platform
        self._rng: random.Random = rng or random.Random()
        self._states: dict[str, ProxyHealthState] = {}
        # proxy -> (decayed selection count, time of last update)
        self._load: dict[str, tuple[float, float]] = {}
        self._last_refresh: float = 0.0

    def _state(self, proxy: str) -> ProxyHealthState:
        state: ProxyHealthState | None = self._states.get(proxy)
        if state is None:
            state = ProxyHealthState()
            self._states[proxy] = state
        return state

    def available(self, proxy: str, now: float | None = None) -> bool:
        '''True when *proxy* is not in cooldown.'''

        state: ProxyHealthState | None = self._states.get(proxy)
        if state is None:
            return True
        return (now or time.time()) >= state.cooldown_until

    def _recent_load(self, proxy: str, now: float) -> float:
        entry: tuple[float, float] | None = self._load.get(proxy)
        if entry is None:
            return 0.0
        count, updated = entry
        return count * 0.5 ** (
            max(0.0, now - updated) / LOAD_HALF_LIFE_SECONDS
        )

    def choose(
        self,
        candidates: list[str],
        token_fraction: Callable[[str], float],
        now: float | None = None,
    ) -> str:
        '''
        Pick a proxy from *candidates* (non-empty) by power of two
        choices and record the selection.

        :param candidates: proxies to choose from
        :param token_fraction: fraction (0..1) of the call type's
            token bucket left for a proxy
        :param now: wall-clock time (tests)
        :returns: the chosen proxy
        :raises: (none)
        '''

        now = now or time.time()
        available: list[str] = [
            p for p in candidates if self.available(p, now)
        ]
        chosen: str
        if not available:
            # Every candidate is cooling down: use the one that
            # recovers first rather than stalling.
            chosen = min(
                candidates,
                key=lambda p: self._state(p).cooldown_until,
            )
        elif len(available) == 1:
            chosen = available[0]
        else:
            first, second = self._rng.sample(available, 2)
            first_score: float = self._score(first, token_fraction, now)
            second_score: float = self._score(
                second, token_fraction, now,
            )
            if first_score == second_score:
                chosen = self._rng.choice((first, second))
            else:
                chosen = first if first_score < second_score else second
        self._load[chosen] = (self._recent_load(chosen, now) + 1.0, now)
        METRIC_PROXY_SELECTED.labels(
            platform=self._platform, proxy=chosen,
            worker_id=get_worker_id(),
        ).inc()
        return chosen

    def _score(
        self, proxy: str, token_fraction: Callable[[str], float],
        now: float,
    ) -> float:
        tokens: float = min(1.0, max(0.0, token_fraction(proxy)))
        return (
            self._recent_load(proxy, now)
            + FAIL_SCORE_WEIGHT * self._state(proxy).fail_ewma
            + (1.0 - tokens)
        )

    async def report(self, proxy: str | None, ok: bool) -> None:
        '''
        Record one request outcome for *proxy*.

        Report transport-level failures only (connection, tunnel,
        pool and TLS errors); HTTP status errors and read timeouts
        say nothing about the proxy. A success on a proxy with a
        clean record is not written to Redis.

        :param proxy: the proxy used; None is ignored
        :param ok: True for success, False for a transport failure
        :raises: (none)
        '''

        if not proxy:
            return
        state: ProxyHealthState = self._state(proxy)
        if ok and state.is_clean():
            return
        now: float = time.time()
        opened: bool
        if self._redis is None:
            opened = apply_outcome(state, ok, now)
        else:
            try:
                result: list[Any] = await self._redis.eval(
                    _REPORT_LUA, 1, _health_key(proxy),
                    '1' if ok else '0', str(now), str(FAILURE_ALPHA),
                    str(FAIL_THRESHOLD_CONSECUTIVE),
                    str(FAIL_EWMA_THRESHOLD),
                    str(BASE_COOLDOWN_SECONDS),
                    str(MAX_COOLDOWN_SECONDS), str(KEY_TTL_SECONDS),
                )
                self._update(proxy, result[:4])
                opened = int(result[4]) == 1
            except Exception as exc:
                _LOGGER.debug(
                    'Proxy health update in Redis failed; '
                    'tracking locally',
                    exc=exc, extra={'proxy': proxy},
                )
                opened = apply_outcome(state, ok, now)
        if opened:
            METRIC_PROXY_COOLDOWNS.labels(
                platform=self._platform, proxy=proxy,
                worker_id=get_worker_id(),
            ).inc()
            _LOGGER.warning(
                'Proxy put in cooldown after transport failures',
                extra={
                    'proxy': proxy,
                    'cooldown_seconds': round(
                        self._state(proxy).cooldown_until - now,
                    ),
                    'fail_ewma': round(self._state(proxy).fail_ewma, 3),
                },
            )
        self._publish(proxy, now)

    async def refresh_if_stale(
        self, proxies: list[str] | None, force: bool = False,
    ) -> None:
        '''Reload fleet-wide health for *proxies* from Redis when the
        snapshot is older than REFRESH_INTERVAL_SECONDS.'''

        if self._redis is None or not proxies:
            return
        now: float = time.time()
        if not force and now - self._last_refresh < REFRESH_INTERVAL_SECONDS:
            return
        self._last_refresh = now
        try:
            pipe: Any = self._redis.pipeline(transaction=False)
            proxy: str
            for proxy in proxies:
                pipe.hmget(
                    _health_key(proxy),
                    [
                        'fail_ewma', 'consecutive_failures',
                        'open_count', 'cooldown_until',
                    ],
                )
            results: list[list[Any]] = await pipe.execute()
        except Exception as exc:
            _LOGGER.debug(
                'Proxy health refresh from Redis failed',
                exc=exc,
            )
            return
        values: list[Any]
        for proxy, values in zip(proxies, results):
            if values[0] is None:
                self._states.pop(proxy, None)
            else:
                self._update(proxy, values)
            self._publish(proxy, now)

    def _update(self, proxy: str, values: list[Any]) -> None:
        def number(value: Any) -> float:
            try:
                parsed: float = float(value)
            except (TypeError, ValueError):
                return 0.0
            return parsed if math.isfinite(parsed) else 0.0

        self._states[proxy] = ProxyHealthState(
            fail_ewma=number(values[0]),
            consecutive_failures=int(number(values[1])),
            open_count=int(number(values[2])),
            cooldown_until=number(values[3]),
        )

    def _publish(self, proxy: str, now: float) -> None:
        state: ProxyHealthState = self._state(proxy)
        labels: dict[str, str] = {
            'platform': self._platform, 'proxy': proxy,
            'worker_id': get_worker_id(),
        }
        METRIC_PROXY_FAIL_EWMA.labels(**labels).set(state.fail_ewma)
        METRIC_PROXY_IN_COOLDOWN.labels(**labels).set(
            1 if now < state.cooldown_until else 0,
        )
