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
A proxy that answers with 407 Proxy Authentication Required is put in
cooldown straight away, for 5 min, doubling each time it answers 407
again after a cooldown ends, up to 6 h: a provider that revoked our
access is soon asked only every few hours, while a burst of 407s
during a network blip costs minutes. A success after the cooldown ends
resets the backoff. Such a cooldown is flagged so
:meth:`ProxyHealth.clear_auth_cooldowns` can lift it when the proxy
files are edited.
A bot check from YouTube on a proxy puts that proxy in cooldown in
this process only (:meth:`ProxyHealth.report_bot_detection`), for
2 min, doubling each time the proxy is challenged again after a
cooldown ends, up to 30 min; a success of the challenged call after
the cooldown ends resets it. YouTube challenges the IP together with this process's session
(cookies, visitor data), so other hosts can keep using the proxy.
With split proxy pools each proxy has one process per host, so this
is a per-host cooldown.
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
import os
import random
import time

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from prometheus_client import Counter, Gauge

from scrape_exchange.proxy_loader import proxy_file_label
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
# Cooldown after a 407 Proxy Authentication Required response,
# doubling per consecutive 407 cooldown up to the maximum.
AUTH_BASE_COOLDOWN_SECONDS: float = 300.0
AUTH_MAX_COOLDOWN_SECONDS: float = 6 * 3600.0
# Cooldown in this process after a YouTube bot check on a proxy,
# doubling per consecutive bot cooldown up to the maximum.
BOT_BASE_COOLDOWN_SECONDS: float = 120.0
BOT_MAX_COOLDOWN_SECONDS: float = 1800.0
# How often a process refreshes its view of fleet-wide health.
REFRESH_INTERVAL_SECONDS: float = 5.0
# Half-life of the per-process recent-selection load counter.
LOAD_HALF_LIFE_SECONDS: float = 10.0
FAIL_SCORE_WEIGHT: float = 4.0
# Failure EWMA below which a success is not written to Redis.
CLEAN_EWMA: float = 0.01
KEY_PREFIX: str = 'proxy_health'
KEY_TTL_SECONDS: int = 86400
# Set by the scraper supervisor on each child: '1' for the child that
# exports the fleet-wide health gauges, '0' for the others. With
# Redis every process sees the same health, so one exporter per host
# is enough. Unset (standalone process) means export.
PUBLISHER_ENV_VAR: str = 'PROXY_HEALTH_METRICS_PUBLISHER'

METRIC_PROXY_SELECTED: Counter = Counter(
    'proxy_selected_total',
    'Proxies chosen by the rate limiter proxy selection.',
    ['platform', 'proxy', 'worker_id'],
)
METRIC_PROXY_COOLDOWNS: Counter = Counter(
    'proxy_health_cooldowns_total',
    'Times a proxy was put in cooldown, by reason: "failures" for '
    'transport failures, "proxy_auth" for a 407 from the proxy.',
    ['platform', 'proxy', 'worker_id', 'reason'],
)
COOLDOWN_REASONS: tuple[str, ...] = (
    'failures', 'proxy_auth', 'bot_detection',
)
# The per-proxy counter above starts each series at 1, which
# increase() and rate() cannot see; this one is created at 0 for every
# proxy file and reason before the first opening, so alerts work.
METRIC_COOLDOWN_OPENINGS: Counter = Counter(
    'proxy_health_cooldown_openings_total',
    'Times a proxy was put in cooldown, by proxy file and reason: '
    '"failures" for transport failures, "proxy_auth" for a 407 '
    'from the proxy, "bot_detection" for a YouTube bot check (a '
    'cooldown in this process only).',
    ['platform', 'proxy_file', 'worker_id', 'reason'],
)
# The health gauges only carry series for proxies that have been
# unhealthy at some point in this process's life, and with Redis only
# the publisher process (see PUBLISHER_ENV_VAR) exports them.
METRIC_PROXY_FAIL_EWMA: Gauge = Gauge(
    'proxy_health_fail_ewma',
    'Fleet-wide exponentially weighted transport failure rate '
    'of a proxy, as last seen by this process. Only proxies that '
    'have been unhealthy have a series.',
    ['platform', 'proxy', 'worker_id'],
    multiprocess_mode='livemax',
)
METRIC_PROXY_IN_COOLDOWN: Gauge = Gauge(
    'proxy_health_in_cooldown',
    '1 while a proxy is in cooldown, as last seen by this process. '
    'Only proxies that have been unhealthy have a series.',
    ['platform', 'proxy', 'worker_id'],
    multiprocess_mode='livemax',
)

# KEYS[1] = health hash. ARGV: ok, now, alpha, consecutive threshold,
# ewma threshold, base cooldown, max cooldown, key ttl, auth failure,
# auth base cooldown, auth max cooldown. Mirrors apply_outcome().
_REPORT_LUA: str = '''
local key = KEYS[1]
local ok = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local alpha = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local ewma_threshold = tonumber(ARGV[5])
local base = tonumber(ARGV[6])
local max_cooldown = tonumber(ARGV[7])
local auth = tonumber(ARGV[9]) or 0
local auth_base = tonumber(ARGV[10]) or 0
local auth_max = tonumber(ARGV[11]) or 0
local v = redis.call(
    'HMGET', key,
    'fail_ewma', 'consecutive_failures', 'open_count', 'cooldown_until',
    'auth_failed', 'auth_open_count'
)
local ewma = tonumber(v[1]) or 0
local consecutive = tonumber(v[2]) or 0
local opens = tonumber(v[3]) or 0
local cooldown_until = tonumber(v[4]) or 0
local auth_failed = tonumber(v[5]) or 0
local auth_opens = tonumber(v[6]) or 0
local opened = 0
if ok == 1 then
    ewma = ewma * (1 - alpha)
    consecutive = 0
    if now >= cooldown_until then
        auth_failed = 0
        auth_opens = 0
        if ewma < ewma_threshold / 2 then
            opens = 0
        end
    end
elseif auth == 1 then
    ewma = ewma * (1 - alpha) + alpha
    consecutive = 0
    if auth_failed ~= 1 or now >= cooldown_until then
        opened = 1
        cooldown_until = math.max(
            cooldown_until,
            now + math.min(auth_base * 2 ^ auth_opens, auth_max)
        )
        auth_opens = auth_opens + 1
    end
    auth_failed = 1
else
    ewma = ewma * (1 - alpha) + alpha
    consecutive = consecutive + 1
    if now >= cooldown_until
            and (consecutive >= threshold or ewma > ewma_threshold) then
        cooldown_until = now + math.min(base * 2 ^ opens, max_cooldown)
        opens = opens + 1
        consecutive = 0
        auth_failed = 0
        opened = 1
    end
end
redis.call(
    'HSET', key,
    'fail_ewma', tostring(ewma),
    'consecutive_failures', tostring(consecutive),
    'open_count', tostring(opens),
    'cooldown_until', tostring(cooldown_until),
    'auth_failed', tostring(auth_failed),
    'auth_open_count', tostring(auth_opens)
)
redis.call('EXPIRE', key, tonumber(ARGV[8]))
return {
    tostring(ewma), tostring(consecutive), tostring(opens),
    tostring(cooldown_until), tostring(auth_failed),
    tostring(auth_opens), opened
}
'''

# KEYS[1] = health hash. Lifts a cooldown that a 407 opened and resets
# the failure record; any other cooldown is left alone. Returns 1 when
# a cooldown was lifted.
_CLEAR_AUTH_LUA: str = '''
local key = KEYS[1]
if tonumber(redis.call('HGET', key, 'auth_failed')) ~= 1 then
    return 0
end
redis.call(
    'HSET', key,
    'fail_ewma', '0', 'consecutive_failures', '0', 'open_count', '0',
    'cooldown_until', '0', 'auth_failed', '0', 'auth_open_count', '0'
)
return 1
'''

_HEALTH_FIELDS: list[str] = [
    'fail_ewma', 'consecutive_failures', 'open_count', 'cooldown_until',
    'auth_failed', 'auth_open_count',
]


def is_proxy_auth_error(exc: BaseException | None) -> bool:
    '''
    True when *exc*, or an exception it was raised from, is the proxy
    answering 407 Proxy Authentication Required.

    httpx raises ``ProxyError('407 Proxy Authentication Required')``;
    curl reports a failed CONNECT tunnel with the status in the text.

    :param exc: the exception a request through a proxy raised
    :returns: whether it is a proxy authentication failure
    :raises: (none)
    '''

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        message: str = str(current)
        if '407' in message and (
            type(current).__name__ == 'ProxyError'
            or 'Proxy Authentication' in message
            or 'CONNECT tunnel' in message
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


@dataclass
class ProxyHealthState:
    '''Health of one proxy.'''

    fail_ewma: float = 0.0
    consecutive_failures: int = 0
    open_count: int = 0
    cooldown_until: float = 0.0
    # True while the cooldown was opened by a 407 from the proxy.
    auth_failed: bool = False
    # 407 cooldowns opened in a row; sets the next one's length.
    auth_open_count: int = 0

    def is_clean(self) -> bool:
        return (
            self.fail_ewma < CLEAN_EWMA
            and self.consecutive_failures == 0
            and self.open_count == 0
            and not self.auth_failed
            and self.auth_open_count == 0
        )


def apply_outcome(
    state: ProxyHealthState, ok: bool, now: float,
    auth_failure: bool = False,
) -> bool:
    '''
    Update *state* with one request outcome; mirrors ``_REPORT_LUA``.

    :param state: the proxy's health, updated in place
    :param ok: True for success, False for a transport failure
    :param now: wall-clock time of the outcome
    :param auth_failure: the failure was a 407 from the proxy, which
        opens a cooldown straight away, AUTH_BASE_COOLDOWN_SECONDS
        doubling per consecutive 407 cooldown up to
        AUTH_MAX_COOLDOWN_SECONDS
    :returns: True when this failure put the proxy in cooldown
    :raises: (none)
    '''

    if ok:
        state.fail_ewma *= 1 - FAILURE_ALPHA
        state.consecutive_failures = 0
        if now >= state.cooldown_until:
            state.auth_failed = False
            state.auth_open_count = 0
            if state.fail_ewma < FAIL_EWMA_THRESHOLD / 2:
                state.open_count = 0
        return False
    state.fail_ewma = state.fail_ewma * (1 - FAILURE_ALPHA) + FAILURE_ALPHA
    if auth_failure:
        state.consecutive_failures = 0
        opened: bool = (
            not state.auth_failed or now >= state.cooldown_until
        )
        if opened:
            state.cooldown_until = max(
                state.cooldown_until,
                now + min(
                    AUTH_BASE_COOLDOWN_SECONDS
                    * 2 ** state.auth_open_count,
                    AUTH_MAX_COOLDOWN_SECONDS,
                ),
            )
            state.auth_open_count += 1
        state.auth_failed = True
        return opened
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
        state.auth_failed = False
        return True
    return False


@dataclass
class BotCooldown:
    '''This process's cooldown of one proxy after YouTube bot
    checks.'''

    until: float = 0.0
    # Bot cooldowns opened in a row; sets the next one's length.
    open_count: int = 0
    # The call YouTube challenged; only its success resets the
    # backoff.
    api: str = ''


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

    The health gauges are exported by every process when health is
    per process (no Redis), and otherwise only when
    PUBLISHER_ENV_VAR is unset or '1'.
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
        self._export_gauges: bool = (
            redis is None
            or os.environ.get(PUBLISHER_ENV_VAR, '1') != '0'
        )
        # Proxies that have a health gauge series. A series is
        # created when a proxy is first seen unhealthy; it cannot be
        # removed again in multiprocess mode, so it then stays.
        self._exported: set[str] = set()
        # Proxies in this process's bot-check cooldown, or whose
        # backoff has not been reset by a success yet.
        self._bot: dict[str, BotCooldown] = {}
        # Proxy files whose cooldown openings series exist.
        self._opening_files: set[str] = set()
        self._opening_list: list[str] | None = None

    def _state(self, proxy: str) -> ProxyHealthState:
        state: ProxyHealthState | None = self._states.get(proxy)
        if state is None:
            state = ProxyHealthState()
            self._states[proxy] = state
        return state

    def available(self, proxy: str, now: float | None = None) -> bool:
        '''True when *proxy* is not in cooldown, fleet-wide or
        in this process after a bot check.'''

        return (now or time.time()) >= self._cooldown_until(proxy)

    def _cooldown_until(self, proxy: str) -> float:
        '''When *proxy*'s fleet-wide and bot cooldowns end.'''

        state: ProxyHealthState | None = self._states.get(proxy)
        bot: BotCooldown | None = self._bot.get(proxy)
        return max(
            state.cooldown_until if state is not None else 0.0,
            bot.until if bot is not None else 0.0,
        )

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
            chosen = min(candidates, key=self._cooldown_until)
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

    async def report(
        self, proxy: str | None, ok: bool, auth_failure: bool = False,
    ) -> None:
        '''
        Record one request outcome for *proxy*.

        Report transport-level failures only (connection, tunnel,
        pool and TLS errors); HTTP status errors and read timeouts
        say nothing about the proxy. A success on a proxy with a
        clean record is not written to Redis.

        :param proxy: the proxy used; None is ignored
        :param ok: True for success, False for a transport failure
        :param auth_failure: the failure was a 407 from the proxy;
            puts the proxy in a 407 cooldown (see the module docs)
        :raises: (none)
        '''

        if not proxy:
            return
        self._init_opening_series([proxy])
        auth_failure = auth_failure and not ok
        now: float = time.time()
        state: ProxyHealthState = self._state(proxy)
        if ok and state.is_clean():
            return
        opened: bool
        if self._redis is None:
            opened = apply_outcome(state, ok, now, auth_failure)
        else:
            try:
                result: list[Any] = await self._redis.eval(
                    _REPORT_LUA, 1, _health_key(proxy),
                    '1' if ok else '0', str(now), str(FAILURE_ALPHA),
                    str(FAIL_THRESHOLD_CONSECUTIVE),
                    str(FAIL_EWMA_THRESHOLD),
                    str(BASE_COOLDOWN_SECONDS),
                    str(MAX_COOLDOWN_SECONDS), str(KEY_TTL_SECONDS),
                    '1' if auth_failure else '0',
                    str(AUTH_BASE_COOLDOWN_SECONDS),
                    str(AUTH_MAX_COOLDOWN_SECONDS),
                )
                self._update(proxy, result[:6])
                opened = int(result[6]) == 1
            except Exception as exc:
                _LOGGER.debug(
                    'Proxy health update in Redis failed; '
                    'tracking locally',
                    exc=exc, extra={'proxy': proxy},
                )
                opened = apply_outcome(state, ok, now, auth_failure)
        if opened:
            reason: str = 'proxy_auth' if auth_failure else 'failures'
            METRIC_PROXY_COOLDOWNS.labels(
                platform=self._platform, proxy=proxy,
                worker_id=get_worker_id(), reason=reason,
            ).inc()
            METRIC_COOLDOWN_OPENINGS.labels(
                platform=self._platform,
                proxy_file=proxy_file_label(proxy),
                worker_id=get_worker_id(), reason=reason,
            ).inc()
            message: str = (
                'Proxy put in cooldown after 407 Proxy '
                'Authentication Required'
                if auth_failure else
                'Proxy put in cooldown after transport failures'
            )
            _LOGGER.warning(
                message,
                extra={
                    'proxy': proxy,
                    'reason': reason,
                    'cooldown_seconds': round(
                        self._state(proxy).cooldown_until - now,
                    ),
                    'fail_ewma': round(self._state(proxy).fail_ewma, 3),
                },
            )
        self._publish(proxy, now)

    def report_bot_detection(self, proxy: str | None, api: str) -> bool:
        '''
        Put *proxy* in this process's bot-check cooldown: 2 min,
        doubling per bot check right after a cooldown ends, up to
        30 min. Checks while the cooldown is open, from requests
        already in flight, do not extend it. Other processes and
        hosts are not affected.

        :param proxy: the proxy YouTube challenged; None is ignored
        :param api: the call that was challenged, e.g. 'player';
            only :meth:`report_api_success` for this call resets the
            backoff
        :returns: True when this check opened a cooldown
        :raises: (none)
        '''

        if not proxy:
            return False
        self._init_opening_series([proxy])
        now: float = time.time()
        bot: BotCooldown = self._bot.setdefault(proxy, BotCooldown())
        if now < bot.until:
            return False
        seconds: float = min(
            BOT_BASE_COOLDOWN_SECONDS * 2 ** bot.open_count,
            BOT_MAX_COOLDOWN_SECONDS,
        )
        bot.until = now + seconds
        bot.open_count += 1
        bot.api = api
        METRIC_COOLDOWN_OPENINGS.labels(
            platform=self._platform,
            proxy_file=proxy_file_label(proxy),
            worker_id=get_worker_id(), reason='bot_detection',
        ).inc()
        _LOGGER.warning(
            'Proxy put in cooldown on this host after bot detection',
            extra={
                'proxy': proxy,
                'reason': 'bot_detection',
                'cooldown_seconds': round(seconds),
                'open_count': bot.open_count,
                'api': api,
            },
        )
        return True

    def report_api_success(self, proxy: str | None, api: str) -> None:
        '''
        Record a successful *api* call through *proxy*. After the
        proxy's bot-check cooldown has ended, a success of the call
        that was challenged resets the backoff; successes of other
        calls, or during the cooldown, do not.

        :param proxy: the proxy the call went through
        :param api: the call that succeeded, e.g. 'player'
        :raises: (none)
        '''

        if not proxy:
            return
        bot: BotCooldown | None = self._bot.get(proxy)
        if bot is None or bot.api != api:
            return
        if time.time() >= bot.until:
            del self._bot[proxy]

    async def clear_auth_cooldowns(self, proxies: list[str]) -> int:
        '''
        Lift the cooldowns that a 407 opened for *proxies*, e.g. after
        the proxy files were edited; other cooldowns are kept. With
        Redis this clears them for every process on every host.

        :param proxies: the proxies to clear
        :returns: the number of cooldowns lifted
        :raises: (none)
        '''

        cleared: list[str] = []
        proxy: str
        if self._redis is None:
            for proxy in proxies:
                state: ProxyHealthState | None = self._states.get(proxy)
                if state is not None and state.auth_failed:
                    self._states[proxy] = ProxyHealthState()
                    cleared.append(proxy)
        else:
            for proxy in proxies:
                try:
                    lifted: Any = await self._redis.eval(
                        _CLEAR_AUTH_LUA, 1, _health_key(proxy),
                    )
                except Exception as exc:
                    _LOGGER.warning(
                        'Clearing proxy authentication cooldown '
                        'in Redis failed',
                        exc=exc, extra={'proxy': proxy},
                    )
                    continue
                if int(lifted) == 1:
                    self._states[proxy] = ProxyHealthState()
                    cleared.append(proxy)
        now: float = time.time()
        for proxy in cleared:
            self._publish(proxy, now)
        if cleared:
            _LOGGER.info(
                'Cleared proxy authentication cooldowns',
                extra={'proxies': cleared, 'count': len(cleared)},
            )
        return len(cleared)

    async def refresh_if_stale(
        self, proxies: list[str] | None, force: bool = False,
    ) -> None:
        '''Reload fleet-wide health for *proxies* from Redis when the
        snapshot is older than REFRESH_INTERVAL_SECONDS.'''

        if not proxies:
            return
        if proxies is not self._opening_list:
            # acquire() passes the same list each time; walk it again
            # only when the pool is replaced.
            self._opening_list = proxies
            self._init_opening_series(proxies)
        if self._redis is None:
            return
        now: float = time.time()
        if not force and now - self._last_refresh < REFRESH_INTERVAL_SECONDS:
            return
        self._last_refresh = now
        try:
            pipe: Any = self._redis.pipeline(transaction=False)
            proxy: str
            for proxy in proxies:
                pipe.hmget(_health_key(proxy), _HEALTH_FIELDS)
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
            auth_failed=(
                len(values) > 4 and int(number(values[4])) == 1
            ),
            auth_open_count=(
                int(number(values[5])) if len(values) > 5 else 0
            ),
        )

    def _init_opening_series(self, proxies: list[str]) -> None:
        '''Create the cooldown openings series at 0 for the proxy
        files of *proxies* not seen before, so the first opening is
        an increase Prometheus can see.'''

        proxy: str
        for proxy in proxies:
            label: str = proxy_file_label(proxy)
            if label in self._opening_files:
                continue
            self._opening_files.add(label)
            reason: str
            for reason in COOLDOWN_REASONS:
                METRIC_COOLDOWN_OPENINGS.labels(
                    platform=self._platform, proxy_file=label,
                    worker_id=get_worker_id(), reason=reason,
                )

    def _publish(self, proxy: str, now: float) -> None:
        if not self._export_gauges:
            return
        state: ProxyHealthState = self._state(proxy)
        in_cooldown: bool = now < state.cooldown_until
        if proxy not in self._exported:
            if state.is_clean() and not in_cooldown:
                return
            self._exported.add(proxy)
        labels: dict[str, str] = {
            'platform': self._platform, 'proxy': proxy,
            'worker_id': get_worker_id(),
        }
        METRIC_PROXY_FAIL_EWMA.labels(**labels).set(state.fail_ewma)
        METRIC_PROXY_IN_COOLDOWN.labels(**labels).set(
            1 if in_cooldown else 0,
        )


async def clear_auth_cooldowns_in_redis(
    redis_dsn: str, proxies: list[str], platform: str,
) -> int:
    '''
    Lift the fleet-wide cooldowns that a 407 Proxy Authentication
    Required opened for *proxies*, using a short-lived Redis client.
    For processes without a rate limiter, such as the scraper
    supervisor.

    :param redis_dsn: Redis holding the proxy health
    :param proxies: the proxies to clear
    :param platform: platform label for metrics
    :returns: the number of cooldowns lifted
    :raises: (none)
    '''

    from scrape_exchange.redis_client import redis_from_url

    redis: Any = redis_from_url(
        redis_dsn,
        component=f'{platform}-proxy-health',
        connect_retry=False,
        decode_responses=True,
        socket_connect_timeout=5.0,
        socket_timeout=10.0,
    )
    try:
        return await ProxyHealth(redis, platform).clear_auth_cooldowns(
            proxies,
        )
    finally:
        await redis.aclose()
