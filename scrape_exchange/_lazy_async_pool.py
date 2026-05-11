'''
Generic lazy cache of long-lived async clients keyed by K.

The factory is invoked the first time a key is seen; subsequent
calls return the cached value. ``aclose_all`` calls
``aclose_attr`` on every cached value (best-effort, swallowing
per-client exceptions) and empties the cache. ``reset_for_tests``
drops cached values without calling aclose so unit tests can
isolate state without needing to drain real connections.

Used by:
- scrape_exchange.proxy_loader (pooled httpx.AsyncClient per
  proxy entry, RSS feed fetches)
- scrape_exchange.youtube.youtube_client (pooled
  AsyncYouTubeClient per proxy entry, HTML browse calls)
- scrape_exchange.youtube.youtube_channel_tabs (pooled
  innertube.InnerTube per proxy entry, InnerTube calls)
'''

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar


K = TypeVar('K')
V = TypeVar('V')


@dataclass
class _LazyAsyncPool(Generic[K, V]):
    '''Lazy cache of one client per key.'''

    factory: Callable[[K], V]
    aclose_attr: str = 'aclose'
    _cache: dict[K, V] = field(default_factory=dict)

    def get(self, key: K) -> V:
        '''Return the cached client for *key*, constructing one
        on first use.'''
        if key not in self._cache:
            self._cache[key] = self.factory(key)
        return self._cache[key]

    async def aclose_all(self) -> None:
        '''Close every cached client, swallowing per-client
        exceptions, then empty the cache. Best-effort: a failing
        aclose on one client does not block the others.'''
        cached: list[V] = list(self._cache.values())
        self._cache.clear()
        for client in cached:
            try:
                fn: Callable[[], object] = getattr(
                    client, self.aclose_attr,
                )
                result: object = fn()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass

    def reset_for_tests(self) -> None:
        '''Drop cached values without invoking aclose. For unit-
        test isolation only — do not call from production code.'''
        self._cache.clear()
