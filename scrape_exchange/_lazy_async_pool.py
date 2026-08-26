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
    _borrow_counts: dict[int, int] = field(default_factory=dict)
    _retired: dict[int, V] = field(default_factory=dict)

    def get(self, key: K) -> V:
        '''Return the cached client for *key*, constructing one
        on first use.'''
        if key not in self._cache:
            self._cache[key] = self.factory(key)
        return self._cache[key]

    def borrow(self, key: K) -> V:
        '''Return the active value and protect it from retirement close.'''
        client: V = self.get(key)
        client_id: int = id(client)
        self._borrow_counts[client_id] = (
            self._borrow_counts.get(client_id, 0) + 1
        )
        return client

    async def release(self, client: V) -> None:
        '''Release one borrower and close a retired value when idle.'''
        client_id: int = id(client)
        count: int = self._borrow_counts.get(client_id, 0)
        if count <= 1:
            self._borrow_counts.pop(client_id, None)
            retired: V | None = self._retired.pop(client_id, None)
            if retired is not None:
                await self._aclose_value(retired)
            return
        self._borrow_counts[client_id] = count - 1

    async def retire_key(self, key: K, *, expected: V) -> bool:
        '''Retire ``expected`` only if it is still active for ``key``.

        New lookups immediately construct a replacement. Closing is
        deferred until all callers that borrowed this generation have
        released it.
        '''
        active: V | None = self._cache.get(key)
        if active is not expected:
            return False
        client: V = self._cache.pop(key)
        client_id: int = id(client)
        if self._borrow_counts.get(client_id, 0) > 0:
            self._retired[client_id] = client
        else:
            await self._aclose_value(client)
        return True

    async def _aclose_value(self, client: V) -> None:
        '''Close one cached value, swallowing client errors.'''
        try:
            fn: Callable[[], object] = getattr(
                client, self.aclose_attr,
            )
            result: object = fn()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            pass

    async def aclose_key(self, key: K) -> None:
        '''Close and remove the cached client for *key*.

        Other keys remain available. Removing the entry before
        awaiting close ensures a subsequent ``get(key)`` constructs
        a replacement rather than returning the retired client.
        '''
        if key not in self._cache:
            return
        client: V = self._cache.pop(key)
        await self._aclose_value(client)

    async def aclose_all(self) -> None:
        '''Close every cached client, swallowing per-client
        exceptions, then empty the cache. Best-effort: a failing
        aclose on one client does not block the others.'''
        clients_by_id: dict[int, V] = {
            id(client): client for client in self._cache.values()
        }
        clients_by_id.update(self._retired)
        self._cache.clear()
        self._retired.clear()
        self._borrow_counts.clear()
        for client in clients_by_id.values():
            await self._aclose_value(client)

    def reset_for_tests(self) -> None:
        '''Drop cached values without invoking aclose. For unit-
        test isolation only — do not call from production code.'''
        self._cache.clear()
        self._retired.clear()
        self._borrow_counts.clear()
