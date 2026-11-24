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
    '''Lazy cache of up to ``slots`` clients per key.

    :meth:`get` always returns the first client of a key. :meth:`borrow`
    returns an idle client of the key, creates another one when every
    existing client is borrowed and the key has fewer than ``slots``
    clients, and otherwise returns the client with the fewest
    borrowers. With ``slots=1`` (the default) every key has a single
    client.
    '''

    factory: Callable[[K], V]
    aclose_attr: str = 'aclose'
    slots: int = 1
    _cache: dict[K, list[V]] = field(default_factory=dict)
    _borrow_counts: dict[int, int] = field(default_factory=dict)
    _retired: dict[int, V] = field(default_factory=dict)

    def get(self, key: K) -> V:
        '''Return the first cached client for *key*, constructing
        one on first use.'''
        clients: list[V] = self._cache.setdefault(key, [])
        if not clients:
            clients.append(self.factory(key))
        return clients[0]

    def _borrowers(self, client: V) -> int:
        '''Number of callers currently borrowing *client*.'''
        return self._borrow_counts.get(id(client), 0)

    def _pick(self, key: K) -> V:
        '''Choose the client of *key* for one borrow.'''
        clients: list[V] = self._cache.setdefault(key, [])
        for client in clients:
            if self._borrowers(client) == 0:
                return client
        if len(clients) < max(1, self.slots):
            client = self.factory(key)
            clients.append(client)
            return client
        return min(clients, key=self._borrowers)

    def borrow(self, key: K) -> V:
        '''Return a client for *key* and protect it from retirement
        close until :meth:`release`.'''
        client: V = self._pick(key)
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

        Other clients of the key stay active; new borrows construct a
        replacement when needed. Closing is deferred until all callers
        that borrowed this generation have released it.
        '''
        clients: list[V] = self._cache.get(key, [])
        index: int | None = next(
            (i for i, c in enumerate(clients) if c is expected), None,
        )
        if index is None:
            return False
        client: V = clients.pop(index)
        if not clients:
            del self._cache[key]
        if self._borrowers(client) > 0:
            self._retired[id(client)] = client
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
        '''Close and remove the cached clients for *key*.

        Other keys remain available. Removing the entry before
        awaiting close ensures a subsequent ``get(key)`` constructs
        a replacement rather than returning the retired client.
        '''
        if key not in self._cache:
            return
        clients: list[V] = self._cache.pop(key)
        for client in clients:
            await self._aclose_value(client)

    async def aclose_all(self) -> None:
        '''Close every cached client, swallowing per-client
        exceptions, then empty the cache. Best-effort: a failing
        aclose on one client does not block the others.'''
        clients_by_id: dict[int, V] = {
            id(client): client
            for clients in self._cache.values()
            for client in clients
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
