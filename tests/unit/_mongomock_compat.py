'''
Test helpers for running MongoVideoBacklog on mongomock.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Any


class HintlessCollection:
    '''Proxy for a mongomock collection that drops the ``hint``
    argument of ``update_many``, which mongomock does not implement.
    Every other attribute is looked up on the wrapped collection at
    call time, so tests can still patch methods on it.'''

    def __init__(self, collection: Any) -> None:
        self._collection: Any = collection
        self.hints: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._collection, name)

    async def update_many(self, *args: Any, **kwargs: Any) -> Any:
        self.hints.append(kwargs.pop('hint', None))
        return await self._collection.update_many(*args, **kwargs)
