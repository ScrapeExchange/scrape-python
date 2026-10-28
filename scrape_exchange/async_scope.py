'''Run one coroutine as a program, closing loop-bound clients first.

``asyncio.run()`` closes its loop as soon as the coroutine returns, and
anything that built a connection pool on that loop afterwards cannot be
awaited again. Closing the pools *inside* the coroutine — before the
loop goes away — is the only way to release their sockets, so entry
points use :func:`run_async` instead of calling ``asyncio.run()``
directly.

See :func:`scrape_exchange.video_queue_factory.close_backlog_clients`
for the sockets this exists to reclaim.
'''

import asyncio
from collections.abc import Coroutine
from typing import Any

from scrape_exchange.video_queue_factory import close_backlog_clients


def run_async[T](main: Coroutine[Any, Any, T]) -> T:
    '''``asyncio.run(main)`` that closes loop-bound clients first.

    :param main: the program's entry coroutine
    :returns: whatever *main* returns
    '''

    async def _wrapped() -> T:
        try:
            return await main
        finally:
            await close_backlog_clients()

    return asyncio.run(_wrapped())
