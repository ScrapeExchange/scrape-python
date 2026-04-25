'''
Unit tests for the fire-and-forget background upload queue added to
:class:`scrape_exchange.exchange_client.ExchangeClient`.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import unittest

import httpx

from scrape_exchange.exchange_client import ExchangeClient


class _FakeFileManager:
    '''
    Minimal stand-in for ``AssetFileManagement`` that records
    successful ``mark_uploaded`` calls so tests can assert the file
    move happened without touching the filesystem.
    '''

    def __init__(self) -> None:
        self.marked: list[str] = []
        self.raise_on_mark: bool = False

    async def mark_uploaded(self, filename: str) -> None:
        if self.raise_on_mark:
            raise OSError('simulated mark_uploaded failure')
        self.marked.append(filename)


def _make_client(
    handler, *, upload_workers: int = 2, queue_max: int = 16,
) -> ExchangeClient:
    '''
    Build an ExchangeClient wired to a fake httpx transport. The
    client intentionally bypasses normal ``__init__`` socket setup by
    installing a ``MockTransport`` via ``_transport`` on the inner
    httpx client so post() routes into *handler*.
    '''

    client: ExchangeClient = ExchangeClient.__new__(ExchangeClient)
    client.exchange_url = 'https://fake.scrape.exchange'
    client.jwt_header = None
    client.headers = {}
    client._upload_queue = None
    client._upload_tasks = []
    client._upload_shutdown = False
    httpx.AsyncClient.__init__(
        client,
        transport=httpx.MockTransport(handler),
        trust_env=False,
    )
    client.UPLOAD_WORKER_COUNT = upload_workers
    client.UPLOAD_QUEUE_MAX = queue_max
    return client


class EnqueueUploadTests(unittest.IsolatedAsyncioTestCase):

    async def test_successful_upload_marks_file(self) -> None:
        seen_requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_requests.append(request)
            return httpx.Response(201, json={'ok': True})

        client: ExchangeClient = _make_client(handler)
        try:
            fm: _FakeFileManager = _FakeFileManager()
            enqueued: bool = client.enqueue_upload(
                'https://fake.scrape.exchange/api/v1/data',
                json={'entity': 'video', 'id': 'abc'},
                file_manager=fm,
                filename='video-dlp-abc.json.br',
                log_extra={'video_id': 'abc'},
            )
            self.assertTrue(enqueued)

            await client.drain_uploads(timeout=2.0)

            self.assertEqual(len(seen_requests), 1)
            self.assertEqual(fm.marked, ['video-dlp-abc.json.br'])
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_failed_upload_leaves_file_alone(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text='boom')

        client: ExchangeClient = _make_client(handler)
        try:
            fm: _FakeFileManager = _FakeFileManager()
            client.enqueue_upload(
                'https://fake.scrape.exchange/api/v1/data',
                json={'entity': 'video'},
                file_manager=fm,
                filename='video-dlp-xyz.json.br',
            )
            with self.assertLogs(
                'scrape_exchange.exchange_client', level='ERROR',
            ):
                await client.drain_uploads(timeout=5.0)

            self.assertEqual(fm.marked, [])
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_enqueue_without_file_manager_is_fire_and_forget(
        self,
    ) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(201, json={'ok': True})

        client: ExchangeClient = _make_client(handler)
        try:
            client.enqueue_upload(
                'https://fake.scrape.exchange/api/v1/data',
                json={'entity': 'video'},
            )
            await client.drain_uploads(timeout=2.0)
            self.assertEqual(len(seen), 1)
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_enqueue_returns_immediately(self) -> None:
        '''
        The caller must not block on the HTTP round-trip. Use a
        handler that stalls until released and assert that
        ``enqueue_upload`` returns before the handler completes.
        '''

        release: asyncio.Event = asyncio.Event()

        async def slow_handler(
            request: httpx.Request,
        ) -> httpx.Response:
            await release.wait()
            return httpx.Response(201)

        client: ExchangeClient = _make_client(slow_handler)
        try:
            fm: _FakeFileManager = _FakeFileManager()
            # This must return without waiting for release.
            enqueued: bool = client.enqueue_upload(
                'https://fake.scrape.exchange/api/v1/data',
                json={'entity': 'video'},
                file_manager=fm,
                filename='video-dlp-abc.json.br',
            )
            self.assertTrue(enqueued)
            self.assertEqual(fm.marked, [])  # not yet

            release.set()
            await client.drain_uploads(timeout=2.0)
            self.assertEqual(fm.marked, ['video-dlp-abc.json.br'])
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_queue_full_drops_silently(self) -> None:
        release: asyncio.Event = asyncio.Event()

        async def slow_handler(
            request: httpx.Request,
        ) -> httpx.Response:
            await release.wait()
            return httpx.Response(201)

        # One worker, tiny queue: 1 in-flight + 2 queued = 3 slots.
        client: ExchangeClient = _make_client(
            slow_handler, upload_workers=1, queue_max=2
        )
        try:
            # First enqueue starts the worker; it takes that job
            # immediately (so the queue has room). Enqueue two more
            # to fill the queue, then a fourth to overflow.
            self.assertTrue(
                client.enqueue_upload(
                    'https://fake/api', json={}, log_extra={'n': 1}
                )
            )
            # Let the worker pick up job 1 so the queue is empty
            # before we fill it.
            await asyncio.sleep(0)
            self.assertTrue(
                client.enqueue_upload(
                    'https://fake/api', json={}, log_extra={'n': 2}
                )
            )
            self.assertTrue(
                client.enqueue_upload(
                    'https://fake/api', json={}, log_extra={'n': 3}
                )
            )
            # Queue is now at maxsize=2; this one must be dropped
            # and a WARNING logged to flag the silent drop.
            with self.assertLogs(
                'scrape_exchange.exchange_client', level='WARNING',
            ):
                dropped: bool = client.enqueue_upload(
                    'https://fake/api', json={}, log_extra={'n': 4}
                )
            self.assertFalse(dropped)

            release.set()
            await client.drain_uploads(timeout=2.0)
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_drain_timeout_cancels_stuck_workers(self) -> None:
        '''
        If the API never responds, ``drain_uploads`` must return
        within *timeout* rather than hang.
        '''

        never: asyncio.Event = asyncio.Event()

        async def blocking_handler(
            request: httpx.Request,
        ) -> httpx.Response:
            await never.wait()
            return httpx.Response(201)

        client: ExchangeClient = _make_client(blocking_handler)
        try:
            client.enqueue_upload(
                'https://fake/api', json={},
                file_manager=_FakeFileManager(),
                filename='foo.json.br',
            )
            start: float = asyncio.get_event_loop().time()
            # drain_uploads logs a WARNING when it cancels stuck
            # workers to surface the timeout in production.
            with self.assertLogs(
                'scrape_exchange.exchange_client', level='WARNING',
            ):
                await client.drain_uploads(timeout=0.2)
            elapsed: float = asyncio.get_event_loop().time() - start
            self.assertLess(elapsed, 1.0)
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_enqueue_after_shutdown_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201)

        client: ExchangeClient = _make_client(handler)
        try:
            await client.drain_uploads(timeout=0.5)
            accepted: bool = client.enqueue_upload(
                'https://fake/api',
                json={},
                file_manager=_FakeFileManager(),
                filename='foo.json.br',
            )
            self.assertFalse(accepted)
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_mark_uploaded_oserror_is_logged_not_raised(
        self,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201)

        client: ExchangeClient = _make_client(handler)
        try:
            fm: _FakeFileManager = _FakeFileManager()
            fm.raise_on_mark = True
            client.enqueue_upload(
                'https://fake/api',
                json={},
                file_manager=fm,
                filename='foo.json.br',
            )
            # Must not raise — the worker swallows and logs.
            with self.assertLogs(
                'scrape_exchange.exchange_client', level='ERROR',
            ):
                await client.drain_uploads(timeout=2.0)
            self.assertEqual(fm.marked, [])
        finally:
            await httpx.AsyncClient.aclose(client)


if __name__ == '__main__':
    unittest.main()
