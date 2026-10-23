'''Integration tests: the Python client against a real backlog-server.

Needs the release binary. Build it with
``cd backlog-server && cargo build --release``, or point
``BACKLOG_SERVER_BIN`` at another build. The tests skip when it is
missing.
'''

import asyncio
import os
import random
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

import redis

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.redis_client import (
    backlog_redis_from_url,
    redis_from_url,
)
from scrape_exchange.video_scrape_queue import (
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
)

REPO: Path = Path(__file__).resolve().parents[2]
BIN: Path = Path(os.environ.get(
    'BACKLOG_SERVER_BIN',
    REPO / 'backlog-server' / 'target' / 'release' / 'backlog-server',
))
PASSWORD: str = 'integration-test'
ALPHABET: str = (
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
)
LAST: str = 'AEIMQUYcgkosw048'


def video_ids(n: int, seed: int) -> list[str]:
    '''*n* distinct valid YouTube video IDs.'''
    rng: random.Random = random.Random(seed)
    out: set[str] = set()
    while len(out) < n:
        head: str = ''.join(rng.choice(ALPHABET) for _ in range(10))
        out.add(head + rng.choice(LAST))
    return sorted(out)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return int(s.getsockname()[1])


class Server:
    '''One backlog-server process on free ports.'''

    def __init__(self, data_dir: Path, **env: str) -> None:
        self.data_dir: Path = data_dir
        self.port: int = free_port()
        self.metrics_port: int = free_port()
        self.log_path: Path = data_dir.parent / f'server-{self.port}.log'
        self.env: dict[str, str] = {
            'PATH': os.environ.get('PATH', ''),
            'BACKLOG_DATA_DIR': str(data_dir),
            'BACKLOG_LISTEN': f'127.0.0.1:{self.port}',
            'BACKLOG_METRICS_LISTEN': f'127.0.0.1:{self.metrics_port}',
            'BACKLOG_PASSWORD': PASSWORD,
            'BACKLOG_FSYNC_INTERVAL_MS': '200',
            'BACKLOG_CHECKPOINT_INTERVAL_SECS': '1',
            **env,
        }
        self.proc: subprocess.Popen[bytes] | None = None
        self._log_fh: Any = None

    @property
    def dsn(self) -> str:
        return f'redis://:{PASSWORD}@localhost:{self.port}/0'

    def start(self) -> None:
        self._log_fh = self.log_path.open('ab')
        self.proc = subprocess.Popen(
            [str(BIN), 'serve'], env=self.env,
            stdout=self._log_fh, stderr=self._log_fh,
        )
        deadline: float = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if self.ping():
                    return
            except redis.exceptions.ConnectionError:
                time.sleep(0.05)
        raise RuntimeError(f'server did not start: {self.log()}')

    def ping(self) -> bool:
        with redis.Redis(
            port=self.port, password=PASSWORD, decode_responses=True,
        ) as c:
            return bool(c.ping())

    def log(self) -> str:
        return self.log_path.read_text(errors='replace')

    def stop(self, sig: int = signal.SIGTERM) -> int:
        assert self.proc is not None
        self.proc.send_signal(sig)
        code: int = self.proc.wait(timeout=60)
        self.proc = None
        self._log_fh.close()
        return code


@unittest.skipUnless(BIN.exists(), f'backlog-server binary not at {BIN}')
class TestBacklogServer(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory[str] = (
            tempfile.TemporaryDirectory()
        )
        self.data: Path = Path(self.tmp.name) / 'data'
        self.data.mkdir()
        self.servers: list[Server] = []
        self.clients: list[Any] = []

    async def asyncTearDown(self) -> None:
        for c in self.clients:
            await c.aclose()
            await c.connection_pool.disconnect()

    def tearDown(self) -> None:
        for s in self.servers:
            if s.proc is not None:
                s.stop(signal.SIGKILL)
        self.tmp.cleanup()

    def server(self, **env: str) -> Server:
        s: Server = Server(self.data, **env)
        s.start()
        self.servers.append(s)
        return s

    def async_client(self, s: Server, component: str) -> Any:
        c: Any = redis_from_url(
            s.dsn, component=component, decode_responses=True,
        )
        self.clients.append(c)
        return c

    def queue(self, s: Server) -> BacklogServerVideoScrapeQueue:
        return BacklogServerVideoScrapeQueue(
            self.async_client(s, 'integration-test'),
            VideoScrapeQueueSettings(),
        )

    async def test_handshake_via_redis_from_url(self) -> None:
        s: Server = self.server()
        client: Any = self.async_client(s, 'test')
        self.assertTrue(await client.ping())
        reply: list[int] = await client.execute_command(
            'BACKLOG.ADD', 'youtube:video', 'dQw4w9WgXcQ',
        )
        self.assertEqual(reply, [1])

    async def test_queue_roundtrip(self) -> None:
        s: Server = self.server()
        q: BacklogServerVideoScrapeQueue = self.queue(s)
        ids: list[str] = video_ids(4, seed=1)
        self.assertEqual(await q.enqueue_many(ids[:3], source='test'), 3)
        self.assertEqual(await q.enqueue_many(ids[:3], source='test'), 0)
        self.assertEqual(
            await q.force_enqueue(ids[3], source='test'), 'added',
        )
        entries: list[VideoScrapeQueueEntry] = await q.pop_entries(10)
        self.assertEqual(
            [e.video_id for e in entries], [ids[3], *ids[:3]],
        )
        self.assertTrue(await q.consume_force(ids[3]))
        await q.complete(ids[3])
        await q.mark(ids[0], state=VideoState.FAILED,
                     last_error='bot_detection')
        self.assertEqual(await q.bump_attempts(ids[1], last_error='x'), 0)
        counts: dict[VideoState, int] = await q.count_by_state()
        self.assertEqual(counts[VideoState.QUEUED], 2)
        self.assertEqual(counts[VideoState.FAILED], 1)
        self.assertIsNone(await q.get_state(ids[3]))
        self.assertEqual(await q.pop(10), [])

    async def test_kill9_keeps_synced_writes(self) -> None:
        s: Server = self.server()
        q: BacklogServerVideoScrapeQueue = self.queue(s)
        ids: list[str] = video_ids(1000, seed=2)
        self.assertEqual(await q.enqueue_many(ids, source='t'), 1000)
        await asyncio.sleep(1.0)
        s.stop(signal.SIGKILL)
        s.start()
        q2: BacklogServerVideoScrapeQueue = self.queue(s)
        states: dict[str, VideoState | None] = await q2.get_states(ids)
        self.assertTrue(
            all(v == VideoState.QUEUED for v in states.values()),
        )
        popped: list[str] = []
        while batch := await q2.pop(500):
            popped.extend(batch)
        self.assertEqual(popped, ids)

    async def test_kill9_during_compaction(self) -> None:
        s: Server = self.server(BACKLOG_COMPACTION_DELTA_ENTRIES='1000')
        q: BacklogServerVideoScrapeQueue = self.queue(s)
        ids: list[str] = video_ids(50_000, seed=3)
        self.assertEqual(await q.enqueue_many(ids, source='t'), 50_000)
        deadline: float = time.monotonic() + 10
        while 'compaction started' not in s.log():
            self.assertLess(time.monotonic(), deadline, s.log())
            await asyncio.sleep(0.01)
        s.stop(signal.SIGKILL)
        await asyncio.sleep(0.3)
        s.start()
        q2: BacklogServerVideoScrapeQueue = self.queue(s)
        known: dict[str, VideoState | None] = {}
        for start in range(0, len(ids), 5000):
            known.update(await q2.get_states(ids[start:start + 5000]))
        missing: list[str] = [k for k, v in known.items() if v is None]
        self.assertEqual(missing, [])
        leftovers: list[Path] = list(self.data.rglob('*.tmp'))
        self.assertEqual(leftovers, [])

    async def test_sigterm_clean_shutdown(self) -> None:
        s: Server = self.server()
        q: BacklogServerVideoScrapeQueue = self.queue(s)
        ids: list[str] = video_ids(10, seed=4)
        await q.enqueue_many(ids, source='t')
        self.assertEqual(s.stop(signal.SIGTERM), 0)
        self.assertIn('checkpoint written at shutdown', s.log())
        s.start()
        q2: BacklogServerVideoScrapeQueue = self.queue(s)
        self.assertEqual(await q2.pop(100), ids)

    def backlog_client(self, s: Server, **dsn_query: str) -> Any:
        '''One backlog client, built the way the scrapers build it.'''
        query: str = '&'.join(
            f'{key}={value}' for key, value in dsn_query.items()
        )
        dsn: str = f'{s.dsn}?{query}' if query else s.dsn
        client: Any = backlog_redis_from_url(
            dsn, component='restart-test', decode_responses=True,
        )
        self.clients.append(client)
        return client

    async def test_client_survives_a_server_restart(self) -> None:
        '''One client and one queue across a server restart: the command
        after the restart must run on a reconnected socket instead of
        raising at the scraper.'''
        s: Server = self.server()
        client: Any = self.backlog_client(s)
        q: BacklogServerVideoScrapeQueue = (
            BacklogServerVideoScrapeQueue(client, VideoScrapeQueueSettings())
        )
        ids: list[str] = video_ids(3, seed=5)
        self.assertEqual(await q.enqueue_many(ids, source='t'), 3)
        s.stop(signal.SIGKILL)
        await asyncio.sleep(0.2)
        s.start()

        # Same client, same pool, dead socket: reconnect and retry.
        self.assertEqual(await q.pop(10), ids)
        self.assertTrue(await client.ping())
        # A write through the reconnected socket lands too.
        await q.complete(ids[0])
        self.assertIsNone(await q.get_state(ids[0]))
        counts: dict[VideoState, int] = await q.count_by_state()
        self.assertEqual(counts[VideoState.QUEUED], 2)

    async def test_health_check_reconnects_an_idle_connection(
        self,
    ) -> None:
        '''A socket the server closed while the client was idle must be
        replaced on the next command instead of failing it.

        The health-check PING catches the dead socket, but the
        reconnect after it runs under the command retry policy, so this
        passes because of the two together, not the health check on
        its own (with BACKLOG_REDIS_COMMAND_RETRIES=0 it fails).'''
        s: Server = self.server()
        client: Any = self.backlog_client(s, health_check_interval='1')
        q: BacklogServerVideoScrapeQueue = (
            BacklogServerVideoScrapeQueue(client, VideoScrapeQueueSettings())
        )
        ids: list[str] = video_ids(2, seed=6)
        self.assertEqual(await q.enqueue_many(ids, source='t'), 2)
        s.stop(signal.SIGKILL)
        # Idle longer than health_check_interval with the peer gone.
        await asyncio.sleep(1.2)
        s.start()
        self.assertEqual(await q.get_states(ids), {
            ids[0]: VideoState.QUEUED, ids[1]: VideoState.QUEUED,
        })
        self.assertEqual(await q.pop(10), ids)


if __name__ == '__main__':
    unittest.main()
