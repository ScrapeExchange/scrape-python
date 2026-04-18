'''
Unit tests for the file-backed rate-limiter backend.

Exercises :class:`_SharedFileBackend` directly (bypassing the
:class:`RateLimiter` orchestration) and the shared-state path
through :class:`YouTubeRateLimiter` end-to-end.
'''

import asyncio
import json
import os
import tempfile
import unittest

from unittest.mock import patch

from scrape_exchange.rate_limiter import (
    _InProcessBackend,
    _SharedFileBackend,
    _assert_local_filesystem,
    _detect_fs_type,
    _proxy_filename,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
    YouTubeCallType,
    _DEFAULT_CONFIGS,
    _GLOBAL_CONFIG,
)


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestProxyFilename(unittest.TestCase):

    def test_stable_hash_for_same_url(self) -> None:
        a: str = _proxy_filename('http://user:pw@host:1234')
        b: str = _proxy_filename('http://user:pw@host:1234')
        self.assertEqual(a, b)

    def test_different_url_yields_different_name(self) -> None:
        a: str = _proxy_filename('http://user:pw@host:1234')
        b: str = _proxy_filename('http://user:pw@host:1235')
        self.assertNotEqual(a, b)

    def test_no_credentials_in_filename(self) -> None:
        name: str = _proxy_filename('http://secret:verysecret@h:80')
        self.assertNotIn('secret', name)
        self.assertNotIn('verysecret', name)

    def test_none_proxy_has_stable_name(self) -> None:
        self.assertEqual(
            _proxy_filename(None), _proxy_filename(None),
        )
        self.assertNotEqual(
            _proxy_filename(None),
            _proxy_filename('http://something:80'),
        )


class TestDetectFsType(unittest.TestCase):

    def test_detects_tmpfs_or_local_fs_for_tmp(self) -> None:
        fs: str | None = _detect_fs_type('/tmp')
        # /tmp is either tmpfs on modern Linux or a local fs; must
        # not be one of the rejected remote types.
        self.assertIsNotNone(fs)
        self.assertNotIn(
            fs, {'nfs', 'nfs4', 'cifs', 'fuse.sshfs'},
        )

    def test_assert_local_filesystem_accepts_tmp(self) -> None:
        # Should not raise on /tmp.
        _assert_local_filesystem('/tmp')

    def test_assert_local_filesystem_rejects_nfs(self) -> None:
        with patch(
            'scrape_exchange.rate_limiter._detect_fs_type',
            return_value='nfs4',
        ):
            with self.assertRaises(RuntimeError) as cm:
                _assert_local_filesystem('/mnt/nfs')
            self.assertIn('nfs4', str(cm.exception))

    def test_assert_local_filesystem_rejects_sshfs(self) -> None:
        with patch(
            'scrape_exchange.rate_limiter._detect_fs_type',
            return_value='fuse.sshfs',
        ):
            with self.assertRaises(RuntimeError):
                _assert_local_filesystem('/mnt/ssh')

    def test_assert_local_filesystem_warns_but_passes_unknown(
        self,
    ) -> None:
        with patch(
            'scrape_exchange.rate_limiter._detect_fs_type',
            return_value=None,
        ):
            # No raise; a warning is logged.
            _assert_local_filesystem('/definitely/not/mounted')


class TestSharedFileBackendBasics(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: str = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _make(self) -> _SharedFileBackend:
        return _SharedFileBackend(
            _DEFAULT_CONFIGS, _GLOBAL_CONFIG, self.state_dir,
        )

    def test_creates_state_dir(self) -> None:
        nested: str = os.path.join(self.state_dir, 'a', 'b')
        _SharedFileBackend(
            _DEFAULT_CONFIGS, _GLOBAL_CONFIG, nested,
        )
        self.assertTrue(os.path.isdir(nested))

    def test_peek_tokens_returns_burst_for_fresh_proxy(self) -> None:
        backend: _SharedFileBackend = self._make()
        tokens: float = backend.peek_tokens(
            YouTubeCallType.BROWSE, 'http://a:3128',
        )
        expected: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        self.assertEqual(tokens, float(expected))

    def test_try_acquire_consumes_token_and_persists(self) -> None:
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        wait, bucket_tokens, _ = _run(
            backend.try_acquire(YouTubeCallType.BROWSE, proxy),
        )
        self.assertEqual(wait, 0.0)
        self.assertAlmostEqual(bucket_tokens, burst - 1, delta=0.01)

        # State file must now exist and contain the decremented value.
        path: str = backend._path(proxy)
        self.assertTrue(os.path.isfile(path))
        with open(path) as f:
            doc: dict = json.loads(f.read())
        self.assertEqual(doc['version'], 1)
        persisted: float = doc['buckets']['browse']['tokens']
        self.assertAlmostEqual(persisted, burst - 1, delta=0.01)

    def test_two_backends_share_state(self) -> None:
        backend1: _SharedFileBackend = self._make()
        backend2: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst

        # Consume 3 tokens via backend1.
        for _ in range(3):
            _run(backend1.try_acquire(
                YouTubeCallType.BROWSE, proxy,
            ))

        # backend2 reads the file and should see the decremented
        # value on its next try_acquire.
        wait, bucket_tokens, _ = _run(
            backend2.try_acquire(YouTubeCallType.BROWSE, proxy),
        )
        self.assertEqual(wait, 0.0)
        self.assertAlmostEqual(
            bucket_tokens, burst - 4, delta=0.2,
        )

    def test_penalise_drains_tokens_on_disk(self) -> None:
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        cfg = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE]
        burst: int = cfg.burst
        refill_rate: float = cfg.refill_rate

        # Touch the file first so it exists with full burst.
        _run(backend.try_acquire(YouTubeCallType.BROWSE, proxy))

        penalty_seconds: float = 10.0
        _run(backend.penalise(
            YouTubeCallType.BROWSE, proxy, penalty_seconds,
        ))

        # The next try_acquire should now report wait > 0.
        wait, bucket_tokens, _ = _run(
            backend.try_acquire(YouTubeCallType.BROWSE, proxy),
        )
        drained_by: float = penalty_seconds * refill_rate
        self.assertLess(
            bucket_tokens,
            float(burst) - drained_by + 1.0,
        )
        self.assertGreater(wait, 0.0)

    def test_penalise_clamps_at_minus_burst(self) -> None:
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        cfg = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE]
        burst: int = cfg.burst

        _run(backend.try_acquire(YouTubeCallType.BROWSE, proxy))
        # Apply an absurdly large penalty.
        _run(backend.penalise(
            YouTubeCallType.BROWSE, proxy, 10_000.0,
        ))

        with open(backend._path(proxy)) as f:
            doc: dict = json.loads(f.read())
        tokens: float = doc['buckets']['browse']['tokens']
        self.assertGreaterEqual(tokens, -float(burst))

    def test_try_acquire_persists_even_when_wait_needed(
        self,
    ) -> None:
        '''Refill state must be persisted even on a no-acquire path
        so that a reader right after this call sees the same
        last_refill and doesn't double-count elapsed time.'''
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        _run(backend.try_acquire(YouTubeCallType.BROWSE, proxy))

        _run(backend.penalise(
            YouTubeCallType.BROWSE, proxy, 10_000.0,
        ))
        wait, _, _ = _run(
            backend.try_acquire(YouTubeCallType.BROWSE, proxy),
        )
        self.assertGreater(wait, 0.0)
        # State file still contains the penalised bucket.
        with open(backend._path(proxy)) as f:
            doc: dict = json.loads(f.read())
        tokens: float = doc['buckets']['browse']['tokens']
        self.assertLess(tokens, 0.0)

    def test_corrupt_state_file_reinitialised(self) -> None:
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        path: str = backend._path(proxy)
        with open(path, 'w') as f:
            f.write('{ this is not valid json')

        # Should not raise; backend reinits the bucket.
        wait, bucket_tokens, _ = _run(
            backend.try_acquire(YouTubeCallType.BROWSE, proxy),
        )
        self.assertEqual(wait, 0.0)
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        self.assertAlmostEqual(
            bucket_tokens, burst - 1, delta=0.1,
        )

    def test_cache_is_updated_after_acquire(self) -> None:
        backend: _SharedFileBackend = self._make()
        proxy: str = 'http://a:3128'
        _run(backend.try_acquire(YouTubeCallType.BROWSE, proxy))
        # After one try_acquire, peek_tokens for this proxy should
        # reflect the consumed state (<= burst - 1 + tiny refill).
        tokens: float = backend.peek_tokens(
            YouTubeCallType.BROWSE, proxy,
        )
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        self.assertLessEqual(tokens, float(burst) - 0.5)

    def test_in_process_backend_returns_same_config_burst(
        self,
    ) -> None:
        '''Sanity check that the old in-process backend still works.'''
        backend: _InProcessBackend = _InProcessBackend(
            _DEFAULT_CONFIGS, _GLOBAL_CONFIG,
        )
        tokens: float = backend.peek_tokens(
            YouTubeCallType.BROWSE, 'http://a:3128',
        )
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        self.assertEqual(tokens, float(burst))


class TestRateLimiterSharedBackend(unittest.IsolatedAsyncioTestCase):
    '''
    Exercise :class:`YouTubeRateLimiter` end-to-end with the shared
    file backend, verifying that state persists across two separate
    limiter instances constructed against the same state dir.
    '''

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: str = self._tmp.name
        YouTubeRateLimiter.reset()
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform',
            return_value=0.0,
        )
        self._jitter_patcher.start()
        self._cookie_patcher = patch.object(
            YouTubeRateLimiter, 'get_cookie_file', return_value=None,
        )
        self._cookie_patcher.start()
        self._svc_patcher = patch.object(
            YouTubeRateLimiter,
            '_start_cookie_services',
            return_value=None,
        )
        self._svc_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        self._cookie_patcher.stop()
        self._svc_patcher.stop()
        YouTubeRateLimiter.reset()
        self._tmp.cleanup()

    async def test_acquire_goes_through_shared_backend(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get(
            state_dir=self.state_dir,
        )
        limiter.set_proxies(['http://a:3128'])
        proxy: str | None = await limiter.acquire(
            YouTubeCallType.BROWSE,
            proxy='http://a:3128',
        )
        self.assertEqual(proxy, 'http://a:3128')

        # State file must exist under the configured dir.
        files: list[str] = os.listdir(self.state_dir)
        self.assertTrue(
            any(f.endswith('.state') for f in files),
            f'no .state file written; got: {files}',
        )

    async def test_two_limiters_same_dir_share_tokens(self) -> None:
        limiter_a: YouTubeRateLimiter = YouTubeRateLimiter.get(
            state_dir=self.state_dir,
        )
        limiter_a.set_proxies(['http://a:3128'])
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst

        # Drain N tokens via limiter_a.
        for _ in range(3):
            await limiter_a.acquire(
                YouTubeCallType.BROWSE, proxy='http://a:3128',
            )

        # A fresh singleton (simulating another process) constructed
        # against the same dir should read the already-decremented
        # state on its next acquire.
        YouTubeRateLimiter.reset()
        limiter_b: YouTubeRateLimiter = YouTubeRateLimiter.get(
            state_dir=self.state_dir,
        )
        limiter_b.set_proxies(['http://a:3128'])

        from scrape_exchange.rate_limiter import _SharedFileBackend
        self.assertIsInstance(
            limiter_b._backend, _SharedFileBackend,
        )
        tokens: float = await limiter_b._backend.try_acquire(
            YouTubeCallType.BROWSE, 'http://a:3128',
        ) and 0
        # Read state file directly — the authoritative value.
        files: list[str] = [
            f for f in os.listdir(self.state_dir)
            if f.endswith('.state')
        ]
        self.assertEqual(len(files), 1)
        with open(os.path.join(self.state_dir, files[0])) as f:
            doc: dict = json.loads(f.read())
        persisted: float = doc['buckets']['browse']['tokens']
        # 3 from limiter_a + 1 from limiter_b above = 4 consumed.
        self.assertAlmostEqual(
            persisted, burst - 4, delta=0.5,
        )

    async def test_penalise_is_fleet_wide(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get(
            state_dir=self.state_dir,
        )
        limiter.set_proxies(['http://a:3128'])

        # Warm the bucket and then drain it via penalty.
        await limiter.acquire(
            YouTubeCallType.BROWSE, proxy='http://a:3128',
        )
        await limiter.penalise(
            YouTubeCallType.BROWSE, 'http://a:3128', 50.0,
        )

        # A second limiter (different process simulation) sees the
        # drained tokens.
        YouTubeRateLimiter.reset()
        limiter_b: YouTubeRateLimiter = YouTubeRateLimiter.get(
            state_dir=self.state_dir,
        )
        limiter_b.set_proxies(['http://a:3128'])
        # Read from disk via try_acquire (authoritative).
        wait, _, _ = await limiter_b._backend.try_acquire(
            YouTubeCallType.BROWSE, 'http://a:3128',
        )
        self.assertGreater(wait, 0.0)


if __name__ == '__main__':
    unittest.main()
