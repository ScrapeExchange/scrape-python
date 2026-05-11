'''Unit tests for the generic _LazyAsyncPool helper.'''

import unittest

from scrape_exchange._lazy_async_pool import _LazyAsyncPool


class _FakeClient:
    '''Records aclose / close calls for testing.'''

    def __init__(self, key: str) -> None:
        self.key: str = key
        self.aclose_called: int = 0
        self.close_called: int = 0

    async def aclose(self) -> None:
        self.aclose_called += 1

    def close(self) -> None:
        self.close_called += 1


class TestLazyAsyncPool(unittest.IsolatedAsyncioTestCase):

    def _make(self, **kw: object) -> _LazyAsyncPool:
        '''Factory wrapper to keep tests terse.'''
        defaults: dict = {
            'factory': _FakeClient,
        }
        defaults.update(kw)
        return _LazyAsyncPool(**defaults)

    async def test_same_key_returns_same_instance(self) -> None:
        pool: _LazyAsyncPool = self._make()
        a: _FakeClient = pool.get('k1')
        b: _FakeClient = pool.get('k1')
        self.assertIs(a, b)

    async def test_different_keys_return_different_instances(
        self,
    ) -> None:
        pool: _LazyAsyncPool = self._make()
        a: _FakeClient = pool.get('k1')
        b: _FakeClient = pool.get('k2')
        self.assertIsNot(a, b)
        self.assertEqual(a.key, 'k1')
        self.assertEqual(b.key, 'k2')

    async def test_aclose_all_calls_aclose_and_clears(self) -> None:
        pool: _LazyAsyncPool = self._make()
        a: _FakeClient = pool.get('k1')
        b: _FakeClient = pool.get('k2')
        await pool.aclose_all()
        self.assertEqual(a.aclose_called, 1)
        self.assertEqual(b.aclose_called, 1)
        # Cache cleared: subsequent get() rebuilds.
        c: _FakeClient = pool.get('k1')
        self.assertIsNot(c, a)

    async def test_aclose_all_with_sync_close_attr(self) -> None:
        pool: _LazyAsyncPool = self._make(aclose_attr='close')
        a: _FakeClient = pool.get('k1')
        await pool.aclose_all()
        self.assertEqual(a.close_called, 1)

    async def test_aclose_all_swallows_exceptions(self) -> None:
        '''A failing aclose on one client does not block the others.'''
        class _Boom:
            async def aclose(self) -> None:
                raise RuntimeError('boom')

        ok: _FakeClient = _FakeClient('ok')
        boom: _Boom = _Boom()
        # Inject pre-built values to control which key returns what.
        pool: _LazyAsyncPool = self._make(
            factory=lambda k: ok if k == 'ok' else boom,
        )
        pool.get('ok')
        pool.get('boom')
        await pool.aclose_all()  # must not raise
        self.assertEqual(ok.aclose_called, 1)

    def test_reset_for_tests_clears_without_aclose(self) -> None:
        pool: _LazyAsyncPool = self._make()
        a: _FakeClient = pool.get('k1')
        pool.reset_for_tests()
        self.assertEqual(a.aclose_called, 0)
        # Cache cleared: rebuild yields a new instance.
        b: _FakeClient = pool.get('k1')
        self.assertIsNot(b, a)


if __name__ == '__main__':
    unittest.main()
