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

    async def test_aclose_key_only_closes_requested_entry(
        self,
    ) -> None:
        pool: _LazyAsyncPool = self._make(aclose_attr='close')
        first: _FakeClient = pool.get('k1')
        second: _FakeClient = pool.get('k2')

        await pool.aclose_key('k1')

        self.assertEqual(first.close_called, 1)
        self.assertEqual(second.close_called, 0)
        self.assertIsNot(pool.get('k1'), first)
        self.assertIs(pool.get('k2'), second)

    async def test_retire_defers_close_until_borrowers_release(
        self,
    ) -> None:
        pool: _LazyAsyncPool = self._make(aclose_attr='close')
        first_borrow: _FakeClient = pool.borrow('k1')
        second_borrow: _FakeClient = pool.borrow('k1')

        retired: bool = await pool.retire_key(
            'k1', expected=first_borrow,
        )
        replacement: _FakeClient = pool.get('k1')

        self.assertTrue(retired)
        self.assertIs(first_borrow, second_borrow)
        self.assertIsNot(replacement, first_borrow)
        self.assertEqual(first_borrow.close_called, 0)

        await pool.release(first_borrow)
        self.assertEqual(first_borrow.close_called, 0)

        await pool.release(second_borrow)
        self.assertEqual(first_borrow.close_called, 1)

    async def test_stale_retire_does_not_evict_replacement(
        self,
    ) -> None:
        pool: _LazyAsyncPool = self._make(aclose_attr='close')
        challenged: _FakeClient = pool.borrow('k1')
        await pool.retire_key('k1', expected=challenged)
        replacement: _FakeClient = pool.get('k1')

        retired: bool = await pool.retire_key(
            'k1', expected=challenged,
        )

        self.assertFalse(retired)
        self.assertIs(pool.get('k1'), replacement)
        await pool.release(challenged)

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


class TestLazyAsyncPoolSlots(unittest.IsolatedAsyncioTestCase):
    '''Several clients per key when ``slots`` > 1.'''

    def _make(self, slots: int) -> _LazyAsyncPool:
        return _LazyAsyncPool(
            factory=_FakeClient, aclose_attr='close', slots=slots,
        )

    async def test_sequential_borrows_reuse_one_client(self) -> None:
        pool: _LazyAsyncPool = self._make(3)
        first: _FakeClient = pool.borrow('k1')
        await pool.release(first)
        second: _FakeClient = pool.borrow('k1')
        await pool.release(second)
        self.assertIs(first, second)
        self.assertEqual(len(pool._cache['k1']), 1)

    async def test_concurrent_borrows_get_separate_clients(self) -> None:
        pool: _LazyAsyncPool = self._make(3)
        clients: list[_FakeClient] = [pool.borrow('k1') for _ in range(3)]
        self.assertEqual(len({id(c) for c in clients}), 3)

    async def test_borrows_beyond_slots_share_least_busy(self) -> None:
        pool: _LazyAsyncPool = self._make(2)
        a: _FakeClient = pool.borrow('k1')
        b: _FakeClient = pool.borrow('k1')
        c: _FakeClient = pool.borrow('k1')
        self.assertIsNot(a, b)
        self.assertIn(c, (a, b))
        self.assertEqual(len(pool._cache['k1']), 2)
        await pool.release(a if c is b else b)
        d: _FakeClient = pool.borrow('k1')
        self.assertIsNot(d, c)

    async def test_idle_client_preferred_over_new_one(self) -> None:
        pool: _LazyAsyncPool = self._make(3)
        a: _FakeClient = pool.borrow('k1')
        b: _FakeClient = pool.borrow('k1')
        await pool.release(a)
        again: _FakeClient = pool.borrow('k1')
        self.assertIs(again, a)
        self.assertEqual(len(pool._cache['k1']), 2)
        await pool.release(b)

    async def test_keys_have_separate_slots(self) -> None:
        pool: _LazyAsyncPool = self._make(2)
        a: _FakeClient = pool.borrow('k1')
        b: _FakeClient = pool.borrow('k2')
        self.assertEqual(a.key, 'k1')
        self.assertEqual(b.key, 'k2')

    async def test_retire_keeps_other_clients_of_key(self) -> None:
        pool: _LazyAsyncPool = self._make(2)
        a: _FakeClient = pool.borrow('k1')
        b: _FakeClient = pool.borrow('k1')
        retired: bool = await pool.retire_key('k1', expected=a)
        self.assertTrue(retired)
        self.assertEqual(pool._cache['k1'], [b])
        self.assertEqual(a.close_called, 0)
        await pool.release(a)
        self.assertEqual(a.close_called, 1)
        replacement: _FakeClient = pool.borrow('k1')
        self.assertIsNot(replacement, a)
        self.assertIsNot(replacement, b)

    async def test_aclose_key_and_all_close_every_slot(self) -> None:
        pool: _LazyAsyncPool = self._make(2)
        a: _FakeClient = pool.borrow('k1')
        b: _FakeClient = pool.borrow('k1')
        c: _FakeClient = pool.borrow('k2')
        await pool.aclose_key('k1')
        self.assertEqual((a.close_called, b.close_called), (1, 1))
        self.assertEqual(c.close_called, 0)
        await pool.aclose_all()
        self.assertEqual(c.close_called, 1)

    def test_get_returns_first_client(self) -> None:
        pool: _LazyAsyncPool = self._make(2)
        a: _FakeClient = pool.borrow('k1')
        pool.borrow('k1')
        self.assertIs(pool.get('k1'), a)


if __name__ == '__main__':
    unittest.main()
