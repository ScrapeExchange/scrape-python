'''
Unit tests for scrape_exchange.redis_copy.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest

from scrape_exchange.redis_copy import (
    Endpoint,
    check_distinct,
    endpoint_from_dsn,
    migrate_args,
    split_batches,
)


class TestEndpoints(unittest.TestCase):

    def test_parses_password_and_db(self) -> None:
        ep: Endpoint = endpoint_from_dsn('redis://:pw@localhost:6380/1')
        self.assertEqual(ep, Endpoint('localhost', 6380, 1, None, 'pw'))

    def test_defaults(self) -> None:
        ep: Endpoint = endpoint_from_dsn('redis://localhost')
        self.assertEqual(ep, Endpoint('localhost', 6379, 0, None, None))

    def test_same_server_and_db_is_refused(self) -> None:
        a: Endpoint = endpoint_from_dsn('redis://:x@localhost:6379/1')
        b: Endpoint = endpoint_from_dsn('redis://:y@localhost:6379/1')
        with self.assertRaises(ValueError):
            check_distinct(a, b)

    def test_same_server_other_db_is_allowed(self) -> None:
        check_distinct(
            endpoint_from_dsn('redis://localhost:6379/0'),
            endpoint_from_dsn('redis://localhost:6379/1'),
        )


class TestBatches(unittest.TestCase):

    def test_big_keys_are_separated(self) -> None:
        batches, big = split_batches(
            ['a', 'b', 'c', 'd', 'e'],
            {'a': 10, 'b': 500, 'c': 10, 'd': 10, 'e': 10},
            batch_size=2, big_key_bytes=100,
        )
        self.assertEqual(batches, [['a', 'c'], ['d', 'e']])
        self.assertEqual(big, ['b'])

    def test_unknown_size_counts_as_small(self) -> None:
        batches, big = split_batches(['a'], {}, 10, 100)
        self.assertEqual((batches, big), ([['a']], []))


class TestMigrateArgs(unittest.TestCase):

    def test_password_only_uses_auth(self) -> None:
        ep: Endpoint = Endpoint('localhost', 6379, 1, None, 'pw')
        self.assertEqual(
            migrate_args(ep, ['k1', 'k2'], 5000),
            ['MIGRATE', 'localhost', 6379, '', 1, 5000, 'COPY',
             'AUTH', 'pw', 'KEYS', 'k1', 'k2'],
        )

    def test_username_uses_auth2(self) -> None:
        ep: Endpoint = Endpoint('localhost', 6379, 1, 'u', 'pw')
        self.assertIn('AUTH2', migrate_args(ep, ['k'], 1))

    def test_no_password_no_auth(self) -> None:
        ep: Endpoint = Endpoint('localhost', 6379, 1, None, None)
        args: list = migrate_args(ep, ['k'], 1)
        self.assertNotIn('AUTH', args)
        self.assertNotIn('REPLACE', args)


if __name__ == '__main__':
    unittest.main()
