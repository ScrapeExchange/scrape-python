'''
Unit tests for scrape_exchange.redis_copy.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest
from unittest.mock import MagicMock

import fakeredis
import redis

from scrape_exchange.redis_copy import (
    CopyStats,
    Endpoint,
    MigrateError,
    VerifyReport,
    check_distinct,
    copy_all,
    endpoint_from_dsn,
    key_shape,
    migrate_args,
    sample_copy,
    split_batches,
    verify,
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

    def test_same_server_other_db_is_refused(self) -> None:
        # MIGRATE blocks the source server while it waits for the
        # target, so a server cannot MIGRATE to itself.
        with self.assertRaises(ValueError):
            check_distinct(
                endpoint_from_dsn('redis://localhost:6379/0'),
                endpoint_from_dsn('redis://localhost:6379/1'),
            )

    def test_other_server_is_allowed(self) -> None:
        check_distinct(
            endpoint_from_dsn('redis://localhost:6379/0'),
            endpoint_from_dsn('redis://localhost:6380/0'),
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


class _SizePipe:
    '''Pipeline double answering every MEMORY USAGE with *size*.'''

    def __init__(self, size: int) -> None:
        self.size: int = size
        self.queued: int = 0
        self.executions: int = 0

    def memory_usage(self, key: str) -> None:
        self.queued += 1

    def execute(self) -> list[int]:
        sizes: list[int] = [self.size] * self.queued
        self.queued = 0
        self.executions += 1
        return sizes


def _source(
    keys: list[str], replies: list[object], size: int = 10,
) -> MagicMock:
    '''A source client whose SCAN yields *keys*, whose keys all use
    *size* bytes and whose MIGRATE calls return (or raise) *replies* in
    order.'''
    src: MagicMock = MagicMock()
    src.scan_iter.return_value = iter(keys)
    src.pipeline.return_value = _SizePipe(size)
    src.execute_command.side_effect = replies
    src.dbsize.return_value = len(keys)
    return src


def _target(size: int = 0) -> MagicMock:
    tgt: MagicMock = MagicMock()
    tgt.dbsize.return_value = size
    return tgt


EP: Endpoint = Endpoint('localhost', 6379, 1, None, 'pw')
KW: dict = dict(
    batch_size=2, timeout_ms=1000, big_key_bytes=100,
    big_key_timeout_ms=9000,
)


class TestCopyAll(unittest.TestCase):

    def test_copies_in_batches(self) -> None:
        src: MagicMock = _source(['a', 'b', 'c'], [b'OK', b'OK'])
        stats, sent = copy_all(src, EP, _target(), **KW)
        self.assertEqual((stats.copied, stats.skipped), (3, 0))
        self.assertEqual(sent, ['a', 'b', 'c'])
        first: tuple = src.execute_command.call_args_list[0].args
        self.assertEqual(first[-3:], ('KEYS', 'a', 'b'))

    def test_nokey_counts_as_skipped(self) -> None:
        src: MagicMock = _source(['a', 'b'], [b'NOKEY'])
        stats, _ = copy_all(src, EP, _target(), **KW)
        self.assertEqual((stats.copied, stats.skipped), (0, 2))

    def test_busykey_stops_the_copy(self) -> None:
        src: MagicMock = _source(
            ['a', 'b'],
            [redis.ResponseError('BUSYKEY Target key name already exists')],
        )
        with self.assertRaises(MigrateError):
            copy_all(src, EP, _target(), **KW)

    def test_big_key_uses_long_timeout(self) -> None:
        src: MagicMock = _source(['big'], [b'OK'], size=1000)
        copy_all(src, EP, _target(), **KW)
        args: tuple = src.execute_command.call_args.args
        self.assertIn(9000, args)

    def test_sizes_take_one_round_trip_per_batch(self) -> None:
        src: MagicMock = _source(['a', 'b', 'c', 'd'], [b'OK', b'OK'])
        copy_all(src, EP, _target(), **KW)
        src.memory_usage.assert_not_called()
        self.assertEqual(src.pipeline.return_value.executions, 2)

    def test_refuses_nonempty_target(self) -> None:
        with self.assertRaises(ValueError):
            copy_all(_source(['a'], []), EP, _target(5), **KW)

    def test_nonempty_target_allowed_when_asked(self) -> None:
        stats, _ = copy_all(
            _source(['a'], [b'OK']), EP, _target(5),
            allow_nonempty=True, **KW,
        )
        self.assertEqual(stats.copied, 1)

    def test_limit_stops_early(self) -> None:
        src: MagicMock = _source(['a', 'b', 'c', 'd'], [b'OK'])
        stats, sent = copy_all(src, EP, _target(), limit=2, **KW)
        self.assertEqual(sent, ['a', 'b'])


class TestSampleCopy(unittest.TestCase):

    def test_deletes_exactly_the_sampled_keys(self) -> None:
        src: MagicMock = _source(['a', 'b', 'c'], [b'OK'])
        src.dbsize.return_value = 300
        tgt: MagicMock = _target()
        report = sample_copy(src, EP, tgt, size=2, **KW)
        tgt.delete.assert_called_once_with('a', 'b')
        self.assertEqual(report.keys, 2)
        self.assertEqual(report.source_dbsize, 300)
        self.assertGreaterEqual(report.estimated_seconds, 0.0)


class TestVerify(unittest.TestCase):

    def setUp(self) -> None:
        self.src: fakeredis.FakeRedis = fakeredis.FakeRedis(
            server=fakeredis.FakeServer(), decode_responses=True,
        )
        self.tgt: fakeredis.FakeRedis = fakeredis.FakeRedis(
            server=fakeredis.FakeServer(), decode_responses=True,
        )

    def _both(self, fn: str, *args: object, **kwargs: object) -> None:
        getattr(self.src, fn)(*args, **kwargs)
        getattr(self.tgt, fn)(*args, **kwargs)

    def test_identical_databases_match(self) -> None:
        self._both('hset', 'h', mapping={'a': '1', 'b': '2'})
        self._both('zadd', 'z', {'m': 1.0})
        self._both('set', 's', 'v')
        report: VerifyReport = verify(self.src, self.tgt, sample_size=10)
        self.assertEqual(report.mismatches, [])
        self.assertEqual(report.checked, 3)

    def test_detects_missing_and_different_keys(self) -> None:
        self._both('set', 's', 'v')
        self.src.hset('h', mapping={'a': '1', 'b': '2'})
        self.tgt.hset('h', mapping={'a': '1'})
        self.src.set('only_src', 'x')
        report: VerifyReport = verify(self.src, self.tgt, sample_size=10)
        self.assertEqual(sorted(report.mismatches), ['h', 'only_src'])

    def test_detects_lost_expiry(self) -> None:
        self.src.set('t', 'v', ex=600)
        self.tgt.set('t', 'v')
        report: VerifyReport = verify(self.src, self.tgt, sample_size=10)
        self.assertEqual(report.mismatches, ['t'])

    def test_key_expired_in_source_is_not_a_mismatch(self) -> None:
        self.assertIsNone(key_shape(self.src, 'gone'))
        self.tgt.set('gone', 'v')
        report: VerifyReport = verify(self.src, self.tgt, sample_size=10)
        self.assertEqual(report.mismatches, [])


class TestCliSettings(unittest.TestCase):

    def test_refuses_same_database(self) -> None:
        from tools.redis_copy_db import CopySettings, run

        settings: CopySettings = CopySettings(
            _cli_parse_args=[],
            source_dsn='redis://localhost:6379/3',
            target_dsn='redis://localhost:6379/3',
        )
        self.assertEqual(run(settings), 2)


if __name__ == '__main__':
    unittest.main()
