'''
Integration test for tools/redis_copy_db.py.

Requires two live Redis 8 servers on localhost, ports 6379 (source)
and 6380 (target): MIGRATE cannot target the source's own server. It
uses database 14 on the source and 15 on the target and empties both.
Start them with host networking, so the source can reach the target:

    docker run --rm -d --network host --name rc-src redis:8.10.2
    docker run --rm -d --network host --name rc-tgt redis:8.10.2 \
        --port 6380
    RUN_INTEGRATION=1 uv run python -m unittest \
        tests.integration.test_redis_copy_db

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest

import redis

from tools.redis_copy_db import CopySettings, run

SOURCE_DSN: str = 'redis://localhost:6379/14'
TARGET_DSN: str = 'redis://localhost:6380/15'


def _settings(**kwargs: object) -> CopySettings:
    return CopySettings(
        _cli_parse_args=[], source_dsn=SOURCE_DSN,
        target_dsn=TARGET_DSN, batch_size=100, big_key_bytes=50_000,
        **kwargs,
    )


class TestRedisCopyDb(unittest.TestCase):

    def setUp(self) -> None:
        self.src: redis.Redis = redis.Redis.from_url(SOURCE_DSN)
        self.tgt: redis.Redis = redis.Redis.from_url(TARGET_DSN)
        try:
            self.src.ping()
            self.tgt.ping()
        except redis.ConnectionError:
            self.skipTest('no Redis on localhost:6379 and :6380')
        self.src.flushdb()
        self.tgt.flushdb()
        pipe: redis.client.Pipeline = self.src.pipeline()
        i: int
        for i in range(2000):
            pipe.hset(f'h:{i}', mapping={'a': str(i), 'b': 'x'})
            pipe.set(f's:{i}', 'v', ex=3600 if i % 2 else None)
        pipe.zadd('big:z', {f'm{i}': i for i in range(5000)})
        pipe.execute()
        try:
            self.src.execute_command('BF.RESERVE', 'bf', '0.001', '1000')
            self.src.execute_command('BF.MADD', 'bf', 'a', 'b')
        except redis.ResponseError:
            pass

    def tearDown(self) -> None:
        self.src.flushdb()
        self.tgt.flushdb()

    def test_copy_then_verify(self) -> None:
        self.assertEqual(run(_settings()), 0)
        self.assertEqual(self.tgt.dbsize(), self.src.dbsize())
        self.assertEqual(self.tgt.zcard('big:z'), 5000)
        self.assertGreater(self.tgt.ttl('s:1'), 0)
        self.assertEqual(self.tgt.ttl('s:2'), -1)
        self.assertEqual(
            run(_settings(mode='verify', sample_size=10_000)), 0,
        )

    def test_second_copy_is_refused(self) -> None:
        self.assertEqual(run(_settings()), 0)
        self.assertEqual(run(_settings()), 2)

    def test_existing_key_stops_copy(self) -> None:
        self.tgt.set('h:5', 'conflict')
        self.assertEqual(run(_settings(allow_nonempty=True)), 1)
        self.assertEqual(self.tgt.get('h:5'), b'conflict')

    def test_sample_leaves_target_empty(self) -> None:
        self.assertEqual(
            run(_settings(mode='sample', sample_size=500)), 0,
        )
        self.assertEqual(self.tgt.dbsize(), 0)


if __name__ == '__main__':
    unittest.main()
