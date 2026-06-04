'''Regression: the channel scraper must build its identity maps with
the correct constructor arguments. RedisNameMap takes a DSN string and
builds its own client, unlike RedisHandleMap which takes a client;
passing a client to RedisNameMap crashes the worker at startup.
'''

import unittest
from unittest import mock


class TestBuildIdentityMaps(unittest.TestCase):
    def test_redis_backends_construct_without_error(self) -> None:
        from tools.yt_channel_scrape import _build_identity_maps
        from scrape_exchange.name_map import RedisNameMap
        from scrape_exchange.creator_map import RedisCreatorMap
        settings = mock.MagicMock()
        settings.redis_dsn = 'redis://localhost:6379/0'
        creator, handle, name = _build_identity_maps(settings)
        self.assertIsInstance(creator, RedisCreatorMap)
        self.assertIsInstance(name, RedisNameMap)

    def test_file_backends_when_no_redis(self) -> None:
        from tools.yt_channel_scrape import _build_identity_maps
        from scrape_exchange.name_map import NullNameMap
        settings = mock.MagicMock()
        settings.redis_dsn = ''
        settings.channel_map_file = '/tmp/does-not-matter.json'
        _creator, _handle, name = _build_identity_maps(settings)
        self.assertIsInstance(name, NullNameMap)


if __name__ == '__main__':
    unittest.main()
