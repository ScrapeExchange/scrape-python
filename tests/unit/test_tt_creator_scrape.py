'''
Unit tests for ``tools.tt_creator_scrape`` — the TikTok creator
scrape daemon. The TikTokSessionPool, CreatorQueue, and
AssetFileManagement collaborators are mocked or run against
tmpdirs; the live browser path is covered by the gated integration
test.
'''

import asyncio
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.file_management import (
    AssetFileManagement,
)
from tools import tt_creator_scrape as tool


def _raw_user(
    username: str = 'someone',
    followers: int = 5,
    sec_uid: str = 'SEC',
    user_id: str = '123',
    video_count: int = 0,
) -> dict:
    '''A minimal raw User.info envelope that from_user_info accepts.'''
    return {
        'userInfo': {
            'user': {
                'uniqueId': username,
                'secUid': sec_uid,
                'id': user_id,
                'nickname': 'Nick',
            },
            'stats': {
                'followerCount': followers,
                'videoCount': video_count,
            },
            'statsV2': {},
        },
    }


class _FakeSessionCtx:
    '''Async context manager standing in for pool.session_for.'''

    def __init__(self, api: object) -> None:
        self._api: object = api

    async def __aenter__(self) -> object:
        return self._api

    async def __aexit__(self, *exc: object) -> bool:
        return False


def _api_returning(resp: object) -> MagicMock:
    api: MagicMock = MagicMock()
    user_obj: MagicMock = MagicMock()
    user_obj.info = AsyncMock(return_value=resp)
    user_obj.videos = MagicMock(return_value=_async_items([]))
    user_obj.liked = MagicMock(return_value=_async_items([]))
    user_obj.playlists = MagicMock(return_value=_async_items([]))
    api.user = MagicMock(return_value=user_obj)
    api.make_request = AsyncMock(
        return_value={'itemList': [], 'hasMore': False},
    )
    return api


async def _async_items(items: list[object]) -> object:
    for item in items:
        yield item


def _video_item(video_id: str, username: str = 'author') -> MagicMock:
    item: MagicMock = MagicMock()
    item.as_dict = {
        'id': video_id,
        'author': {'uniqueId': username},
    }
    return item


def _playlist_item(
    playlist_id: str,
    name: str = 'Series',
    video_count: int = 7,
) -> MagicMock:
    item: MagicMock = MagicMock()
    item.id = playlist_id
    item.name = name
    item.video_count = video_count
    item.cover_url = 'https://example.com/cover.jpg'
    return item


def _settings(**overrides: object) -> MagicMock:
    s: MagicMock = MagicMock()
    s.creator_data_directory = overrides.get(
        'creator_data_directory', '/tmp/x',
    )
    s.redis_dsn = overrides.get('redis_dsn', 'redis://h:6379/0')
    s.session_state_dir = overrides.get(
        'session_state_dir', '/tmp/state',
    )
    s.session_bootstrap_timeout_ms = overrides.get(
        'session_bootstrap_timeout_ms', 90000,
    )
    s.creator_concurrency = overrides.get('creator_concurrency', 0)
    s.creator_num_processes = overrides.get('creator_num_processes', 1)
    s.creator_retry_interval_seconds = overrides.get(
        'creator_retry_interval_seconds', 300,
    )
    s.creator_rate_limit_retry_interval_seconds = overrides.get(
        'creator_rate_limit_retry_interval_seconds', 1800,
    )
    s.creator_retry_jitter_fraction = overrides.get(
        'creator_retry_jitter_fraction', 0,
    )
    s.creator_bot_failure_threshold = overrides.get(
        'creator_bot_failure_threshold', 1,
    )
    s.creator_bot_cooldown_seconds = overrides.get(
        'creator_bot_cooldown_seconds', 1800,
    )
    s.creator_bot_cooldown_max_seconds = overrides.get(
        'creator_bot_cooldown_max_seconds', 3600,
    )
    s.creator_claim_ttl_seconds = overrides.get(
        'creator_claim_ttl_seconds', 600,
    )
    s.creator_queue_idle_poll_seconds = overrides.get(
        'creator_queue_idle_poll_seconds', 30,
    )
    s.creator_orphan_recovery_interval_seconds = overrides.get(
        'creator_orphan_recovery_interval_seconds', 600,
    )
    s.creator_video_ref_count = overrides.get(
        'creator_video_ref_count', 0,
    )
    s.creator_playlist_ref_count = overrides.get(
        'creator_playlist_ref_count', 0,
    )
    s.worker_id = overrides.get('worker_id', '0')
    s.creator_short_url_resolve_timeout_seconds = overrides.get(
        'creator_short_url_resolve_timeout_seconds', 10,
    )
    s.creator_short_url_retry_interval_seconds = overrides.get(
        'creator_short_url_retry_interval_seconds', 300,
    )
    s.creator_short_url_user_agent = overrides.get(
        'creator_short_url_user_agent', 'ua',
    )
    s.creator_priority_queues = overrides.get(
        'creator_priority_queues',
        '168:1000000,336:100000,720:10000,4320:0',
    )
    return s


# ---------------------------------------------------------------
# _validate_settings
# ---------------------------------------------------------------

class TestCreatorSettings(unittest.TestCase):

    def test_generic_log_flags_feed_creator_logging(self) -> None:
        s = tool.CreatorSettings(
            _env_file=None,
            _cli_parse_args=[
                '--log-file', '/dev/stdout',
                '--log-level', 'debug',
            ],
        )
        self.assertEqual(s.creator_log_file, '/dev/stdout')
        self.assertEqual(s.creator_log_level, 'DEBUG')

    def test_creator_concurrency_defaults_to_auto(self) -> None:
        s = tool.CreatorSettings(
            _env_file=None,
            _cli_parse_args=[],
        )
        self.assertEqual(s.creator_concurrency, 0)

    def test_auto_creator_concurrency_uses_proxy_process_ratio(
        self,
    ) -> None:
        self.assertEqual(tool._auto_creator_concurrency(18, 1), 18)
        self.assertEqual(tool._auto_creator_concurrency(18, 2), 18)
        self.assertEqual(tool._auto_creator_concurrency(18, 4), 18)
        self.assertEqual(tool._auto_creator_concurrency(0, 4), 1)

    def test_explicit_creator_concurrency_wins(self) -> None:
        s: MagicMock = _settings(
            creator_concurrency=3,
            creator_num_processes=9,
        )
        self.assertEqual(
            tool._resolve_creator_concurrency(s, proxy_count=18),
            3,
        )


class TestMain(unittest.TestCase):

    def test_generic_log_flags_pass_to_runner(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            argv: list[str] = [
                'tt_creator_scrape.py',
                '--redis-dsn', 'redis://h:6379/0',
                '--tiktok-creator-data-dir', d,
                '--log-file', '/dev/stdout',
                '--log-level', 'debug',
            ]
            with patch.object(tool.sys, 'argv', argv), patch.object(
                tool, 'ScraperRunner',
            ) as runner_cls:
                runner_cls.return_value.run_sync.return_value = 0
                with self.assertRaises(SystemExit) as cm:
                    tool.main()
            self.assertEqual(cm.exception.code, 0)
            self.assertEqual(
                runner_cls.call_args.kwargs['log_file'],
                '/dev/stdout',
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['log_level'],
                'DEBUG',
            )
            self.assertTrue(
                runner_cls.call_args.kwargs['split_proxy_pool'],
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency'],
                1,
            )

    def test_main_auto_concurrency_uses_proxy_count(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            proxy_file: str = os.path.join(d, 'proxies.txt')
            with open(proxy_file, 'w') as f:
                f.write(
                    'http://p1:8080\n'
                    'http://p2:8080\n'
                    'http://p3:8080\n'
                )
            argv: list[str] = [
                'tt_creator_scrape.py',
                '--redis-dsn', 'redis://h:6379/0',
                '--tiktok-creator-data-dir', d,
            ]
            env: dict[str, str] = {
                'PROXY_FILES': proxy_file,
            }
            with patch.dict(os.environ, env, clear=True), patch.object(
                tool.sys, 'argv', argv,
            ), patch.object(
                tool, 'ScraperRunner',
            ) as runner_cls:
                runner_cls.return_value.run_sync.return_value = 0
                with self.assertRaises(SystemExit) as cm:
                    tool.main()
                self.assertEqual(
                    os.environ['TIKTOK_CREATOR_CONCURRENCY'],
                    '3',
                )
            self.assertEqual(cm.exception.code, 0)
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency'],
                3,
            )

    def test_main_explicit_concurrency_is_fleet_wide_cap(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            proxy_file: str = os.path.join(d, 'proxies.txt')
            with open(proxy_file, 'w') as f:
                f.write('\n'.join(
                    f'http://p{i}:8080' for i in range(1, 11)
                ))
            argv: list[str] = [
                'tt_creator_scrape.py',
                '--redis-dsn', 'redis://h:6379/0',
                '--tiktok-creator-data-dir', d,
            ]
            env: dict[str, str] = {
                'PROXY_FILES': proxy_file,
                'TIKTOK_CREATOR_NUM_PROCESSES': '4',
                'TIKTOK_CREATOR_CONCURRENCY': '5',
            }
            sampled: list[str] = [
                'http://p10:8080', 'http://p3:8080',
                'http://p8:8080', 'http://p1:8080',
                'http://p6:8080',
            ]
            with patch.dict(os.environ, env, clear=True), patch.object(
                tool.sys, 'argv', argv,
            ), patch.object(
                tool, 'random_proxy_subset', return_value=sampled,
            ) as proxy_subset, patch.object(
                tool, 'ScraperRunner',
            ) as runner_cls:
                runner_cls.return_value.run_sync.return_value = 0
                with self.assertRaises(SystemExit):
                    tool.main()

            proxy_subset.assert_called_once()
            self.assertEqual(
                runner_cls.call_args.kwargs['settings'].proxies,
                sampled,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['num_processes'],
                4,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency'],
                5,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['child_concurrencies'],
                [2, 1, 1, 1],
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency_env_var'],
                'TIKTOK_CREATOR_CONCURRENCY',
            )


class TestValidateSettings(unittest.TestCase):

    def test_missing_redis_exits(self) -> None:
        s: MagicMock = _settings(redis_dsn='')
        with self.assertRaises(SystemExit):
            tool._validate_settings(s)

    def test_missing_data_dir_exits(self) -> None:
        s: MagicMock = _settings(creator_data_directory='')
        with self.assertRaises(SystemExit):
            tool._validate_settings(s)

    def test_valid_creates_missing_dir(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            data_dir: str = os.path.join(d, 'data')
            s: MagicMock = _settings(
                creator_data_directory=data_dir,
            )
            tool._validate_settings(s)
            self.assertTrue(os.path.isdir(data_dir))


# ---------------------------------------------------------------
# _handle_failure
# ---------------------------------------------------------------

class TestHandleFailure(unittest.IsolatedAsyncioTestCase):

    def _queue(self) -> MagicMock:
        q: MagicMock = MagicMock()
        q.release = AsyncMock()
        q.remove = AsyncMock()
        return q

    async def test_transient_releases_with_retry(self) -> None:
        q: MagicMock = self._queue()
        s: MagicMock = _settings()
        reason: str = await tool._handle_failure(
            TimeoutError('navigation timeout'), 'u', q, s,
        )
        self.assertEqual(reason, 'transient')
        q.release.assert_awaited_once()
        self.assertEqual(
            q.release.call_args.kwargs['retry_interval_seconds'],
            300,
        )
        q.remove.assert_not_awaited()

    async def test_rate_limit_releases_with_long_retry(self) -> None:
        q: MagicMock = self._queue()
        s: MagicMock = _settings(
            creator_rate_limit_retry_interval_seconds=1800,
        )
        reason: str = await tool._handle_failure(
            RuntimeError('TikTok bot detection: empty response body'),
            'u', q, s,
        )
        self.assertEqual(reason, 'rate_limit')
        q.release.assert_awaited_once()
        self.assertEqual(
            q.release.call_args.kwargs['retry_interval_seconds'],
            1800,
        )

    async def test_unavailable_removes(self) -> None:
        q: MagicMock = self._queue()
        s: MagicMock = _settings()
        reason: str = await tool._handle_failure(
            RuntimeError('user not found'), 'u', q, s,
        )
        self.assertEqual(reason, 'unavailable')
        q.remove.assert_awaited_once_with('u')
        q.release.assert_not_awaited()

    async def test_other_releases_normally(self) -> None:
        q: MagicMock = self._queue()
        s: MagicMock = _settings()
        reason: str = await tool._handle_failure(
            RuntimeError('weird kaboom'), 'u', q, s,
        )
        self.assertEqual(reason, 'other')
        q.release.assert_awaited_once_with('u')
        self.assertNotIn(
            'retry_interval_seconds', q.release.call_args.kwargs,
        )


# ---------------------------------------------------------------
# _scrape_one
# ---------------------------------------------------------------

class TestScrapeOne(unittest.IsolatedAsyncioTestCase):

    async def test_rejected_user_response_is_rate_limit(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.as_dict = {
            'statusCode': 10201,
            'status_code': 10201,
        }
        user_obj.info = AsyncMock(side_effect=KeyError('user'))
        api.user = MagicMock(return_value=user_obj)

        with self.assertRaises(tool.TikTokProfileResponseError) as ctx:
            await tool._scrape_one(
                api, 'therock', _settings(),
            )

        self.assertIn('status=10201', str(ctx.exception))
        self.assertEqual(
            tool.classify_tiktok_error(ctx.exception),
            'rate_limit',
        )

    async def test_empty_user_info_is_rate_limit(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.as_dict = {
            'userInfo': {
                'user': {}, 'stats': {}, 'shareMeta': {},
            },
        }
        user_obj.info = AsyncMock(side_effect=KeyError('id'))
        api.user = MagicMock(return_value=user_obj)

        with self.assertRaises(tool.TikTokProfileResponseError) as ctx:
            await tool._scrape_one(
                api, 'mrbeast', _settings(),
            )

        self.assertIn("missing='id'", str(ctx.exception))
        self.assertEqual(
            tool.classify_tiktok_error(ctx.exception),
            'rate_limit',
        )

    async def test_mismatched_creator_identity_is_transient(
        self,
    ) -> None:
        api: MagicMock = _api_returning(_raw_user('finneas'))

        with self.assertRaises(
            tool.TikTokProfileIdentityError,
        ) as ctx:
            await tool._scrape_one(
                api, 'fifaworldcup', _settings(),
            )

        self.assertIn("requested='fifaworldcup'", str(ctx.exception))
        self.assertIn("returned='finneas'", str(ctx.exception))
        self.assertEqual(
            tool.classify_tiktok_error(ctx.exception),
            'transient',
        )

    async def test_missing_reported_videos_is_transient(self) -> None:
        api: MagicMock = _api_returning(
            _raw_user('finneas', video_count=348),
        )

        with self.assertRaises(
            tool.TikTokVideoListResponseError,
        ) as ctx:
            await tool._scrape_one(
                api, 'finneas', _settings(),
            )

        self.assertIn('profile_video_count=348', str(ctx.exception))
        self.assertEqual(
            tool.classify_tiktok_error(ctx.exception),
            'transient',
        )

    async def test_collects_video_repost_and_liked_refs(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.info = AsyncMock(
            return_value=_raw_user(
                'charli', followers=42, sec_uid='SEC9',
            ),
        )
        user_obj.videos = MagicMock(
            return_value=_async_items([
                _video_item('111', 'charli'),
            ]),
        )
        user_obj.liked = MagicMock(
            return_value=_async_items([
                _video_item('333', 'other'),
            ]),
        )
        user_obj.playlists = MagicMock(
            return_value=_async_items([
                _playlist_item('444', 'Dance series', 12),
            ]),
        )
        api.user = MagicMock(return_value=user_obj)
        api.make_request = AsyncMock(
            return_value={
                'itemList': [
                    {
                        'id': '222',
                        'author': {'uniqueId': 'reposter'},
                    },
                ],
                'hasMore': False,
            },
        )
        creator = await tool._scrape_one(
            api, 'charli', _settings(
                creator_video_ref_count=5,
                creator_playlist_ref_count=5,
            ),
        )
        self.assertEqual(
            creator.videos[0].url,
            'https://www.tiktok.com/@charli/video/111',
        )
        self.assertEqual(creator.videos[0].username, 'charli')
        self.assertEqual(creator.reposts[0].video_id, '222')
        self.assertEqual(creator.reposts[0].username, 'reposter')
        self.assertEqual(
            creator.liked[0].url,
            'https://www.tiktok.com/@other/video/333',
        )
        self.assertEqual(creator.liked[0].username, 'other')
        self.assertEqual(creator.playlists[0].playlist_id, '444')
        self.assertEqual(creator.playlists[0].name, 'Dance series')
        self.assertEqual(creator.playlists[0].video_count, 12)
        api.make_request.assert_awaited_once()

    async def test_zero_video_ref_count_means_unlimited(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.info = AsyncMock(
            return_value=_raw_user(
                'charli', followers=42, sec_uid='SEC9',
            ),
        )
        user_obj.videos = MagicMock(
            return_value=_async_items([
                _video_item('111', 'charli'),
                _video_item('112', 'charli'),
            ]),
        )
        user_obj.liked = MagicMock(
            return_value=_async_items([
                _video_item('333', 'other'),
                _video_item('334', 'other'),
            ]),
        )
        user_obj.playlists = MagicMock(return_value=_async_items([]))
        api.user = MagicMock(return_value=user_obj)
        api.make_request = AsyncMock(side_effect=[
            {
                'itemList': [
                    {
                        'id': '221',
                        'author': {'uniqueId': 'reposter'},
                    },
                ],
                'hasMore': True,
                'cursor': 35,
            },
            {
                'itemList': [
                    {
                        'id': '222',
                        'author': {'uniqueId': 'reposter'},
                    },
                ],
                'hasMore': False,
                'cursor': 70,
            },
        ])
        creator = await tool._scrape_one(
            api, 'charli', _settings(
                creator_video_ref_count=0,
                creator_playlist_ref_count=0,
            ),
        )
        self.assertEqual(
            [v.video_id for v in creator.videos],
            ['111', '112'],
        )
        self.assertEqual(
            [v.video_id for v in creator.liked],
            ['333', '334'],
        )
        self.assertEqual(
            [v.video_id for v in creator.reposts],
            ['221', '222'],
        )
        self.assertEqual(
            user_obj.videos.call_args.kwargs['count'],
            tool.sys.maxsize,
        )
        self.assertEqual(
            user_obj.liked.call_args.kwargs['count'],
            tool.sys.maxsize,
        )
        self.assertEqual(api.make_request.await_count, 2)

    async def test_liked_failure_is_non_fatal(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.info = AsyncMock(
            return_value=_raw_user('charli', sec_uid='SEC9'),
        )
        user_obj.videos = MagicMock(
            return_value=_async_items([
                _video_item('111', 'charli'),
            ]),
        )

        async def _bad_liked() -> object:
            raise RuntimeError('liked list is private')
            yield None

        user_obj.liked = MagicMock(return_value=_bad_liked())
        user_obj.playlists = MagicMock(return_value=_async_items([]))
        api.user = MagicMock(return_value=user_obj)
        api.make_request = AsyncMock(
            return_value={'itemList': [], 'hasMore': False},
        )
        creator = await tool._scrape_one(
            api, 'charli', _settings(
                creator_video_ref_count=5,
                creator_playlist_ref_count=5,
            ),
        )
        self.assertEqual(len(creator.videos), 1)
        self.assertEqual(creator.liked, [])

    async def test_private_account_skips_collections(self) -> None:
        resp: dict = _raw_user('private', sec_uid='SEC9')
        resp['userInfo']['user']['privateAccount'] = True
        api: MagicMock = _api_returning(resp)
        creator = await tool._scrape_one(
            api, 'private', _settings(creator_video_ref_count=5),
        )
        self.assertTrue(creator.private_account)
        self.assertEqual(creator.videos, [])
        api.make_request.assert_not_awaited()


# ---------------------------------------------------------------
# identity maps
# ---------------------------------------------------------------

class TestCreatorIdentityMaps(unittest.IsolatedAsyncioTestCase):

    async def test_persist_identity_writes_redis_maps(self) -> None:
        creator = tool.TikTokCreator.from_user_info(
            _raw_user(
                'charli',
                sec_uid='SEC9',
                user_id='12345',
            ),
        )
        maps = tool.CreatorIdentityMaps(
            user_id_to_username=MagicMock(),
            username_to_user_id=MagicMock(),
            nickname_to_sec_uid=MagicMock(),
        )
        maps.user_id_to_username.put = AsyncMock()
        maps.username_to_user_id.put = AsyncMock()
        maps.nickname_to_sec_uid.put = AsyncMock()
        await tool._persist_creator_identity(creator, maps)
        maps.user_id_to_username.put.assert_awaited_once_with(
            '12345', 'charli',
        )
        maps.username_to_user_id.put.assert_awaited_once_with(
            'charli', '12345',
        )
        maps.nickname_to_sec_uid.put.assert_awaited_once_with(
            'Nick', 'SEC9',
        )

    async def test_persist_identity_skips_missing_nickname(
        self,
    ) -> None:
        resp: dict = _raw_user('charli', sec_uid='SEC9')
        resp['userInfo']['user'].pop('nickname')
        creator = tool.TikTokCreator.from_user_info(resp)
        maps = tool.CreatorIdentityMaps(
            user_id_to_username=MagicMock(),
            username_to_user_id=MagicMock(),
            nickname_to_sec_uid=MagicMock(),
        )
        maps.user_id_to_username.put = AsyncMock()
        maps.username_to_user_id.put = AsyncMock()
        maps.nickname_to_sec_uid.put = AsyncMock()
        await tool._persist_creator_identity(creator, maps)
        maps.nickname_to_sec_uid.put.assert_not_awaited()


class TestBuildCreatorIdentityMaps(unittest.TestCase):

    def test_redis_builds_three_maps(self) -> None:
        s: MagicMock = _settings(redis_dsn='redis://h:6379/0')
        creator_map: MagicMock = MagicMock()
        creator_map.redis_client = MagicMock()
        with patch.object(
            tool, 'RedisCreatorMap', return_value=creator_map,
        ) as cm, patch.object(
            tool, 'RedisHandleMap',
        ) as hm, patch.object(
            tool, 'RedisNameMap',
        ) as nm:
            maps = tool._build_creator_identity_maps(s)
        self.assertIsNotNone(maps)
        cm.assert_called_once_with(
            'redis://h:6379/0', platform='tiktok',
        )
        hm.assert_called_once_with(
            creator_map.redis_client, platform='tiktok',
        )
        nm.assert_called_once_with(
            'redis://h:6379/0', platform='tiktok',
        )


# ---------------------------------------------------------------
# _process_creator
# ---------------------------------------------------------------

class TestProcessCreator(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fm: AssetFileManagement = AssetFileManagement(
            self._tmp.name,
            prefix_rankings={'creator': [tool.CREATOR_FILE_PREFIX]},
        )

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    def _queue(self) -> MagicMock:
        q: MagicMock = MagicMock()
        q.release = AsyncMock()
        q.remove = AsyncMock()
        q.update_tier = AsyncMock()
        return q

    def _identity_maps(self) -> MagicMock:
        maps: MagicMock = MagicMock()
        maps.user_id_to_username.put = AsyncMock()
        maps.username_to_user_id.put = AsyncMock()
        maps.nickname_to_sec_uid.put = AsyncMock()
        return maps

    def _video_queue(self) -> MagicMock:
        q: MagicMock = MagicMock()
        q.enqueue = AsyncMock(return_value=True)
        return q

    async def test_success_writes_record_maps_and_retiers(
        self,
    ) -> None:
        api: MagicMock = _api_returning(
            _raw_user('charli', followers=42, sec_uid='S9'),
        )
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(api),
        )
        q: MagicMock = self._queue()
        video_q: MagicMock = self._video_queue()
        maps: MagicMock = self._identity_maps()
        s: MagicMock = _settings()
        await tool._process_creator(
            'charli', 'http://p:8080', pool, q, video_q, self.fm, s,
            '0', 'p', maps,
        )
        # record on disk
        written: dict = await self.fm.read_file(
            tool._creator_filename('charli'),
        )
        self.assertEqual(written['username'], 'charli')
        self.assertEqual(written['follower_count'], 42)
        maps.user_id_to_username.put.assert_awaited_once_with(
            '123', 'charli',
        )
        maps.username_to_user_id.put.assert_awaited_once_with(
            'charli', '123',
        )
        maps.nickname_to_sec_uid.put.assert_awaited_once_with(
            'Nick', 'S9',
        )
        # re-tier by measured followers, then release
        q.update_tier.assert_awaited_once_with('charli', 42)
        q.release.assert_awaited_once_with('charli')
        video_q.enqueue.assert_not_awaited()

    async def test_success_enqueues_discovered_video_urls(
        self,
    ) -> None:
        api: MagicMock = _api_returning(
            _raw_user('charli', followers=42, sec_uid='S9'),
        )
        user_obj: MagicMock = api.user.return_value
        user_obj.videos = MagicMock(
            return_value=_async_items([
                _video_item('v1', 'charli'),
                _video_item('v2', 'charli'),
            ]),
        )
        user_obj.liked = MagicMock(
            return_value=_async_items([
                _video_item('v2', 'charli'),
                _video_item('v3', 'charli'),
            ]),
        )
        api.make_request = AsyncMock(
            return_value={
                'itemList': [
                    {'id': 'v3', 'author': {'uniqueId': 'charli'}},
                    {'id': 'v4', 'author': {'uniqueId': 'charli'}},
                ],
                'hasMore': False,
            },
        )
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(api),
        )
        q: MagicMock = self._queue()
        video_q: MagicMock = self._video_queue()
        s: MagicMock = _settings(creator_video_ref_count=10)

        await tool._process_creator(
            'charli', 'http://p:8080', pool, q, video_q, self.fm, s,
            '0', 'p', self._identity_maps(),
        )

        self.assertEqual(
            video_q.enqueue.await_args_list,
            [
                unittest.mock.call(
                    'https://www.tiktok.com/@charli/video/v1',
                    source='tiktok_creator',
                ),
                unittest.mock.call(
                    'https://www.tiktok.com/@charli/video/v2',
                    source='tiktok_creator',
                ),
                unittest.mock.call(
                    'https://www.tiktok.com/@charli/video/v3',
                    source='tiktok_creator',
                ),
                unittest.mock.call(
                    'https://www.tiktok.com/@charli/video/v4',
                    source='tiktok_creator',
                ),
            ],
        )
        q.update_tier.assert_awaited_once_with('charli', 42)
        q.release.assert_awaited_once_with('charli')

    async def test_video_enqueue_failure_does_not_fail_creator(
        self,
    ) -> None:
        api: MagicMock = _api_returning(
            _raw_user('charli', followers=42, sec_uid='S9'),
        )
        api.user.return_value.videos = MagicMock(
            return_value=_async_items([_video_item('v1', 'charli')]),
        )
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(api),
        )
        q: MagicMock = self._queue()
        video_q: MagicMock = self._video_queue()
        video_q.enqueue = AsyncMock(side_effect=RuntimeError('redis'))
        s: MagicMock = _settings(creator_video_ref_count=10)

        await tool._process_creator(
            'charli', 'http://p:8080', pool, q, video_q, self.fm, s,
            '0', 'p', self._identity_maps(),
        )

        video_q.enqueue.assert_awaited_once_with(
            'https://www.tiktok.com/@charli/video/v1',
            source='tiktok_creator',
        )
        q.update_tier.assert_awaited_once_with('charli', 42)
        q.release.assert_awaited_once_with('charli')

    async def test_failure_routes_to_handle_failure(self) -> None:
        api: MagicMock = MagicMock()
        user_obj: MagicMock = MagicMock()
        user_obj.info = AsyncMock(
            side_effect=RuntimeError('user not found'),
        )
        api.user = MagicMock(return_value=user_obj)
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(api),
        )
        q: MagicMock = self._queue()
        video_q: MagicMock = self._video_queue()
        s: MagicMock = _settings()
        await tool._process_creator(
            'ghost', 'http://p:8080', pool, q, video_q, self.fm, s,
            '0', 'p', self._identity_maps(),
        )
        q.remove.assert_awaited_once_with('ghost')
        q.update_tier.assert_not_awaited()


# ---------------------------------------------------------------
# _proxy_worker
# ---------------------------------------------------------------

class TestProxyWorker(unittest.IsolatedAsyncioTestCase):

    async def test_short_url_resolution_is_rate_limited(self) -> None:
        short_url = 'https://vm.tiktok.com/ZM123/'
        q = MagicMock()
        q.claim_batch = AsyncMock(return_value=[
            (short_url, short_url, 0.0),
        ])
        pool = MagicMock()
        pool.gate_api_request = AsyncMock()
        shutdown = asyncio.Event()

        async def _resolve(*_args: object) -> None:
            shutdown.set()

        with patch.object(tool, 'Watchdog'), patch.object(
            tool, '_resolve_and_enqueue_short_url', _resolve,
        ):
            await tool._proxy_worker(
                'http://p:8080', pool, q, MagicMock(),
                MagicMock(), _settings(), shutdown, '0', MagicMock(),
            )

        pool.gate_api_request.assert_awaited_once_with(
            'http://p:8080',
        )

    async def test_rebuilds_after_consecutive_bot_responses(self) -> None:
        q: MagicMock = MagicMock()
        q.claim_batch = AsyncMock(side_effect=[
            [('a', 'a', 0.0)],
            [('b', 'b', 0.0)],
            [('c', 'c', 0.0)],
        ])
        pool = MagicMock()
        pool.quarantine = AsyncMock()
        pool.rebuild = AsyncMock(return_value=True)
        s = _settings(
            creator_bot_failure_threshold=3,
            creator_bot_cooldown_seconds=60,
        )
        shutdown = asyncio.Event()

        async def _fake_process(*_args: object) -> str:
            return 'rate_limit'

        async def _sleep(seconds: float) -> None:
            self.assertEqual(seconds, 60)
            shutdown.set()

        with patch.object(tool, 'Watchdog'), patch.object(
            tool, '_process_creator', _fake_process,
        ), patch.object(tool.asyncio, 'sleep', _sleep):
            await tool._proxy_worker(
                'http://p:8080', pool, q, MagicMock(),
                MagicMock(), s, shutdown, '0', MagicMock(),
            )

        pool.quarantine.assert_awaited_once_with('http://p:8080')
        pool.rebuild.assert_awaited_once_with('http://p:8080')

    async def test_success_resets_bot_failure_count(self) -> None:
        q: MagicMock = MagicMock()
        q.claim_batch = AsyncMock(side_effect=[
            [('a', 'a', 0.0)], [('ok', 'ok', 0.0)],
            [('b', 'b', 0.0)], [('c', 'c', 0.0)],
            [('d', 'd', 0.0)],
        ])
        pool = MagicMock()
        pool.quarantine = AsyncMock()
        pool.rebuild = AsyncMock(return_value=True)
        s = _settings(
            creator_bot_failure_threshold=3,
            creator_bot_cooldown_seconds=60,
        )
        shutdown = asyncio.Event()
        outcomes = iter([
            'rate_limit', None, 'rate_limit', 'rate_limit', 'rate_limit',
        ])

        async def _fake_process(*_args: object) -> str | None:
            return next(outcomes)

        async def _sleep(_seconds: float) -> None:
            shutdown.set()

        with patch.object(tool, 'Watchdog'), patch.object(
            tool, '_process_creator', _fake_process,
        ), patch.object(tool.asyncio, 'sleep', _sleep):
            await tool._proxy_worker(
                'http://p:8080', pool, q, MagicMock(),
                MagicMock(), s, shutdown, '0', MagicMock(),
            )

        self.assertEqual(q.claim_batch.await_count, 5)
        pool.quarantine.assert_awaited_once()
        pool.rebuild.assert_awaited_once()

    async def test_bot_cooldown_backs_off_after_repeated_circuits(
        self,
    ) -> None:
        q: MagicMock = MagicMock()
        q.claim_batch = AsyncMock(side_effect=[
            [('a', 'a', 0.0)],
            [('b', 'b', 0.0)],
        ])
        pool = MagicMock()
        pool.quarantine = AsyncMock()
        pool.rebuild = AsyncMock(return_value=True)
        s = _settings(
            creator_bot_failure_threshold=1,
            creator_bot_cooldown_seconds=60,
            creator_bot_cooldown_max_seconds=120,
        )
        shutdown = asyncio.Event()
        sleeps: list[float] = []

        async def _fake_process(*_args: object) -> str:
            return 'rate_limit'

        async def _sleep(seconds: float) -> None:
            sleeps.append(seconds)
            if len(sleeps) == 2:
                shutdown.set()

        with patch.object(tool, 'Watchdog'), patch.object(
            tool, '_process_creator', _fake_process,
        ), patch.object(tool.asyncio, 'sleep', _sleep):
            await tool._proxy_worker(
                'http://p:8080', pool, q, MagicMock(),
                MagicMock(), s, shutdown, '0', MagicMock(),
            )

        self.assertEqual(sleeps, [60, 120])
        self.assertEqual(pool.quarantine.await_count, 2)
        self.assertEqual(pool.rebuild.await_count, 2)

    async def test_idle_sleeps_and_touches_watchdog(self) -> None:
        q: MagicMock = MagicMock()
        q.claim_batch = AsyncMock(return_value=[])
        q.next_due_time = AsyncMock(return_value=None)
        s: MagicMock = _settings()
        shutdown: asyncio.Event = asyncio.Event()

        async def _stop(_seconds: float) -> None:
            shutdown.set()

        with patch.object(tool, 'Watchdog') as wd, patch.object(
            tool.asyncio, 'sleep', _stop,
        ):
            await tool._proxy_worker(
                'http://p:8080', MagicMock(), q, MagicMock(),
                MagicMock(), s, shutdown, '0', MagicMock(),
            )
        q.claim_batch.assert_awaited()
        wd.get.return_value.touch_work.assert_called()

    async def test_claims_and_processes_one(self) -> None:
        q: MagicMock = MagicMock()
        q.claim_batch = AsyncMock(
            return_value=[('alice', 'alice', 0.0)],
        )
        s: MagicMock = _settings()
        shutdown: asyncio.Event = asyncio.Event()
        seen: list[str] = []

        async def _fake_process(username: str, *a: object) -> None:
            seen.append(username)
            shutdown.set()

        with patch.object(tool, 'Watchdog'), patch.object(
            tool, '_process_creator', _fake_process,
        ):
            await tool._proxy_worker(
                'http://p:8080', MagicMock(), q, MagicMock(),
                MagicMock(), s, shutdown, '0', MagicMock(),
            )
        self.assertEqual(seen, ['alice'])


# ---------------------------------------------------------------
# _build_queue
# ---------------------------------------------------------------

class TestBuildQueue(unittest.TestCase):

    def test_builds_redis_backend(self) -> None:
        s: MagicMock = _settings(redis_dsn='redis://h:6379/0')
        tiers: list[tool.TierConfig] = [
            tool.TierConfig(
                tier=1, min_subscribers=0, interval_hours=24,
            ),
        ]
        queue: MagicMock = MagicMock()
        queue._build_queue_keys.return_value = ['q1']
        with patch.object(
            tool, 'RedisCreatorQueue', return_value=queue,
        ) as rq:
            result = tool._build_queue(s, 'w1', tiers)
        rq.assert_called_once_with(
            'redis://h:6379/0', 'w1', 'tiktok',
            key_namespace='scrape',
        )
        self.assertIs(result, queue)
        self.assertEqual(queue._tiers, tiers)
        self.assertEqual(queue._key_queues, ['q1'])


# ---------------------------------------------------------------
# _run_worker topology
# ---------------------------------------------------------------

class TestRunWorkerTopology(unittest.IsolatedAsyncioTestCase):

    async def test_one_worker_per_ready_proxy(self) -> None:
        proxies: list[str] = [
            'http://p1:8080', 'http://p2:8080',
        ]
        pool: MagicMock = MagicMock()
        pool.bootstrap = AsyncMock()
        pool.shutdown = AsyncMock()
        pool.run_refresh_loop = AsyncMock()
        pool.ready_proxies = MagicMock(return_value=proxies)

        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(creator_concurrency=len(proxies))
        ctx.settings.ms_token_ttl_seconds = 14400
        ctx.settings.ms_token_refresh_fraction = 0.75
        ctx.settings.ms_token_refresh_interval_seconds = 300
        ctx.settings.session_bootstrap_timeout_ms = 120000
        ctx.settings.creator_priority_queues = (
            '6:1000000,24:100000,72:10000,168:0'
        )
        ctx.proxies = proxies
        ctx.rate_limiter = MagicMock()

        worker_calls: list[str] = []

        async def _fake_worker(proxy: str, *a: object) -> None:
            worker_calls.append(proxy)

        async def _noop_loop(*a: object) -> None:
            await asyncio.sleep(0)

        with patch.object(
            tool, 'TikTokSessionPool', return_value=pool,
        ) as pool_cls, patch.object(
            tool, '_build_queue', return_value=MagicMock(),
        ), patch.object(
            tool, '_build_creator_identity_maps',
            return_value=MagicMock(),
        ), patch.object(
            tool, '_proxy_worker', _fake_worker,
        ), patch.object(
            tool, '_maintenance_loop', _noop_loop,
        ), patch.object(
            tool, 'AssetFileManagement',
        ):
            await tool._run_worker(ctx)

        self.assertEqual(worker_calls, proxies)
        self.assertEqual(
            pool_cls.call_args.kwargs['bootstrap_timeout_ms'],
            120000,
        )
        pool.bootstrap.assert_awaited_once()
        pool.shutdown.assert_awaited_once()

    async def test_creator_concurrency_caps_ready_proxy_workers(
        self,
    ) -> None:
        proxies: list[str] = [
            'http://p1:8080', 'http://p2:8080', 'http://p3:8080',
        ]
        pool: MagicMock = MagicMock()
        pool.bootstrap = AsyncMock()
        pool.shutdown = AsyncMock()
        pool.run_refresh_loop = AsyncMock()
        pool.ready_proxies = MagicMock(return_value=proxies)

        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(creator_concurrency=2)
        ctx.settings.ms_token_ttl_seconds = 14400
        ctx.settings.ms_token_refresh_fraction = 0.75
        ctx.settings.ms_token_refresh_interval_seconds = 300
        ctx.settings.session_bootstrap_timeout_ms = 120000
        ctx.proxies = proxies
        ctx.rate_limiter = MagicMock()

        worker_calls: list[str] = []

        async def _fake_worker(proxy: str, *a: object) -> None:
            worker_calls.append(proxy)

        async def _noop_loop(*a: object) -> None:
            await asyncio.sleep(0)

        with patch.object(
            tool, 'TikTokSessionPool', return_value=pool,
        ) as pool_cls, patch.object(
            tool, '_build_queue', return_value=MagicMock(),
        ), patch.object(
            tool, '_build_creator_identity_maps',
            return_value=MagicMock(),
        ), patch.object(
            tool, '_proxy_worker', _fake_worker,
        ), patch.object(
            tool, '_maintenance_loop', _noop_loop,
        ), patch.object(
            tool, 'AssetFileManagement',
        ), patch.object(tool._LOGGER, 'info') as log_info:
            await tool._run_worker(ctx)

        self.assertEqual(worker_calls, proxies[:2])
        self.assertEqual(
            pool_cls.call_args.kwargs['proxies'],
            proxies[:2],
        )
        selection_log = next(
            call for call in log_info.call_args_list
            if call.args[0]
            == 'TikTok creator active proxy selection complete'
        )
        self.assertEqual(
            selection_log.kwargs['extra']['active_proxy_endpoints'],
            ['p1:8080', 'p2:8080'],
        )
        self.assertEqual(
            selection_log.kwargs['extra']['idle_ready_proxy_count'],
            1,
        )
        self.assertEqual(
            selection_log.kwargs['extra']['configured_concurrency'],
            2,
        )


# ---------------------------------------------------------------
# _maintenance_loop
# ---------------------------------------------------------------

class TestMaintenanceLoop(unittest.IsolatedAsyncioTestCase):

    async def test_recovers_claims_and_publishes_sizes(
        self,
    ) -> None:
        q: MagicMock = MagicMock()
        q.cleanup_stale_claims = AsyncMock(return_value=0)
        q.scan_and_recover_orphans = AsyncMock(
            return_value={
                1: {
                    'queued': 2, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 1,
                },
                2: {
                    'queued': 3, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 0,
                },
            },
        )
        q.queue_sizes_by_tier = AsyncMock(
            return_value={1: 2, 2: 3},
        )
        s: MagicMock = _settings()
        shutdown: asyncio.Event = asyncio.Event()

        async def _stop(_seconds: float) -> None:
            shutdown.set()

        with patch.object(tool, 'Watchdog') as wd, patch.object(
            tool.asyncio, 'sleep', _stop,
        ):
            await tool._maintenance_loop(q, s, '0', shutdown)

        wd.get.return_value.touch_work.assert_called()
        q.scan_and_recover_orphans.assert_awaited_once_with(
            recover=True,
        )
        q.cleanup_stale_claims.assert_not_awaited()
        self.assertEqual(
            tool.METRIC_SCRAPE_QUEUE_SIZE.labels(
                platform='tiktok', scraper='tiktok_creator',
                entity='creator', state='queued', worker_id='0',
            )._value.get(),
            5,
        )


# ---------------------------------------------------------------
# _raise_fd_limit
# ---------------------------------------------------------------

class TestRaiseFdLimit(unittest.TestCase):

    def test_raises_soft_to_hard(self) -> None:
        with patch.object(tool, 'resource') as res:
            res.RLIM_INFINITY = -1
            res.RLIMIT_NOFILE = 7
            res.getrlimit.return_value = (1024, 4096)
            tool._raise_fd_limit()
        res.setrlimit.assert_called_once_with(7, (4096, 4096))

    def test_unbounded_hard_uses_target(self) -> None:
        with patch.object(tool, 'resource') as res:
            res.RLIM_INFINITY = -1
            res.RLIMIT_NOFILE = 7
            res.getrlimit.return_value = (1024, -1)
            tool._raise_fd_limit()
        res.setrlimit.assert_called_once_with(
            7, (tool._FD_TARGET, -1),
        )


# ---------------------------------------------------------------
# metrics
# ---------------------------------------------------------------

class TestMetrics(unittest.TestCase):

    def test_generic_lifecycle_metrics_exist(self) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SCRAPE_QUEUE_ENQUEUES,
            METRIC_SCRAPE_QUEUE_SIZE,
            METRIC_SCRAPE_RECORDS_WRITTEN,
            METRIC_SCRAPE_RETRIES,
        )
        METRIC_SCRAPE_QUEUE_SIZE.labels(
            platform='tiktok', scraper='tiktok_creator',
            entity='creator', state='queued', worker_id='0',
        ).set(3)
        METRIC_SCRAPE_QUEUE_ENQUEUES.labels(
            platform='tiktok', scraper='tiktok_creator',
            entity='video', source='tiktok_creator',
        ).inc()
        METRIC_SCRAPE_RETRIES.labels(
            platform='tiktok', scraper='tiktok_creator',
            entity='creator', api='tiktokapi', reason='transient',
        ).inc()
        METRIC_SCRAPE_RECORDS_WRITTEN.labels(
            platform='tiktok', scraper='tiktok_creator',
            entity='creator',
        ).inc()


from scrape_exchange.tiktok.short_url import (
    ShortUrlOutcome,
    ShortUrlResolution,
)


class TestResolveAndEnqueueShortUrl(
    unittest.IsolatedAsyncioTestCase,
):

    def _queue(self) -> MagicMock:
        q: MagicMock = MagicMock()
        q.schedule_if_absent = AsyncMock(return_value=True)
        q.discard_member = AsyncMock()
        q.reschedule_in = AsyncMock()
        return q

    async def test_resolved_schedules_and_discards(self) -> None:
        q = self._queue()
        s = _settings()
        with patch.object(
            tool, 'resolve_creator_short_url',
            AsyncMock(return_value=ShortUrlResolution(
                ShortUrlOutcome.RESOLVED, 'alice',
            )),
        ):
            await tool._resolve_and_enqueue_short_url(
                'https://vm.tiktok.com/ZG', 'http://127.0.0.1:8080',
                q, s, 'w1', '127.0.0.1',
            )
        q.schedule_if_absent.assert_awaited_once()
        self.assertEqual(
            q.schedule_if_absent.await_args.args[0], 'alice',
        )
        q.discard_member.assert_awaited_once_with(
            'https://vm.tiktok.com/ZG',
        )
        q.reschedule_in.assert_not_awaited()

    async def test_transient_reschedules(self) -> None:
        q = self._queue()
        s = _settings()
        with patch.object(
            tool, 'resolve_creator_short_url',
            AsyncMock(return_value=ShortUrlResolution(
                ShortUrlOutcome.TRANSIENT,
            )),
        ):
            await tool._resolve_and_enqueue_short_url(
                'https://vm.tiktok.com/ZG', 'http://127.0.0.1:8080',
                q, s, 'w1', '127.0.0.1',
            )
        q.reschedule_in.assert_awaited_once_with(
            'https://vm.tiktok.com/ZG', 300,
        )
        q.discard_member.assert_not_awaited()
        q.schedule_if_absent.assert_not_awaited()

    async def test_unavailable_discards(self) -> None:
        q = self._queue()
        s = _settings()
        with patch.object(
            tool, 'resolve_creator_short_url',
            AsyncMock(return_value=ShortUrlResolution(
                ShortUrlOutcome.UNAVAILABLE,
            )),
        ):
            await tool._resolve_and_enqueue_short_url(
                'https://vm.tiktok.com/ZG', 'http://127.0.0.1:8080',
                q, s, 'w1', '127.0.0.1',
            )
        q.discard_member.assert_awaited_once_with(
            'https://vm.tiktok.com/ZG',
        )
        q.schedule_if_absent.assert_not_awaited()
        q.reschedule_in.assert_not_awaited()


class TestCreatorShortUrlSettings(unittest.TestCase):

    def test_short_url_settings_defaults(self) -> None:
        s = tool.CreatorSettings(
            proxies='http://127.0.0.1:8080',
            redis_dsn='redis://h:6379/0',
            _cli_parse_args=[],
        )
        self.assertEqual(
            s.creator_short_url_resolve_timeout_seconds, 10,
        )
        self.assertEqual(
            s.creator_short_url_retry_interval_seconds, 300,
        )
        self.assertIn('Mobile', s.creator_short_url_user_agent)


if __name__ == '__main__':
    unittest.main()
