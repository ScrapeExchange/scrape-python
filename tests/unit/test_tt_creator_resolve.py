'''Unit tests for tools/tt_creator_resolve.py.'''

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import httpx2

from tools import tt_creator_resolve as mod


def _page(item: dict | None, status_msg: str = '') -> str:
    '''Build a minimal video page carrying the rehydration payload.'''
    detail: dict[str, Any] = {}
    if item is not None:
        detail['itemInfo'] = {'itemStruct': item}
    if status_msg:
        detail['statusMsg'] = status_msg
    payload: dict[str, Any] = {
        '__DEFAULT_SCOPE__': {'webapp.video-detail': detail},
    }
    body: str = json.dumps(payload)
    return (
        '<html><head></head><body><script '
        'id="__UNIVERSAL_DATA_FOR_REHYDRATION__" '
        f'type="application/json">{body}</script></body></html>'
    )


class _FakeVideo:
    '''Stand-in for TikTokVideo.from_api's return value.'''

    def __init__(self, item: dict) -> None:
        author: dict = item.get('author', {})
        self.video_id: str = str(item.get('id'))
        self.username: str | None = author.get('uniqueId')
        self.author_id: str | None = author.get('id')
        self.sec_uid: str = author.get('secUid', '')
        self.author_nickname: str | None = author.get('nickname')

    @classmethod
    def from_api(cls, payload: dict) -> _FakeVideo:
        if payload.get('malformed'):
            raise ValueError('missing fields')
        return cls(payload)


def _item() -> dict:
    return {
        'id': '7691484712954645774',
        'author': {
            'id': '77926453077549056',
            'uniqueId': 'some.creator',
            'secUid': 'MS4wLjABAAAArnYn',
            'nickname': 'Some Creator',
        },
    }


class TestReadVideoIds(unittest.TestCase):

    def test_dedupes_and_skips_junk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'ids.lst'
            path.write_text(
                '1\n\n# comment\n2\nabc\n1\n 3 \n', encoding='utf-8',
            )
            self.assertEqual(mod.read_video_ids(path), [1, 2, 3])

    def test_reads_stdin_when_path_is_dash(self) -> None:
        with mock.patch(
            'sys.stdin', mock.Mock(read=lambda: '5\n6\n'),
        ):
            self.assertEqual(mod.read_video_ids(None), [5, 6])


class TestBrowserHeaders(unittest.TestCase):

    def test_headers_look_like_a_desktop_browser(self) -> None:
        headers: dict[str, str] = mod.browser_headers('UA-TEST')
        self.assertEqual(headers['User-Agent'], 'UA-TEST')
        self.assertIn('text/html', headers['Accept'])
        self.assertEqual(headers['Sec-Fetch-Mode'], 'navigate')
        self.assertEqual(headers['Sec-Fetch-Dest'], 'document')
        self.assertEqual(headers['Sec-Ch-Ua-Platform'], '"macOS"')
        self.assertEqual(headers['Referer'], 'https://www.tiktok.com/')


class TestParseAuthor(unittest.TestCase):

    def test_resolves_creator_fields(self) -> None:
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo):
            result: mod.Resolution = mod.parse_author(
                _page(_item()), 7691484712954645774,
            )
        self.assertEqual(result.status, mod.STATUS_RESOLVED)
        self.assertEqual(result.username, 'some.creator')
        self.assertEqual(result.user_id, '77926453077549056')
        self.assertEqual(result.sec_uid, 'MS4wLjABAAAArnYn')
        self.assertEqual(result.nickname, 'Some Creator')

    def test_missing_item_is_unavailable(self) -> None:
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo):
            result = mod.parse_author(
                _page(None, status_msg='item not found'), 42,
            )
        self.assertEqual(result.status, mod.STATUS_UNAVAILABLE)
        self.assertEqual(result.detail, 'item not found')

    def test_missing_payload_is_unavailable(self) -> None:
        with self.assertLogs(
            'tools.tt_creator_resolve', level='WARNING',
        ) as logs:
            result = mod.parse_author(
                '<html><body>nope</body></html>', 42,
            )
        self.assertEqual(result.status, mod.STATUS_UNAVAILABLE)
        self.assertEqual(result.detail, 'no rehydration payload')
        self.assertEqual(
            logs.records[0].getMessage(),
            'Creator username not extracted',
        )
        self.assertEqual(logs.records[0].video_id, 42)
        self.assertEqual(logs.records[0].detail, 'no rehydration payload')

    def test_missing_item_logs_the_reason(self) -> None:
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo), self.assertLogs(
            'tools.tt_creator_resolve', level='WARNING',
        ) as logs:
            mod.parse_author(_page(None, status_msg='item not found'), 7)
        self.assertEqual(logs.records[0].status, mod.STATUS_UNAVAILABLE)
        self.assertEqual(logs.records[0].detail, 'item not found')

    def test_bot_check_page_is_transient(self) -> None:
        result = mod.parse_author(
            '<html><body>Please wait at verify.tiktok.com</body></html>',
            42,
        )
        self.assertEqual(result.status, mod.STATUS_TRANSIENT)
        self.assertEqual(result.detail, 'bot check')

    def test_malformed_item_is_an_error(self) -> None:
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo):
            result = mod.parse_author(
                _page({'id': '1', 'malformed': True}), 1,
            )
        self.assertEqual(result.status, mod.STATUS_ERROR)
        self.assertIn('ValueError', result.detail or '')

    def test_item_without_username_is_unavailable(self) -> None:
        item: dict = _item()
        item['author'] = {'id': '1'}
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo):
            result = mod.parse_author(_page(item), 1)
        self.assertEqual(result.status, mod.STATUS_UNAVAILABLE)
        self.assertEqual(result.detail, 'item has no author username')

    def test_tiktok_server_errors_are_transient(self) -> None:
        for status_msg in (
            'ErrSysPanic',
            'RPCError{PSM:[ies.item.attr] ErrType:[RPC_FAILED]}',
        ):
            with self.subTest(status_msg=status_msg):
                result = mod.parse_author(
                    _page(None, status_msg=status_msg), 42,
                )
                self.assertEqual(result.status, mod.STATUS_TRANSIENT)

    def test_log_names_the_proxy(self) -> None:
        with self.assertLogs(
            'tools.tt_creator_resolve', level='WARNING',
        ) as logs:
            mod.parse_author(
                '<html><body>nope</body></html>', 42,
                'http://user:pw@10.1.2.3:50100',
            )
        self.assertEqual(logs.records[0].proxy_ip, '10.1.2.3')
        self.assertEqual(logs.records[0].proxy_port, '50100')


def _response(
    url: str, status_code: int = 200, text: str = '',
) -> httpx2.Response:
    '''A response whose final URL is *url*, as after redirects.'''
    return httpx2.Response(
        status_code, text=text, request=httpx2.Request('GET', url),
    )


class TestResolverResolve(unittest.IsolatedAsyncioTestCase):

    VIDEO_URL: str = 'https://www.tiktok.com/@_/video/42'

    def _resolver(
        self, responses: list[httpx2.Response], proxies: list[str],
    ) -> tuple[mod.Resolver, mock.MagicMock]:
        settings: mock.MagicMock = mock.MagicMock()
        settings.max_retries = 3
        settings.proxies = proxies
        limiter: mock.MagicMock = mock.MagicMock()
        limiter.acquire = mock.AsyncMock(side_effect=proxies)
        resolver: mod.Resolver = mod.Resolver(settings, limiter, {})
        client: mock.MagicMock = mock.MagicMock()
        client.get = mock.AsyncMock(side_effect=responses)
        resolver._client = mock.MagicMock(return_value=client)
        return resolver, client

    async def test_region_redirect_retries_on_another_proxy(
        self,
    ) -> None:
        resolver, _ = self._resolver(
            [
                _response('https://www.tiktok.com/hk/about'),
                _response(self.VIDEO_URL, text=_page(_item())),
            ],
            ['http://10.0.0.1:1', 'http://10.0.0.2:2'],
        )
        with mock.patch.object(
            mod, 'TikTokVideo', _FakeVideo,
        ), mock.patch.object(
            mod.asyncio, 'sleep', mock.AsyncMock(),
        ), self.assertLogs(
            'tools.tt_creator_resolve', level='WARNING',
        ) as logs:
            result = await resolver.resolve(42)

        self.assertEqual(result.status, mod.STATUS_RESOLVED)
        self.assertEqual(result.username, 'some.creator')
        self.assertEqual(
            logs.records[0].detail, 'redirected to /hk/about',
        )
        self.assertEqual(logs.records[0].proxy_ip, '10.0.0.1')

    async def test_persistent_login_redirect_stays_transient(
        self,
    ) -> None:
        login: str = (
            'https://www.tiktok.com/login?redirect_url=x'
        )
        resolver, client = self._resolver(
            [_response(login) for _ in range(4)],
            [f'http://10.0.0.{n}:1' for n in range(1, 5)],
        )
        with mock.patch.object(
            mod.asyncio, 'sleep', mock.AsyncMock(),
        ), self.assertLogs('tools.tt_creator_resolve'):
            result = await resolver.resolve(42)

        self.assertEqual(result.status, mod.STATUS_TRANSIENT)
        self.assertEqual(result.detail, 'redirected to /login')
        self.assertEqual(client.get.await_count, 4)

    async def test_video_page_is_parsed(self) -> None:
        resolver, _ = self._resolver(
            [_response(self.VIDEO_URL, text=_page(_item()))],
            ['http://10.0.0.1:1'],
        )
        with mock.patch.object(mod, 'TikTokVideo', _FakeVideo):
            result = await resolver.resolve(42)
        self.assertEqual(result.status, mod.STATUS_RESOLVED)


class TestClassifyStatus(unittest.TestCase):

    def test_maps_statuses(self) -> None:
        self.assertEqual(mod._classify_status(200), None)
        self.assertEqual(
            mod._classify_status(429), mod.STATUS_TRANSIENT,
        )
        self.assertEqual(
            mod._classify_status(403), mod.STATUS_TRANSIENT,
        )
        self.assertEqual(
            mod._classify_status(503), mod.STATUS_TRANSIENT,
        )
        self.assertEqual(
            mod._classify_status(404), mod.STATUS_UNAVAILABLE,
        )


class TestOutputRecords(unittest.TestCase):

    def test_append_record_writes_one_jsonl_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'nested' / 'out.jsonl'
            mod.append_record(
                path,
                mod.Resolution(
                    video_id=42,
                    status=mod.STATUS_RESOLVED,
                    username='someone',
                    user_id='7',
                ),
            )
            mod.append_record(
                path,
                mod.Resolution(
                    video_id=43,
                    status=mod.STATUS_UNAVAILABLE,
                    detail='gone',
                ),
            )
            lines: list[str] = path.read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])['video_id'], 42)
        self.assertEqual(json.loads(lines[0])['username'], 'someone')
        self.assertEqual(json.loads(lines[1])['status'], 'unavailable')

    def test_load_processed_ids_reads_existing_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'processed.lst'
            path.write_text('1\n2\nnot-an-id\n\n 3 \n')
            self.assertEqual(mod.load_processed_ids(path), {1, 2, 3})

    def test_load_processed_ids_missing_file_is_empty(self) -> None:
        self.assertEqual(
            mod.load_processed_ids(Path('/nonexistent/p.lst')), set(),
        )
        self.assertEqual(mod.load_processed_ids(None), set())

    def test_processed_log_appends_and_flushes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'nested' / 'processed.lst'
            log: mod.ProcessedLog = mod.ProcessedLog(path)
            log.record(11)
            log.record(12)
            # Visible on disk before close, so a kill keeps it.
            self.assertEqual(
                path.read_text().splitlines(), ['11', '12'],
            )
            log.close()
            self.assertEqual(log.recorded, 2)


def _resolve_settings(**kwargs: Any) -> mod.ResolveSettings:
    base: dict[str, Any] = {
        'redis_dsn': 'redis://localhost:6379/0',
        'creator_priority_queues': '168:1000000,720:10000,4320:0',
    }
    base.update(kwargs)
    return mod.ResolveSettings(  # type: ignore[arg-type]
        _cli_parse_args=False, **base,
    )


class _FakeCreatorQueue:
    '''Stand-in for RedisCreatorQueue recording schedule calls.'''

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._tiers: list[Any] = []
        self._key_queues: list[str] = []
        self._redis = mock.Mock()
        self._redis.aclose = mock.AsyncMock()
        self.calls: list[tuple[str, str, int]] = []
        self.fail: bool = False

    def _build_queue_keys(self, tiers: list[Any]) -> list[str]:
        return [f'q{t.tier}' for t in tiers]

    async def schedule_if_absent(
        self, creator_id: str, name: str, weight: int,
    ) -> bool:
        if self.fail:
            raise mod.RedisError('connection lost')
        self.calls.append((creator_id, name, weight))
        return creator_id == 'new.one'


class TestCreatorEnqueuer(unittest.IsolatedAsyncioTestCase):

    def _enqueuer(self, **kwargs: Any) -> mod.CreatorEnqueuer:
        with mock.patch.object(mod, 'RedisCreatorQueue', _FakeCreatorQueue):
            return mod.CreatorEnqueuer(_resolve_settings(**kwargs))

    async def test_queues_at_fallback_weight_and_logs(self) -> None:
        enqueuer: mod.CreatorEnqueuer = self._enqueuer()
        with self.assertLogs(
            'tools.tt_creator_resolve', level='INFO',
        ) as logs:
            self.assertTrue(await enqueuer.add('new.one'))
            self.assertTrue(await enqueuer.add('old.one'))
        self.assertEqual(
            enqueuer._queue.calls,
            [('new.one', 'new.one', 10000), ('old.one', 'old.one', 10000)],
        )
        self.assertEqual((enqueuer.added, enqueuer.existing), (1, 1))
        first, second = logs.records
        self.assertEqual(first.getMessage(), 'Creator queue add')
        self.assertEqual(first.tier, 2)
        self.assertEqual(first.outcome, 'queued')
        self.assertEqual(second.outcome, 'already_queued')

    async def test_same_username_is_scheduled_once(self) -> None:
        enqueuer: mod.CreatorEnqueuer = self._enqueuer()
        with self.assertLogs('tools.tt_creator_resolve', level='INFO'):
            await enqueuer.add('new.one')
            await enqueuer.add('new.one')
        self.assertEqual(len(enqueuer._queue.calls), 1)

    async def test_explicit_weight_overrides_fallback(self) -> None:
        enqueuer: mod.CreatorEnqueuer = self._enqueuer(creator_weight=0)
        with self.assertLogs('tools.tt_creator_resolve', level='INFO'):
            await enqueuer.add('someone')
        self.assertEqual(enqueuer._queue.calls[0][2], 0)

    async def test_redis_failure_returns_false_and_allows_retry(
        self,
    ) -> None:
        enqueuer: mod.CreatorEnqueuer = self._enqueuer()
        enqueuer._queue.fail = True
        with self.assertLogs('tools.tt_creator_resolve', level='WARNING'):
            self.assertFalse(await enqueuer.add('new.one'))
        self.assertEqual(enqueuer.failed, 1)
        enqueuer._queue.fail = False
        with self.assertLogs('tools.tt_creator_resolve', level='INFO'):
            self.assertTrue(await enqueuer.add('new.one'))
        self.assertEqual(enqueuer.added, 1)

    def test_requires_redis_dsn(self) -> None:
        with self.assertRaises(RuntimeError):
            mod.CreatorEnqueuer(_resolve_settings(redis_dsn=None))


class TestFilterQueuedVideoIds(unittest.IsolatedAsyncioTestCase):

    async def test_drops_queued_ids_in_batches(self) -> None:
        queue: mock.MagicMock = mock.MagicMock()

        async def _states(ids: list[str]) -> dict[str, Any]:
            return {
                video_id: (
                    mod.VideoState.QUEUED if video_id in ('2', '4')
                    else mod.VideoState.FAILED if video_id == '3'
                    else None
                )
                for video_id in ids
            }

        queue.get_states = mock.AsyncMock(side_effect=_states)
        redis: mock.MagicMock = mock.MagicMock()
        redis.aclose = mock.AsyncMock()
        with mock.patch.object(
            mod, 'redis_from_url', return_value=redis,
        ), mock.patch.object(
            mod, 'make_video_scrape_queue', return_value=queue,
        ) as factory, mock.patch.object(
            mod, 'close_backlog_clients', mock.AsyncMock(),
        ) as close_backlog, mock.patch.object(
            mod, 'VIDEO_QUEUE_STATE_BATCH', 2,
        ):
            remaining: list[int] = await mod.filter_queued_video_ids(
                'redis://x', [1, 2, 3, 4, 5],
            )

        # Only QUEUED is skipped; failed and unknown ids are fetched.
        self.assertEqual(remaining, [1, 3, 5])
        self.assertEqual(queue.get_states.await_count, 3)
        self.assertEqual(factory.call_args.kwargs['platform'], 'tiktok')
        redis.aclose.assert_awaited_once()
        close_backlog.assert_awaited_once()


class TestRunQueueing(unittest.IsolatedAsyncioTestCase):

    async def test_skips_video_queue_ids_and_queues_creators_live(
        self,
    ) -> None:
        resolved: dict[int, str] = {1: 'alpha', 3: 'gamma'}
        fetched: list[int] = []

        class _Resolver:
            def __init__(self, *args: Any) -> None:
                pass

            async def resolve(self, video_id: int) -> mod.Resolution:
                fetched.append(video_id)
                return mod.Resolution(
                    video_id=video_id,
                    status=mod.STATUS_RESOLVED,
                    username=resolved[video_id],
                )

            async def close(self) -> None:
                pass

        enqueuer: mock.MagicMock = mock.MagicMock()
        # Redis refuses gamma: its id must stay out of the log.
        enqueuer.add = mock.AsyncMock(
            side_effect=lambda name: name != 'gamma',
        )
        enqueuer.close = mock.AsyncMock()
        enqueuer.added, enqueuer.existing, enqueuer.failed = 1, 0, 1

        with tempfile.TemporaryDirectory() as tmp:
            input_file: Path = Path(tmp) / 'ids.lst'
            input_file.write_text('1\n2\n3\n')
            processed_file: Path = Path(tmp) / 'done.lst'
            settings: mod.ResolveSettings = _resolve_settings(
                input_file=input_file, processed_file=processed_file,
            )
            with mock.patch.object(
                mod, 'filter_queued_video_ids',
                mock.AsyncMock(return_value=[1, 3]),
            ), mock.patch.object(
                mod, 'TikTokRateLimiter',
            ), mock.patch.object(
                mod, 'Resolver', _Resolver,
            ), mock.patch.object(
                mod, 'CreatorEnqueuer', return_value=enqueuer,
            ), mock.patch('builtins.print'), self.assertLogs(
                'tools.tt_creator_resolve', level='INFO',
            ):
                exit_code: int = await mod.run(settings)

            self.assertEqual(exit_code, 0)
            self.assertEqual(sorted(fetched), [1, 3])
            self.assertEqual(
                sorted(c.args[0] for c in enqueuer.add.await_args_list),
                ['alpha', 'gamma'],
            )
            self.assertEqual(processed_file.read_text(), '1\n')
            enqueuer.close.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
