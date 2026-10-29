'''Unit tests for tools/tt_creator_resolve.py.'''

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

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
        result = mod.parse_author('<html><body>nope</body></html>', 42)
        self.assertEqual(result.status, mod.STATUS_UNAVAILABLE)
        self.assertEqual(result.detail, 'no rehydration payload')

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


class TestEnqueueCreators(unittest.TestCase):

    def _settings(self, **kwargs: Any) -> mod.ResolveSettings:
        base: dict[str, Any] = {
            'redis_dsn': 'redis://localhost:6379/0',
            'creator_priority_queues': '168:1000000,720:10000,4320:0',
        }
        base.update(kwargs)
        return mod.ResolveSettings(  # type: ignore[arg-type]
            _cli_parse_args=False, **base,
        )

    def test_adds_each_username_at_fallback_weight(self) -> None:
        calls: list[tuple[str, str, int]] = []

        class _Queue:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._tiers: list[Any] = []
                self._key_queues: list[str] = []
                self._redis = mock.Mock()
                self._redis.aclose = mock.AsyncMock()

            def _build_queue_keys(self, tiers: list[Any]) -> list[str]:
                return [f'q{t.tier}' for t in tiers]

            async def add_member(
                self, creator_id: str, name: str, weight: int,
            ) -> bool:
                calls.append((creator_id, name, weight))
                return creator_id == 'new.one'

        with mock.patch.object(mod, 'RedisCreatorQueue', _Queue):
            added, existing = __import__('asyncio').run(
                mod.enqueue_creators(
                    self._settings(), ['new.one', 'old.one'],
                ),
            )
        self.assertEqual((added, existing), (1, 1))
        self.assertEqual(
            calls,
            [('new.one', 'new.one', 10000), ('old.one', 'old.one', 10000)],
        )

    def test_explicit_weight_overrides_fallback(self) -> None:
        seen: list[int] = []

        class _Queue:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._redis = mock.Mock()
                self._redis.aclose = mock.AsyncMock()

            def _build_queue_keys(self, tiers: list[Any]) -> list[str]:
                return []

            async def add_member(
                self, creator_id: str, name: str, weight: int,
            ) -> bool:
                seen.append(weight)
                return True

        with mock.patch.object(mod, 'RedisCreatorQueue', _Queue):
            __import__('asyncio').run(
                mod.enqueue_creators(
                    self._settings(creator_weight=0), ['someone'],
                ),
            )
        self.assertEqual(seen, [0])

    def test_requires_redis_dsn(self) -> None:
        settings = self._settings(redis_dsn=None)
        with self.assertRaises(RuntimeError):
            __import__('asyncio').run(
                mod.enqueue_creators(settings, ['someone']),
            )


if __name__ == '__main__':
    unittest.main()
