import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx
from innertube.errors import RequestError as InnerTubeRequestError

from tools.yt_discover_search import (
    DiscoverSearchSettings,
    DiscoveredChannel,
    _ChannelEmitter,
    _channel_output_stream,
    _dedupe_channels,
    _get_continuation_token,
    _extract_words_from_random_payload,
    _normalise_handle,
    _search_page_with_retry,
    discover_for_term,
    extract_channels,
)


class TestDiscoverSearchSettings(unittest.TestCase):
    def test_cli_positional_search_terms(self) -> None:
        settings = DiscoverSearchSettings(
            _cli_parse_args=['special', 'topic'],
        )

        self.assertEqual(
            settings.search_terms,
            ['special', 'topic'],
        )

    def test_cli_settings_flags(self) -> None:
        settings = DiscoverSearchSettings(
            _cli_parse_args=[
                '--youtube-search-continuations', '3',
                '--random-word-language', 'de',
                '--keyword-count', '4',
            ],
        )

        self.assertEqual(settings.youtube_search_continuations, 3)
        self.assertEqual(settings.random_word_language, 'de')
        self.assertEqual(settings.keyword_count, 4)

    def test_keyword_count_defaults_to_one(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])

        self.assertEqual(settings.keyword_count, 1)

    def test_output_file_defaults_to_searched_channels_jsonl(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])

        self.assertEqual(
            settings.output_file,
            'data/searched_channels.jsonl',
        )


class TestRandomWordPayload(unittest.TestCase):
    def test_extracts_list_of_string_words(self) -> None:
        self.assertEqual(
            _extract_words_from_random_payload(['alpha', 'bravo']),
            ['alpha', 'bravo'],
        )

    def test_extracts_dict_word_values(self) -> None:
        self.assertEqual(
            _extract_words_from_random_payload([
                {'word': 'alpha'},
                {'word': 'bravo'},
            ]),
            ['alpha', 'bravo'],
        )


class TestNormaliseHandle(unittest.TestCase):
    def test_keeps_at_handle(self) -> None:
        self.assertEqual(_normalise_handle('@HistoryMatters'), '@HistoryMatters')

    def test_extracts_handle_from_path(self) -> None:
        self.assertEqual(
            _normalise_handle('/@HistoryMatters/videos'),
            '@HistoryMatters',
        )

    def test_rejects_channel_path(self) -> None:
        self.assertIsNone(
            _normalise_handle('/channel/UC22BdTgxefuvUivrjesETjg'),
        )


class TestExtractChannels(unittest.TestCase):
    def test_extracts_channel_renderer_id_and_handle(self) -> None:
        payload = {
            'contents': {
                'twoColumnSearchResultsRenderer': {
                    'primaryContents': {
                        'sectionListRenderer': {
                            'contents': [
                                {
                                    'itemSectionRenderer': {
                                        'contents': [
                                            {
                                                'channelRenderer': {
                                                    'channelId': (
                                                        'UC22BdTgxefuvUivrjesETjg'
                                                    ),
                                                    'navigationEndpoint': {
                                                        'browseEndpoint': {
                                                            'browseId': (
                                                                'UC22BdTgxefuvUivrjesETjg'
                                                            ),
                                                            'canonicalBaseUrl': (
                                                                '/@HistoryMatters'
                                                            ),
                                                        }
                                                    },
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        self.assertEqual(
            extract_channels(payload),
            [
                DiscoveredChannel(
                    'UC22BdTgxefuvUivrjesETjg',
                    '@HistoryMatters',
                )
            ],
        )

    def test_extracts_owner_browse_endpoint_from_video_result(self) -> None:
        payload = {
            'videoRenderer': {
                'ownerText': {
                    'runs': [
                        {
                            'navigationEndpoint': {
                                'browseEndpoint': {
                                    'browseId': (
                                        'UCuAXFkgsw1L7xaCfnd5JJOw'
                                    ),
                                    'canonicalBaseUrl': (
                                        '/@RickAstleyYT'
                                    ),
                                }
                            }
                        }
                    ]
                }
            }
        }

        self.assertEqual(
            extract_channels(payload),
            [
                DiscoveredChannel(
                    'UCuAXFkgsw1L7xaCfnd5JJOw',
                    '@RickAstleyYT',
                )
            ],
        )

    def test_extracts_continuation_token(self) -> None:
        payload = {
            'continuationItemRenderer': {
                'continuationEndpoint': {
                    'continuationCommand': {'token': 'tok123'}
                }
            }
        }

        self.assertEqual(_get_continuation_token(payload), 'tok123')


class TestDedupeChannels(unittest.TestCase):
    def test_id_record_replaces_handle_only_record(self) -> None:
        channels = _dedupe_channels([
            DiscoveredChannel(None, '@HistoryMatters'),
            DiscoveredChannel(
                'UC22BdTgxefuvUivrjesETjg',
                '@HistoryMatters',
            ),
        ])

        self.assertEqual(
            channels,
            [
                DiscoveredChannel(
                    'UC22BdTgxefuvUivrjesETjg',
                    '@HistoryMatters',
                )
            ],
        )


class _RecordingStream(io.StringIO):
    '''StringIO that counts flush() calls.'''

    def __init__(self) -> None:
        super().__init__()
        self.flush_count: int = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


class TestChannelEmitter(unittest.TestCase):
    def _lines(self, stream: _RecordingStream) -> list[dict]:
        return [
            json.loads(line)
            for line in stream.getvalue().splitlines()
        ]

    def test_emits_new_channel_once(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        self.assertTrue(
            emitter.emit(DiscoveredChannel('UCabc', '@abc')),
        )
        self.assertFalse(
            emitter.emit(DiscoveredChannel('UCabc', '@abc')),
        )
        self.assertEqual(
            self._lines(stream),
            [{'channel_id': 'UCabc', 'channel_handle': '@abc'}],
        )

    def test_skips_duplicate_id_with_new_handle(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        self.assertTrue(
            emitter.emit(DiscoveredChannel('UCabc', '@abc')),
        )
        self.assertFalse(
            emitter.emit(DiscoveredChannel('UCabc', '@other')),
        )
        self.assertEqual(len(self._lines(stream)), 1)

    def test_handle_only_then_id_reemits_upgrade(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        self.assertTrue(
            emitter.emit(DiscoveredChannel(None, '@abc')),
        )
        # The richer id+handle record re-emits so the id is kept.
        self.assertTrue(
            emitter.emit(DiscoveredChannel('UCabc', '@abc')),
        )
        self.assertEqual(
            self._lines(stream),
            [
                {'channel_id': None, 'channel_handle': '@abc'},
                {'channel_id': 'UCabc', 'channel_handle': '@abc'},
            ],
        )

    def test_id_then_handle_only_skips(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        self.assertTrue(
            emitter.emit(DiscoveredChannel('UCabc', '@abc')),
        )
        self.assertFalse(
            emitter.emit(DiscoveredChannel(None, '@abc')),
        )
        self.assertEqual(len(self._lines(stream)), 1)

    def test_skips_channel_with_no_identity(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        self.assertFalse(
            emitter.emit(DiscoveredChannel(None, None)),
        )
        self.assertEqual(stream.getvalue(), '')

    def test_flushes_each_emitted_line(self) -> None:
        stream = _RecordingStream()
        emitter = _ChannelEmitter(stream)

        emitter.emit(DiscoveredChannel('UCabc', '@abc'))
        emitter.emit(DiscoveredChannel('UCdef', '@def'))
        # One flush per written line; the skipped duplicate adds
        # none.
        emitter.emit(DiscoveredChannel('UCabc', '@abc'))
        self.assertEqual(stream.flush_count, 2)


class TestChannelOutputStream(unittest.TestCase):
    def test_creates_parent_directory_for_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'nested' / 'channels.jsonl'

            with _channel_output_stream(str(path)) as stream:
                stream.write('{"channel_id":"UCabc"}\n')

            self.assertTrue(path.exists())
            self.assertEqual(
                path.read_text(encoding='utf-8'),
                '{"channel_id":"UCabc"}\n',
            )


class _FakeLimiter:
    '''Minimal stand-in for YouTubeRateLimiter in tests.'''

    def select_proxy(self, _call_type: object) -> str | None:
        return None


async def _drain(agen) -> list:
    return [item async for item in agen]


class TestSearchPageWithRetry(unittest.IsolatedAsyncioTestCase):
    async def test_returns_payload_on_first_attempt(self) -> None:
        payload = {'ok': True}
        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=mock.AsyncMock(return_value=payload),
        ) as search:
            result = await _search_page_with_retry(
                'term', continuation=None, proxy=None,
                limiter=_FakeLimiter(),
            )
        self.assertEqual(result, payload)
        self.assertEqual(search.await_count, 1)

    async def test_retries_once_then_succeeds(self) -> None:
        payload = {'ok': True}
        search = mock.AsyncMock(
            side_effect=[httpx.ReadTimeout('slow'), payload],
        )
        with mock.patch(
            'tools.yt_discover_search._innertube_search', new=search,
        ), mock.patch(
            'tools.yt_discover_search.asyncio.sleep',
            new=mock.AsyncMock(),
        ) as sleep:
            result = await _search_page_with_retry(
                'term', continuation=None, proxy=None,
                limiter=_FakeLimiter(),
            )
        self.assertEqual(result, payload)
        self.assertEqual(search.await_count, 2)
        sleep.assert_awaited_once()

    async def test_returns_none_after_two_transient_failures(
        self,
    ) -> None:
        search = mock.AsyncMock(
            side_effect=[
                httpx.ConnectError('boom'),
                InnerTubeRequestError('500'),
            ],
        )
        with mock.patch(
            'tools.yt_discover_search._innertube_search', new=search,
        ), mock.patch(
            'tools.yt_discover_search.asyncio.sleep',
            new=mock.AsyncMock(),
        ):
            result = await _search_page_with_retry(
                'term', continuation=None, proxy=None,
                limiter=_FakeLimiter(),
            )
        self.assertIsNone(result)
        self.assertEqual(search.await_count, 2)

    async def test_proxy_error_is_transient(self) -> None:
        '''A proxy 503 (httpx.ProxyError) must be treated as
        transient and retried/skipped, not crash the tool.'''

        search = mock.AsyncMock(
            side_effect=[
                httpx.ProxyError('503 Service Unavailable'),
                httpx.ProxyError('503 Service Unavailable'),
            ],
        )
        with mock.patch(
            'tools.yt_discover_search._innertube_search', new=search,
        ), mock.patch(
            'tools.yt_discover_search.asyncio.sleep',
            new=mock.AsyncMock(),
        ):
            result = await _search_page_with_retry(
                'term', continuation=None, proxy=None,
                limiter=_FakeLimiter(),
            )
        self.assertIsNone(result)
        self.assertEqual(search.await_count, 2)

    async def test_non_transient_error_propagates(self) -> None:
        search = mock.AsyncMock(side_effect=ValueError('bug'))
        with mock.patch(
            'tools.yt_discover_search._innertube_search', new=search,
        ):
            with self.assertRaises(ValueError):
                await _search_page_with_retry(
                    'term', continuation=None, proxy=None,
                    limiter=_FakeLimiter(),
                )
        self.assertEqual(search.await_count, 1)


class TestDiscoverForTermFailure(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_stops_term_when_page_gives_up(self) -> None:
        '''A None page (retry exhausted) ends the term but keeps
        channels yielded from the earlier successful page.'''

        page_one = {
            'channelRenderer': {
                'channelId': 'UC22BdTgxefuvUivrjesETjg',
            },
            'continuationItemRenderer': {
                'continuationEndpoint': {
                    'continuationCommand': {'token': 'NEXT'},
                },
            },
        }
        # First page returns a payload with a continuation token;
        # the second page exhausts its retry and returns None.
        retry = mock.AsyncMock(side_effect=[page_one, None])
        with mock.patch(
            'tools.yt_discover_search._search_page_with_retry',
            new=retry,
        ):
            channels = await _drain(
                discover_for_term(
                    'term', continuations=5,
                    limiter=_FakeLimiter(),
                )
            )
        self.assertEqual(
            channels,
            [DiscoveredChannel('UC22BdTgxefuvUivrjesETjg', None)],
        )
        # Two pages attempted: the good one and the one that gave up.
        self.assertEqual(retry.await_count, 2)


if __name__ == '__main__':
    unittest.main()
