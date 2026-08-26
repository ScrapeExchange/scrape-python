import asyncio
import io
import json
import os
import signal
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx
from innertube.errors import RequestError as InnerTubeRequestError
from innertube.errors import ResponseError as InnerTubeResponseError

from tools.yt_discover_search import (
    DiscoverSearchSettings,
    DiscoveredChannel,
    _ChannelEmitter,
    _PidFile,
    _channel_output_stream,
    _dedupe_channels,
    _get_continuation_token,
    _extract_words_from_random_payload,
    _normalise_handle,
    _search_page_with_retry,
    choose_random_search_terms,
    discover_for_term,
    discover_for_terms,
    extract_channels,
    main_async,
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

    def test_youtube_search_concurrency_cli_setting(self) -> None:
        settings: DiscoverSearchSettings = DiscoverSearchSettings(
            _cli_parse_args=[
                '--youtube-search-concurrency', '4',
            ],
        )

        self.assertEqual(settings.youtube_search_concurrency, 4)

    def test_output_file_defaults_to_searched_channels_jsonl(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])

        self.assertEqual(
            settings.output_file,
            'data/searched_channels.jsonl',
        )

    def test_pid_file_defaults_to_var_tmp(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])

        self.assertEqual(
            settings.pid_file,
            '/var/tmp/yt_discover_search.pid',
        )

    def test_defaults_include_50_additional_languages(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])
        original_languages: set[str] = {
            'en', 'es', 'it', 'de', 'fr', 'zh', 'pt-br', 'ro',
        }
        additional_languages: set[str] = {
            'af', 'ar', 'az', 'be', 'bg', 'bn', 'bs', 'ca', 'cs',
            'cy', 'da', 'el', 'eo', 'et', 'eu', 'fa', 'fi', 'ga',
            'gl', 'he', 'hi', 'hr', 'hu', 'hy', 'id', 'is', 'ja',
            'ka', 'kk', 'ko', 'lt', 'lv', 'mk', 'ms', 'nl', 'no',
            'pl', 'ru', 'sk', 'sl', 'sq', 'sr', 'sv', 'sw', 'ta',
            'th', 'tr', 'uk', 'ur', 'vi',
        }
        configured_languages: list[str] = (
            settings.random_word_languages.split(',')
        )

        self.assertEqual(len(configured_languages), 58)
        self.assertEqual(
            set(configured_languages) - original_languages,
            additional_languages,
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


class TestRandomSearchTerms(unittest.IsolatedAsyncioTestCase):
    async def test_gets_japanese_words_from_wikimedia(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])
        response = mock.Mock()
        response.json.return_value = {
            'query': {
                'random': [
                    {'title': '日本の音楽'},
                    {'title': '世界文化遺産'},
                ],
            },
        }
        client = mock.AsyncMock()
        client.get.return_value = response

        with mock.patch(
            'tools.yt_discover_search.httpx.AsyncClient',
        ) as client_type:
            client_type.return_value.__aenter__.return_value = client
            terms = await choose_random_search_terms(
                settings,
                count=2,
                random_word_language='ja',
            )

        self.assertEqual(terms, ['日本の音楽', '世界文化遺産'])
        request = client.get.await_args
        self.assertEqual(
            request.args[0],
            'https://ja.wikipedia.org/w/api.php',
        )
        self.assertEqual(request.kwargs['params']['list'], 'random')
        self.assertIn('User-Agent', request.kwargs['headers'])

    async def test_extracts_words_from_wikimedia_titles(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])
        response = mock.Mock()
        response.json.return_value = {
            'query': {
                'random': [
                    {'title': 'Geschiedenis van Amsterdam'},
                ],
            },
        }
        client = mock.AsyncMock()
        client.get.return_value = response

        with mock.patch(
            'tools.yt_discover_search.httpx.AsyncClient',
        ) as client_type:
            client_type.return_value.__aenter__.return_value = client
            terms = await choose_random_search_terms(
                settings,
                count=2,
                random_word_language='nl',
            )

        self.assertEqual(terms, ['Geschiedenis', 'Amsterdam'])

    async def test_wikimedia_failure_uses_offline_fallback(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])
        client = mock.AsyncMock()
        client.get.side_effect = httpx.ReadTimeout('slow')

        with mock.patch(
            'tools.yt_discover_search.httpx.AsyncClient',
        ) as client_type, mock.patch(
            'tools.yt_discover_search.random.choice',
            return_value='water',
        ):
            client_type.return_value.__aenter__.return_value = client
            terms = await choose_random_search_terms(
                settings,
                count=2,
                random_word_language='ar',
            )

        self.assertEqual(terms, ['water', 'water'])


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

    def test_reopens_rotated_output_file_in_append_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'channels.jsonl'
            rotated: Path = Path(tmp) / 'channels.jsonl.1'

            with _channel_output_stream(str(path)) as stream:
                stream.write('before rotation\n')
                stream.flush()
                path.rename(rotated)
                path.write_text(
                    'existing replacement\n',
                    encoding='utf-8',
                )

                stream.reopen()
                stream.write('after rotation\n')

            self.assertEqual(
                rotated.read_text(encoding='utf-8'),
                'before rotation\n',
            )
            self.assertEqual(
                path.read_text(encoding='utf-8'),
                'existing replacement\nafter rotation\n',
            )


class TestPidFile(unittest.TestCase):
    def test_replaces_owned_stale_pid_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'discover.pid'
            path.write_text('99999999\n', encoding='utf-8')
            pid_file = _PidFile(str(path))

            pid_file.acquire()

            self.assertEqual(
                path.read_text(encoding='utf-8'),
                f'{os.getpid()}\n',
            )
            pid_file.release()
            self.assertFalse(path.exists())


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

    async def test_non_json_response_error_is_transient(self) -> None:
        '''YouTube can return an HTML interstitial where InnerTube
        expects JSON; skip the current term instead of crashing.'''

        search = mock.AsyncMock(
            side_effect=[
                InnerTubeResponseError(
                    "Expected JSON response, got "
                    "'text/html; charset=UTF-8'",
                ),
                InnerTubeResponseError(
                    "Expected JSON response, got "
                    "'text/html; charset=UTF-8'",
                ),
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


class TestDiscoverForTerms(unittest.IsolatedAsyncioTestCase):
    async def test_searches_queue_concurrently_with_unique_proxies(
        self,
    ) -> None:
        active: int = 0
        max_active: int = 0
        calls: list[tuple[str, str | None]] = []
        overlap: asyncio.Event = asyncio.Event()

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del continuation, limiter
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            calls.append((term, proxy))
            try:
                if len(calls) == 1:
                    await overlap.wait()
                else:
                    overlap.set()
                await asyncio.sleep(0)
                return {
                    'channelRenderer': {
                        'channelId': 'UC22BdTgxefuvUivrjesETjg',
                    },
                }
            finally:
                active -= 1

        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ):
            channels: list[DiscoveredChannel] = await _drain(
                discover_for_terms(
                    ['one', 'two', 'three'],
                    continuations=0,
                    concurrency=2,
                    proxies=['proxy-1', 'proxy-2', 'proxy-3'],
                    limiter=_FakeLimiter(),
                )
            )

        self.assertEqual(len(channels), 3)
        self.assertEqual(max_active, 2)
        self.assertEqual(
            {proxy for _, proxy in calls[:2]},
            {'proxy-1', 'proxy-2'},
        )
        self.assertNotIn('proxy-3', {proxy for _, proxy in calls})

    async def test_concurrency_is_capped_by_proxy_count(self) -> None:
        active: int = 0
        max_active: int = 0
        proxies_seen: set[str | None] = set()

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del term, continuation, limiter
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            proxies_seen.add(proxy)
            try:
                await asyncio.sleep(0)
                return {}
            finally:
                active -= 1

        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ):
            await _drain(
                discover_for_terms(
                    ['one', 'two', 'three', 'four'],
                    continuations=0,
                    concurrency=5,
                    proxies=['proxy-1', 'proxy-2'],
                    limiter=_FakeLimiter(),
                )
            )

        self.assertEqual(max_active, 2)
        self.assertEqual(proxies_seen, {'proxy-1', 'proxy-2'})

    async def test_no_proxies_uses_one_direct_worker(self) -> None:
        active: int = 0
        max_active: int = 0
        proxies_seen: set[str | None] = set()

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del term, continuation, limiter
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            proxies_seen.add(proxy)
            try:
                await asyncio.sleep(0)
                return {}
            finally:
                active -= 1

        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ):
            await _drain(
                discover_for_terms(
                    ['one', 'two'],
                    continuations=0,
                    concurrency=5,
                    proxies=[],
                    limiter=_FakeLimiter(),
                )
            )

        self.assertEqual(max_active, 1)
        self.assertEqual(proxies_seen, {None})

    async def test_connection_failures_rotate_through_spare_proxies(
        self,
    ) -> None:
        proxies_seen: list[str | None] = []

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del term, continuation, limiter
            proxies_seen.append(proxy)
            if proxy != 'proxy-3':
                raise httpx.ProxyError('connection failed')
            return {
                'channelRenderer': {
                    'channelId': 'UC22BdTgxefuvUivrjesETjg',
                },
            }

        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ), mock.patch(
            'tools.yt_discover_search.asyncio.sleep',
            new=mock.AsyncMock(),
        ):
            channels: list[DiscoveredChannel] = await _drain(
                discover_for_terms(
                    ['term'],
                    continuations=0,
                    concurrency=1,
                    proxies=['proxy-1', 'proxy-2', 'proxy-3'],
                    limiter=_FakeLimiter(),
                )
            )

        self.assertEqual(
            proxies_seen,
            ['proxy-1', 'proxy-2', 'proxy-3'],
        )
        self.assertEqual(len(channels), 1)

    async def test_failover_does_not_take_an_active_proxy(self) -> None:
        calls: list[tuple[str, str | None]] = []

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del continuation, limiter
            calls.append((term, proxy))
            if proxy == 'proxy-1':
                raise httpx.ProxyError('connection failed')
            await asyncio.sleep(0)
            return {}

        with mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ), mock.patch(
            'tools.yt_discover_search.asyncio.sleep',
            new=mock.AsyncMock(),
        ):
            await _drain(
                discover_for_terms(
                    ['one', 'two'],
                    continuations=0,
                    concurrency=2,
                    proxies=['proxy-1', 'proxy-2', 'proxy-3'],
                    limiter=_FakeLimiter(),
                )
            )

        failed_term: str = next(
            term for term, proxy in calls
            if proxy == 'proxy-1'
        )
        self.assertEqual(
            [proxy for term, proxy in calls if term == failed_term],
            ['proxy-1', 'proxy-3'],
        )


class TestMainConcurrency(unittest.IsolatedAsyncioTestCase):
    async def test_foreign_owned_pid_file_exits_with_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pid_path: Path = Path(tmp) / 'discover.pid'
            pid_path.write_text('99999999\n', encoding='utf-8')
            different_uid: int = os.geteuid() + 1
            stderr = io.StringIO()

            with mock.patch(
                'tools.yt_discover_search.os.geteuid',
                return_value=different_uid,
            ), mock.patch('sys.stderr', stderr):
                result: int = await main_async([
                    '--pid-file', str(pid_path),
                    'one',
                ])

            self.assertEqual(result, 1)
            self.assertIn(
                'not owned by the current user',
                stderr.getvalue(),
            )
            self.assertEqual(
                pid_path.read_text(encoding='utf-8'),
                '99999999\n',
            )

    async def test_existing_running_pid_exits_with_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pid_path: Path = Path(tmp) / 'discover.pid'
            pid_path.write_text(
                f'{os.getpid()}\n',
                encoding='utf-8',
            )
            stderr = io.StringIO()

            with mock.patch('sys.stderr', stderr):
                result: int = await main_async([
                    '--pid-file', str(pid_path),
                    'one',
                ])

            self.assertEqual(result, 1)
            self.assertIn('still running', stderr.getvalue())
            self.assertEqual(
                pid_path.read_text(encoding='utf-8'),
                f'{os.getpid()}\n',
            )

    async def test_pid_file_exists_while_running_and_is_removed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path: Path = Path(tmp) / 'channels.jsonl'
            pid_path: Path = Path(tmp) / 'discover.pid'

            async def discover(*args, **kwargs):
                del args, kwargs
                self.assertEqual(
                    pid_path.read_text(encoding='utf-8'),
                    f'{os.getpid()}\n',
                )
                if False:
                    yield

            limiter: mock.Mock = mock.Mock()
            with mock.patch(
                'tools.yt_discover_search.configure_logging',
            ), mock.patch(
                'tools.yt_discover_search.configure_innertube_executor',
            ), mock.patch(
                'tools.yt_discover_search.YouTubeRateLimiter.get',
                return_value=limiter,
            ), mock.patch(
                'tools.yt_discover_search.discover_for_terms',
                new=discover,
            ), mock.patch(
                'tools.yt_discover_search.aclose_pooled_innertube',
                new=mock.AsyncMock(),
            ), mock.patch(
                'tools.yt_discover_search.shutdown_innertube_executor',
            ):
                result: int = await main_async([
                    '--output-file', str(output_path),
                    '--pid-file', str(pid_path),
                    'one',
                ])

            self.assertEqual(result, 0)
            self.assertFalse(pid_path.exists())

    async def test_sighup_reopens_output_file(self) -> None:
        callbacks: dict[int, object] = {}
        loop: mock.Mock = mock.Mock()
        loop.add_signal_handler.side_effect = (
            lambda sig, callback: callbacks.__setitem__(
                sig, callback,
            )
        )

        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'channels.jsonl'
            rotated: Path = Path(tmp) / 'channels.jsonl.1'

            async def discover(*args, **kwargs):
                del args, kwargs
                yield DiscoveredChannel('UCfirst', '@first')
                path.rename(rotated)
                path.write_text(
                    '{"existing":true}\n',
                    encoding='utf-8',
                )
                callback: object = callbacks[signal.SIGHUP]
                self.assertTrue(callable(callback))
                callback()
                yield DiscoveredChannel('UCsecond', '@second')

            limiter: mock.Mock = mock.Mock()
            with mock.patch(
                'tools.yt_discover_search.configure_logging',
            ), mock.patch(
                'tools.yt_discover_search.configure_innertube_executor',
            ), mock.patch(
                'tools.yt_discover_search.YouTubeRateLimiter.get',
                return_value=limiter,
            ), mock.patch(
                'tools.yt_discover_search.discover_for_terms',
                new=discover,
            ), mock.patch(
                'tools.yt_discover_search.asyncio.get_running_loop',
                return_value=loop,
            ), mock.patch(
                'tools.yt_discover_search.aclose_pooled_innertube',
                new=mock.AsyncMock(),
            ), mock.patch(
                'tools.yt_discover_search.shutdown_innertube_executor',
            ):
                result: int = await main_async([
                    '--output-file', str(path),
                    '--pid-file', str(
                        Path(tmp) / 'discover.pid'
                    ),
                    'one',
                ])

            self.assertEqual(result, 0)
            self.assertEqual(
                rotated.read_text(encoding='utf-8').splitlines(),
                [
                    '{"channel_id":"UCfirst",'
                    '"channel_handle":"@first"}',
                ],
            )
            self.assertEqual(
                path.read_text(encoding='utf-8').splitlines(),
                [
                    '{"existing":true}',
                    '{"channel_id":"UCsecond",'
                    '"channel_handle":"@second"}',
                ],
            )
        loop.add_signal_handler.assert_called_once()
        self.assertEqual(
            loop.add_signal_handler.call_args.args[0],
            signal.SIGHUP,
        )
        loop.remove_signal_handler.assert_called_once_with(
            signal.SIGHUP,
        )

    async def test_cli_concurrency_runs_searches_in_parallel(self) -> None:
        active: int = 0
        max_active: int = 0

        async def search(
            term: str,
            *,
            continuation: str | None,
            proxy: str | None,
            limiter: object,
        ) -> dict[str, object]:
            del term, continuation, proxy, limiter
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0)
                return {}
            finally:
                active -= 1

        limiter: mock.Mock = mock.Mock()
        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            'tools.yt_discover_search.configure_logging',
        ), mock.patch(
            'tools.yt_discover_search.configure_innertube_executor',
        ), mock.patch(
            'tools.yt_discover_search.YouTubeRateLimiter.get',
            return_value=limiter,
        ), mock.patch(
            'tools.yt_discover_search._innertube_search',
            new=search,
        ), mock.patch(
            'tools.yt_discover_search.aclose_pooled_innertube',
            new=mock.AsyncMock(),
        ), mock.patch(
            'tools.yt_discover_search.shutdown_innertube_executor',
        ):
            result: int = await main_async([
                '--output-file', str(Path(tmp) / 'channels.jsonl'),
                '--pid-file', str(Path(tmp) / 'discover.pid'),
                '--youtube-search-concurrency', '2',
                '--proxies', (
                    'http://localhost:8001,'
                    'http://localhost:8002'
                ),
                'one', 'two',
            ])

        self.assertEqual(result, 0)
        self.assertEqual(max_active, 2)


if __name__ == '__main__':
    unittest.main()
