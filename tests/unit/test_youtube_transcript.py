'''
Unit tests for the YouTube transcript panel parsing, the transcript
fields of YouTubeVideo, the video schema and the best-effort transcript
step of InnerTubeVideoParser.
'''

import json
import unittest

from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from innertube.errors import RequestError as InnerTubeRequestError
from innertube.models import Error as InnerTubeError
from jsonschema import Draft202012Validator
from prometheus_client import REGISTRY

from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.rate_limiter import RateLimitWaitExceeded
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube import youtube_video_innertube as innertube_mod
from scrape_exchange.youtube.youtube_client import _get_scraper
from scrape_exchange.youtube.youtube_transcript import (
    build_panel_body, build_panel_params, caption_languages,
    detect_audio_language, has_transcript_panel, match_language,
    parse_transcript_panel,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video_innertube import (
    InnerTubeVideoParser,
)

COLLATERAL: Path = Path(__file__).parent.parent / 'collateral'
VIDEO_ID: str = '7PCQ7_m9uxw'
PROXY: str = 'http://localhost:8080'


def _load_panel() -> dict[str, Any]:
    return json.loads(
        (COLLATERAL / 'youtube_transcript_panel.json').read_text()
    )


def _segment_item(start_time: int, text: str) -> dict[str, Any]:
    return {
        'macroMarkersPanelItemViewModel': {
            'item': {
                'timelineItemViewModel': {
                    'contentItems': [{
                        'transcriptSegmentViewModel': {'simpleText': text},
                    }],
                },
            },
            'onTap': {
                'innertubeCommand': {
                    'watchEndpoint': {'startTimeSeconds': start_time},
                },
            },
        },
    }


def _chapter_item(start_time: int, title: str) -> dict[str, Any]:
    return {
        'macroMarkersPanelItemViewModel': {
            'item': {'timelineChapterViewModel': {'title': title}},
            'onTap': {
                'innertubeCommand': {
                    'watchEndpoint': {'startTimeSeconds': start_time},
                },
            },
        },
    }


def _next_data(with_transcript: bool = True) -> dict[str, Any]:
    data: dict[str, Any] = {'engagementPanels': []}
    if with_transcript:
        data['engagementPanels'].append({
            'engagementPanelSectionListRenderer': {
                'content': {
                    'continuationItemRenderer': {
                        'continuationEndpoint': {
                            'getTranscriptEndpoint': {'params': 'abc'},
                        },
                    },
                },
            },
        })
    return data


def _player_data() -> dict[str, Any]:
    return {
        'playabilityStatus': {'status': 'OK'},
        'videoDetails': {'videoId': VIDEO_ID, 'title': 'A title'},
    }


def _count(outcome: str, reason: str) -> float:
    return REGISTRY.get_sample_value(
        'youtube_transcript_scrapes_total',
        {
            'platform': 'youtube',
            'scraper': _get_scraper(),
            'outcome': outcome,
            'reason': reason,
            'worker_id': get_worker_id(),
            'proxy_file': proxy_file_label(PROXY),
        },
    ) or 0.0


class TestPanelRequest(unittest.TestCase):
    def test_params_match_those_sent_by_the_browser(self) -> None:
        self.assertEqual(
            build_panel_params(VIDEO_ID), 'qgkPCgs3UENRN19tOXV4dxgC',
        )

    def test_body(self) -> None:
        self.assertEqual(
            build_panel_body(VIDEO_ID),
            {
                'panelId': 'PAmodern_transcript_view',
                'params': 'qgkPCgs3UENRN19tOXV4dxgC',
            },
        )

    def test_body_with_language(self) -> None:
        self.assertEqual(
            build_panel_body(VIDEO_ID, 'nl'),
            {
                'panelId': 'PAmodern_transcript_view',
                'params': 'qgkPCgs3UENRN19tOXV4dxgC',
                'context': {'client': {'hl': 'nl'}},
            },
        )
        self.assertNotIn('context', build_panel_body(VIDEO_ID, None))
        self.assertNotIn('context', build_panel_body(VIDEO_ID, ''))

    def test_invalid_video_id(self) -> None:
        for video_id in ('', 'vidé'):
            with self.assertRaises(ValueError):
                build_panel_params(video_id)


def _player_with_tracks(
    *tracks: tuple[str, str | None], microformat: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        'captions': {
            'playerCaptionsTracklistRenderer': {
                'captionTracks': [
                    {'languageCode': code, 'kind': kind} if kind
                    else {'languageCode': code}
                    for code, kind in tracks
                ],
            },
        },
    }
    if microformat:
        data['microformat'] = {
            'playerMicroformatRenderer': {
                'defaultAudioLanguage': microformat,
            },
        }
    return data


class TestAudioLanguage(unittest.TestCase):
    def test_asr_track_gives_spoken_language(self) -> None:
        self.assertEqual(
            detect_audio_language(_player_with_tracks(('en', 'asr'))), 'en',
        )

    def test_asr_track_among_manual_tracks(self) -> None:
        player: dict[str, Any] = _player_with_tracks(
            ('en', None), ('nl', 'asr'), ('de', None),
        )
        self.assertEqual(detect_audio_language(player), 'nl')

    def test_microformat_takes_precedence(self) -> None:
        player: dict[str, Any] = _player_with_tracks(
            ('en', 'asr'), microformat='fr',
        )
        self.assertEqual(detect_audio_language(player), 'fr')

    def test_manual_tracks_only_is_unknown(self) -> None:
        player: dict[str, Any] = _player_with_tracks(
            ('nl-NL', None), ('en', None),
        )
        self.assertIsNone(detect_audio_language(player))

    def test_no_captions_or_unexpected_data(self) -> None:
        for data in (
            {}, {'captions': {}}, {'captions': None},
            {'captions': {'playerCaptionsTracklistRenderer': {
                'captionTracks': 'x'}}},
            {'captions': {'playerCaptionsTracklistRenderer': {
                'captionTracks': [None, {'kind': 'asr'}]}}},
            {'microformat': None}, {'microformat': 'x'},
            {'microformat': {'playerMicroformatRenderer': None}},
            {'captions': {'playerCaptionsTracklistRenderer': None}},
        ):
            with self.subTest(data=data):
                self.assertIsNone(detect_audio_language(data))

    def test_caption_languages(self) -> None:
        self.assertEqual(
            caption_languages(
                _player_with_tracks(('nl-NL', None), ('en', 'asr'))
            ),
            ['nl-NL', 'en'],
        )
        self.assertEqual(caption_languages({}), [])


class TestApplyDefaultAudioLanguage(unittest.TestCase):
    def test_set_from_asr_track(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        InnerTubeVideoParser._apply_player_data(
            video, _player_data() | _player_with_tracks(('en', 'asr')),
        )
        self.assertEqual(video.default_audio_language, 'en')

    def test_existing_value_kept_when_not_detectable(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        video.default_audio_language = 'fr'
        InnerTubeVideoParser._apply_player_data(
            video, _player_data() | _player_with_tracks(('en', None)),
        )
        self.assertEqual(video.default_audio_language, 'fr')

    def test_nothing_detectable_leaves_language_unset(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        InnerTubeVideoParser._apply_player_data(video, _player_data())
        self.assertIsNone(video.default_audio_language)


class TestMatchLanguage(unittest.TestCase):
    def test_exact_match_ignores_case(self) -> None:
        self.assertEqual(match_language(['en', 'nl-NL'], 'NL-nl'), 'nl-NL')

    def test_primary_subtag_match(self) -> None:
        self.assertEqual(match_language(['en', 'nl-NL'], 'nl'), 'nl-NL')
        self.assertEqual(match_language(['nl', 'en'], 'nl-BE'), 'nl')
        self.assertEqual(match_language(['pt_BR'], 'pt'), 'pt_BR')

    def test_exact_match_preferred_over_primary(self) -> None:
        self.assertEqual(
            match_language(['pt-PT', 'pt-BR'], 'pt-BR'), 'pt-BR',
        )

    def test_no_match(self) -> None:
        self.assertIsNone(match_language(['en'], 'nl'))
        self.assertIsNone(match_language([], 'en'))


class TestHasTranscriptPanel(unittest.TestCase):
    def test_with_transcript(self) -> None:
        self.assertTrue(has_transcript_panel(_next_data(True)))

    def test_without_transcript(self) -> None:
        self.assertFalse(has_transcript_panel(_next_data(False)))
        self.assertFalse(has_transcript_panel({}))
        self.assertFalse(has_transcript_panel(None))


class TestParseTranscriptPanel(unittest.TestCase):
    def test_parses_captured_response(self) -> None:
        segments, chapters = parse_transcript_panel(_load_panel())

        self.assertEqual(len(segments), 8)
        self.assertEqual(
            segments[0],
            {
                'start_time': 0,
                'text': (
                    'How pirates actually slept on a freezing ship '
                    "without heating. Hey guys, tonight we're climbing "
                    'into a hammock aboard a pirate ship in the middle '
                    'of the North'
                ),
            },
        )
        self.assertEqual(
            [segment['start_time'] for segment in segments],
            [0, 8, 18, 25, 138, 148, 157, 166],
        )
        self.assertEqual(
            chapters,
            [
                {'start_time': 0, 'title': 'Chapter 1: Life in the hammock'},
                {
                    'start_time': 136,
                    'title': 'Chapter 2: The challenges of cold',
                },
            ],
        )

    def test_without_chapters(self) -> None:
        segments, chapters = parse_transcript_panel(
            {'items': [_segment_item(0, 'one'), _segment_item(4, 'two')]}
        )
        self.assertEqual(
            segments,
            [
                {'start_time': 0, 'text': 'one'},
                {'start_time': 4, 'text': 'two'},
            ],
        )
        self.assertEqual(chapters, [])

    def test_sorts_by_start_time_keeping_order_of_equal_times(self) -> None:
        segments, chapters = parse_transcript_panel({
            'items': [
                _segment_item(9, 'late'), _segment_item(1, 'first'),
                _segment_item(1, 'second'),
                _chapter_item(5, 'B'), _chapter_item(0, 'A'),
            ],
        })
        self.assertEqual(
            [segment['text'] for segment in segments],
            ['first', 'second', 'late'],
        )
        self.assertEqual(
            [chapter['title'] for chapter in chapters], ['A', 'B'],
        )

    def test_ignores_incomplete_items(self) -> None:
        no_start: dict[str, Any] = _segment_item(0, 'no start time')
        del no_start['macroMarkersPanelItemViewModel']['onTap']
        bool_start: dict[str, Any] = _segment_item(True, 'bool')
        blank: dict[str, Any] = _segment_item(3, '   ')
        untitled: dict[str, Any] = _chapter_item(3, '')

        segments, chapters = parse_transcript_panel(
            {'items': [no_start, bool_start, blank, untitled]}
        )
        self.assertEqual(segments, [])
        self.assertEqual(chapters, [])

    def test_empty_and_unexpected_responses(self) -> None:
        for data in ({}, {'content': None}, {'items': [None, 1, 'x']}):
            self.assertEqual(parse_transcript_panel(data), ([], []))


class TestVideoTranscriptFields(unittest.TestCase):
    def _video(self) -> YouTubeVideo:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        video.url = f'https://www.youtube.com/watch?v={VIDEO_ID}'
        video.channel_id = 'UCabcdefghijABCDEFGHIJ'
        video.embed_url = f'https://www.youtube.com/embed/{VIDEO_ID}'
        video.transcript = [
            {'start_time': 0, 'text': 'one'},
            {'start_time': 4, 'text': 'two'},
        ]
        video.transcript_chapters = [{'start_time': 0, 'title': 'A'}]
        video.transcript_language = 'nl-NL'
        return video

    def test_default_is_empty_and_omitted_from_dict(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        self.assertEqual(video.transcript, [])
        self.assertEqual(video.transcript_chapters, [])
        data: dict[str, Any] = video.to_dict()
        self.assertNotIn('transcript', data)
        self.assertNotIn('transcript_chapters', data)
        self.assertNotIn('transcript_language', data)

    def test_roundtrip(self) -> None:
        video: YouTubeVideo = self._video()
        data: dict[str, Any] = video.to_dict()
        self.assertEqual(data['transcript'], video.transcript)
        self.assertEqual(
            data['transcript_chapters'], video.transcript_chapters,
        )

        restored: YouTubeVideo = YouTubeVideo.from_dict(
            json.loads(json.dumps(data))
        )
        self.assertEqual(restored.transcript, video.transcript)
        self.assertEqual(
            restored.transcript_chapters, video.transcript_chapters,
        )
        self.assertEqual(data['transcript_language'], 'nl-NL')
        self.assertEqual(restored.transcript_language, 'nl-NL')

    def test_equality_considers_transcript(self) -> None:
        video: YouTubeVideo = self._video()
        other: YouTubeVideo = YouTubeVideo.from_dict(video.to_dict())
        self.assertEqual(video, other)
        other.transcript = other.transcript[:1]
        self.assertNotEqual(video, other)
        other = YouTubeVideo.from_dict(video.to_dict())
        other.transcript_chapters = []
        self.assertNotEqual(video, other)
        other = YouTubeVideo.from_dict(video.to_dict())
        other.transcript_language = 'en'
        self.assertNotEqual(video, other)

    def test_from_dict_without_transcript(self) -> None:
        data: dict[str, Any] = self._video().to_dict()
        del data['transcript']
        del data['transcript_chapters']
        del data['transcript_language']
        video: YouTubeVideo = YouTubeVideo.from_dict(data)
        self.assertIsNone(video.transcript_language)
        self.assertEqual(video.transcript, [])
        self.assertEqual(video.transcript_chapters, [])


class TestVideoSchema(unittest.TestCase):
    def setUp(self) -> None:
        schema: dict[str, Any] = json.loads(
            (COLLATERAL / 'boinko-youtube-video-schema.json').read_text()
        )
        Draft202012Validator.check_schema(schema)
        self.validator: Draft202012Validator = Draft202012Validator(schema)

    def _data(self) -> dict[str, Any]:
        return {
            'video_id': VIDEO_ID,
            'url': f'https://www.youtube.com/watch?v={VIDEO_ID}',
            'channel_id': 'UCabcdefghijABCDEFGHIJ',
        }

    def test_transcript_is_optional(self) -> None:
        self.validator.validate(self._data())

    def test_valid_transcript(self) -> None:
        data: dict[str, Any] = self._data() | {
            'transcript': [
                {'start_time': 0, 'text': 'one'},
                {'start_time': 4, 'text': 'two'},
            ],
            'transcript_chapters': [{'start_time': 0, 'title': 'A'}],
            'transcript_language': 'nl-NL',
        }
        self.validator.validate(data)

    def test_invalid_transcripts(self) -> None:
        invalid: list[dict[str, Any]] = [
            {'transcript': {'start_time': 0, 'text': 'x'}},
            {'transcript': [{'start_time': 0}]},
            {'transcript': [{'text': 'x'}]},
            {'transcript': [{'start_time': -1, 'text': 'x'}]},
            {'transcript': [{'start_time': 1.5, 'text': 'x'}]},
            {'transcript': [{'start_time': 0, 'text': ''}]},
            {'transcript': [{'start_time': 0, 'text': 'x', 'end': 1}]},
            {'transcript_language': ''},
            {'transcript_language': None},
            {'transcript_chapters': [{'start_time': 0}]},
            {'transcript_chapters': [{'start_time': 0, 'title': ''}]},
        ]
        for extra in invalid:
            with self.subTest(extra=extra):
                self.assertFalse(
                    self.validator.is_valid(self._data() | extra)
                )

    def test_video_with_transcript_validates(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        video.url = f'https://www.youtube.com/watch?v={VIDEO_ID}'
        video.channel_id = 'UCabcdefghijABCDEFGHIJ'
        segments, chapters = parse_transcript_panel(_load_panel())
        video.transcript = segments
        video.transcript_chapters = chapters
        data: dict[str, Any] = json.loads(json.dumps(video.to_dict()))
        errors: list[str] = [
            error.message for error in self.validator.iter_errors(data)
        ]
        # Other fields of a mostly empty video may not validate; only
        # the transcript fields matter here
        self.assertFalse(
            [error for error in errors if 'transcript' in error], errors,
        )
        self.assertTrue(
            self.validator.is_valid(
                {k: data[k] for k in (
                    'video_id', 'url', 'channel_id', 'transcript',
                    'transcript_chapters',
                )}
            )
        )


class TestScrapeTranscript(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.limiter: MagicMock = MagicMock()
        self.limiter.acquire = AsyncMock(return_value=PROXY)
        self.limiter.penalise = AsyncMock()
        self.limiter.report_proxy_result = AsyncMock()

        patcher = patch.object(
            innertube_mod.YouTubeRateLimiter, 'get',
            return_value=self.limiter,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.innertube: MagicMock = MagicMock()
        self.video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        self.parser: InnerTubeVideoParser = InnerTubeVideoParser(
            self.video, self.innertube, PROXY,
        )

    async def _scrape(
        self, next_data: dict[str, Any], call: AsyncMock,
    ) -> None:
        with patch.object(innertube_mod, '_call_innertube', new=call):
            await self.parser._scrape_transcript(next_data, PROXY)

    async def test_success(self) -> None:
        before: float = _count('success', 'none')
        call: AsyncMock = AsyncMock(return_value=_load_panel())

        await self._scrape(_next_data(), call)

        self.assertEqual(len(self.video.transcript), 8)
        self.assertEqual(len(self.video.transcript_chapters), 2)
        self.assertEqual(_count('success', 'none'), before + 1)
        self.limiter.acquire.assert_awaited_once()
        self.assertEqual(
            self.limiter.acquire.await_args.args,
            (innertube_mod.YouTubeCallType.TRANSCRIPT,),
        )
        client, function, endpoint, params, body = call.await_args.args
        self.assertIs(client, self.innertube)
        self.assertIs(function, self.innertube.adaptor.dispatch)
        self.assertEqual(endpoint, 'get_panel')
        self.assertIsNone(params)
        self.assertEqual(body, build_panel_body(VIDEO_ID))

    async def test_requests_transcript_in_audio_language(self) -> None:
        call: AsyncMock = AsyncMock(return_value=_load_panel())

        with patch.object(innertube_mod, '_call_innertube', new=call):
            await self.parser._scrape_transcript(
                _next_data(), PROXY, language='nl',
                available_languages=['nl-NL', 'en'],
            )

        body: dict[str, Any] = call.await_args.args[4]
        self.assertEqual(body, build_panel_body(VIDEO_ID, 'nl'))
        self.assertEqual(body['context'], {'client': {'hl': 'nl'}})
        self.assertEqual(self.video.transcript_language, 'nl-NL')

    async def test_language_unknown_without_matching_track(self) -> None:
        call: AsyncMock = AsyncMock(return_value=_load_panel())

        with patch.object(innertube_mod, '_call_innertube', new=call):
            await self.parser._scrape_transcript(
                _next_data(), PROXY, language='de',
                available_languages=['nl-NL', 'en'],
            )

        self.assertEqual(len(self.video.transcript), 8)
        self.assertIsNone(self.video.transcript_language)

    async def test_default_language_is_english(self) -> None:
        call: AsyncMock = AsyncMock(return_value=_load_panel())

        with patch.object(innertube_mod, '_call_innertube', new=call):
            await self.parser._scrape_transcript(
                _next_data(), PROXY, available_languages=['en'],
            )

        self.assertNotIn('context', call.await_args.args[4])
        self.assertEqual(self.video.transcript_language, 'en')

    async def test_no_transcript_panel_skips_request(self) -> None:
        before: float = _count('unavailable', 'no_panel')
        call: AsyncMock = AsyncMock()

        await self._scrape(_next_data(False), call)

        call.assert_not_awaited()
        self.limiter.acquire.assert_not_awaited()
        self.assertEqual(self.video.transcript, [])
        self.assertEqual(_count('unavailable', 'no_panel'), before + 1)

    async def test_empty_panel_is_unavailable(self) -> None:
        before: float = _count('unavailable', 'empty')

        await self._scrape(_next_data(), AsyncMock(return_value={}))

        self.assertEqual(self.video.transcript, [])
        self.assertEqual(_count('unavailable', 'empty'), before + 1)

    async def test_unparsable_panel_is_a_failure(self) -> None:
        before: float = _count('failure', 'parse_error')

        with patch.object(
            innertube_mod, 'parse_transcript_panel',
            side_effect=ValueError('boom'),
        ):
            await self._scrape(_next_data(), AsyncMock(return_value={}))

        self.assertEqual(self.video.transcript, [])
        self.assertEqual(_count('failure', 'parse_error'), before + 1)

    async def test_rate_limited_response(self) -> None:
        before: float = _count('failure', 'rate_limited')
        error: InnerTubeRequestError = InnerTubeRequestError(
            InnerTubeError(code=429, message='slow down', reason='quota'),
        )

        await self._scrape(_next_data(), AsyncMock(side_effect=error))

        self.assertEqual(self.video.transcript, [])
        self.assertEqual(_count('failure', 'rate_limited'), before + 1)
        self.limiter.penalise.assert_awaited_once_with(
            innertube_mod.YouTubeCallType.TRANSCRIPT, PROXY,
            innertube_mod._TRANSCRIPT_PENALTY_SECONDS,
        )

    async def test_http_error_response(self) -> None:
        before: float = _count('failure', 'http_error')
        error: InnerTubeRequestError = InnerTubeRequestError(
            InnerTubeError(
                code=400, message='Precondition check failed.',
                reason='failedPrecondition',
            ),
        )

        await self._scrape(_next_data(), AsyncMock(side_effect=error))

        self.assertEqual(_count('failure', 'http_error'), before + 1)
        self.limiter.penalise.assert_not_awaited()

    async def test_rate_limit_wait_exceeded(self) -> None:
        before: float = _count('failure', 'rate_wait')
        self.limiter.acquire = AsyncMock(
            side_effect=RateLimitWaitExceeded(
                'transcript', PROXY, 20.0, 15.0,
            ),
        )
        call: AsyncMock = AsyncMock()

        await self._scrape(_next_data(), call)

        call.assert_not_awaited()
        self.assertEqual(_count('failure', 'rate_wait'), before + 1)

    async def test_wait_is_capped(self) -> None:
        call: AsyncMock = AsyncMock(return_value=_load_panel())
        with patch.object(innertube_mod, '_call_innertube', new=call):
            await self.parser._scrape_transcript(_next_data(), PROXY, 2.0)
            await self.parser._scrape_transcript(_next_data(), PROXY, 999.0)

        waits: list[float] = [
            args.kwargs['max_wait']
            for args in self.limiter.acquire.await_args_list
        ]
        self.assertEqual(
            waits, [2.0, innertube_mod._TRANSCRIPT_MAX_WAIT_SECONDS],
        )

    async def test_unexpected_exception_is_swallowed(self) -> None:
        before: float = _count('failure', 'other')

        await self._scrape(
            _next_data(), AsyncMock(side_effect=RuntimeError('boom')),
        )

        self.assertEqual(self.video.transcript, [])
        self.assertEqual(_count('failure', 'other'), before + 1)

    async def test_timeout_is_swallowed(self) -> None:
        before: float = _count('failure', 'timeout')

        await self._scrape(
            _next_data(), AsyncMock(side_effect=TimeoutError()),
        )

        self.assertEqual(_count('failure', 'timeout'), before + 1)

    async def test_transport_error_reports_proxy_failure(self) -> None:
        import httpx

        before: float = _count('failure', 'transport')
        error: httpx.ConnectError = httpx.ConnectError('refused')

        await self._scrape(_next_data(), AsyncMock(side_effect=error))

        self.assertEqual(_count('failure', 'transport'), before + 1)
        self.limiter.report_proxy_result.assert_awaited_once_with(
            PROXY, False, error,
        )

    async def test_borrowed_client_is_released(self) -> None:
        parser: InnerTubeVideoParser = InnerTubeVideoParser(
            self.video, None, PROXY,
        )
        client: MagicMock = MagicMock()
        with (
            patch.object(
                innertube_mod, 'borrow_pooled_innertube_for_entry',
                return_value=client,
            ) as borrow,
            patch.object(
                innertube_mod, 'release_pooled_innertube',
                new=AsyncMock(),
            ) as release,
            patch.object(
                innertube_mod, '_call_innertube',
                new=AsyncMock(side_effect=RuntimeError('boom')),
            ),
        ):
            await parser._scrape_transcript(_next_data(), PROXY)

        borrow.assert_called_once_with(PROXY)
        release.assert_awaited_once_with(client)


class TestScrapeKeepsVideoOnTranscriptProblems(
    unittest.IsolatedAsyncioTestCase,
):
    async def _scrape(
        self, panel: Any, next_data: dict[str, Any] | None = None,
    ) -> YouTubeVideo:
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=PROXY)
        limiter.penalise = AsyncMock()
        limiter.report_proxy_result = AsyncMock()

        async def fake_call(
            client: Any, function: Any, *args: Any,
        ) -> dict[str, Any]:
            if function is client.player:
                return _player_data()
            if function is client.next:
                return _next_data() if next_data is None else next_data
            if isinstance(panel, Exception):
                raise panel
            return panel

        with (
            patch.object(
                innertube_mod.YouTubeRateLimiter, 'get',
                return_value=limiter,
            ),
            patch.object(innertube_mod, '_call_innertube', new=fake_call),
        ):
            return await InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id=VIDEO_ID),
                innertube=MagicMock(), proxy=PROXY,
            )

    async def test_transcript_is_added_to_video(self) -> None:
        video: YouTubeVideo = await self._scrape(_load_panel())

        self.assertEqual(video.title, 'A title')
        self.assertEqual(len(video.transcript), 8)
        self.assertEqual(len(video.transcript_chapters), 2)

    async def test_audio_language_drives_transcript_request(self) -> None:
        captured: list[Any] = []
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=PROXY)
        limiter.penalise = AsyncMock()
        limiter.report_proxy_result = AsyncMock()
        player: dict[str, Any] = _player_data() | _player_with_tracks(
            ('nl-NL', None), ('nl', 'asr'),
        )

        async def fake_call(
            client: Any, function: Any, *args: Any,
        ) -> dict[str, Any]:
            if function is client.player:
                return player
            if function is client.next:
                return _next_data()
            captured.append(args)
            return _load_panel()

        with (
            patch.object(
                innertube_mod.YouTubeRateLimiter, 'get',
                return_value=limiter,
            ),
            patch.object(innertube_mod, '_call_innertube', new=fake_call),
        ):
            video: YouTubeVideo = await InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id=VIDEO_ID),
                innertube=MagicMock(), proxy=PROXY,
            )

        self.assertEqual(video.default_audio_language, 'nl')
        self.assertEqual(captured[0][2]['context'], {'client': {'hl': 'nl'}})
        # An exact match on the audio language is preferred
        self.assertEqual(video.transcript_language, 'nl')
        self.assertEqual(video.to_dict()['default_audio_language'], 'nl')

    async def test_failing_transcript_does_not_fail_video(self) -> None:
        video: YouTubeVideo = await self._scrape(RuntimeError('boom'))

        self.assertEqual(video.title, 'A title')
        self.assertEqual(video.transcript, [])

    async def test_video_without_transcript(self) -> None:
        video: YouTubeVideo = await self._scrape(
            deepcopy(_load_panel()), _next_data(False),
        )

        self.assertEqual(video.title, 'A title')
        self.assertEqual(video.transcript, [])


if __name__ == '__main__':
    unittest.main()
