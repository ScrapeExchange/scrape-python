'''
Helpers for YouTube's transcript panel, the "Show transcript" section
below a video.

The panel is not the same thing as the caption tracks: it is fetched
with InnerTube ``get_panel`` and, for videos with chapters, includes the
chapter headings. The ``params`` for the call are a base64 encoded
protobuf message that only contains the video ID, so they can be built
without scraping them from the watch page.

:author: Boinko <boinko@scrape.exchange>
:copyright: 2026 Boinko
:license: GPL-3.0
'''

import base64

from typing import Any

TRANSCRIPT_PANEL_ID: str = 'PAmodern_transcript_view'

# protobuf: field 149 (length-delimited) wrapping
# {1: <video_id>, 3: 2}. Field 3 selects the modern transcript view.
_PARAMS_PREFIX: bytes = b'\xaa\x09'
_PARAMS_SUFFIX: bytes = b'\x18\x02'

_ENDPOINT_KEY: str = 'getTranscriptEndpoint'

TranscriptSegment = dict[str, int | str]
TranscriptChapter = dict[str, int | str]


def build_panel_params(video_id: str) -> str:
    '''
    Build the ``params`` value for an InnerTube ``get_panel`` request
    for the transcript panel of a video

    :param video_id: the 11 character YouTube video ID
    :returns: the base64 encoded protobuf message
    :raises: ValueError if the video ID is empty or not ASCII
    '''

    if not video_id or not video_id.isascii():
        raise ValueError(f'Invalid video ID: {video_id!r}')

    video_id_field: bytes = (
        b'\x0a' + bytes([len(video_id)]) + video_id.encode('ascii')
    )
    message: bytes = video_id_field + _PARAMS_SUFFIX

    return base64.b64encode(
        _PARAMS_PREFIX + bytes([len(message)]) + message
    ).decode('ascii')


def build_panel_body(
    video_id: str, language: str | None = None,
) -> dict[str, Any]:
    '''
    Request body for ``get_panel``.

    The panel serves the caption track that matches the ``hl`` of the
    request and falls back to the default track (usually English) if
    the video has no track in that language. It never translates. The
    client context of the InnerTube client is merged into the body, so
    ``language`` only takes effect for clients without a locale of
    their own, which is the case for the pooled Web client.

    :param video_id: the 11 character YouTube video ID
    :param language: BCP-47 language code to request the transcript in
    '''

    body: dict[str, Any] = {
        'panelId': TRANSCRIPT_PANEL_ID,
        'params': build_panel_params(video_id),
    }
    if language:
        body['context'] = {'client': {'hl': language}}

    return body


def detect_audio_language(player_data: dict[str, Any]) -> str | None:
    '''
    Find the language that is spoken in the video from a ``player``
    response: ``defaultAudioLanguage`` of the microformat if present,
    otherwise the language of the auto-generated (``asr``) caption
    track, which is always in the spoken language.

    :returns: the language code or None if it can not be determined,
    for example when the uploader only provided manual subtitles
    '''

    language: Any = _dig(
        player_data, 'microformat', 'playerMicroformatRenderer',
        'defaultAudioLanguage',
    )
    if isinstance(language, str) and language:
        return language

    for track in _caption_tracks(player_data):
        code: Any = track.get('languageCode')
        if track.get('kind') == 'asr' and isinstance(code, str) and code:
            return code

    return None


def caption_languages(player_data: dict[str, Any]) -> list[str]:
    '''
    Language codes of the caption tracks in a ``player`` response
    '''

    return [
        track['languageCode'] for track in _caption_tracks(player_data)
        if isinstance(track.get('languageCode'), str)
        and track['languageCode']
    ]


def _dig(data: Any, *keys: str) -> Any:
    '''
    Nested ``dict.get()`` that returns None instead of raising when a
    level is missing, null or not a dict
    '''

    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key)

    return data


def _caption_tracks(player_data: dict[str, Any]) -> list[dict[str, Any]]:
    tracks: Any = _dig(
        player_data, 'captions', 'playerCaptionsTracklistRenderer',
        'captionTracks',
    )
    if not isinstance(tracks, list):
        return []

    return [track for track in tracks if isinstance(track, dict)]


def match_language(available: list[str], requested: str) -> str | None:
    '''
    The caption track language that the panel serves for a request in
    *requested*: an exact match, otherwise a track with the same
    primary language subtag (``nl`` matches ``nl-NL`` and vice versa).
    The panel response does not say which language it contains, so this
    is how the language of a transcript is derived.

    :returns: the language code of the matching track or None
    '''

    wanted: str = requested.lower()
    for code in available:
        if code.lower() == wanted:
            return code

    primary: str = wanted.replace('_', '-').split('-')[0]
    for code in available:
        if code.lower().replace('_', '-').split('-')[0] == primary:
            return code

    return None


def has_transcript_panel(next_data: dict[str, Any] | None) -> bool:
    '''
    Whether the ``next`` response of a video advertises a transcript.
    Videos without one (live streams, premieres, no captions) lack the
    endpoint, which saves the ``get_panel`` request.
    '''

    return bool(next_data) and _contains_key(next_data, _ENDPOINT_KEY)


def _contains_key(data: Any, key: str) -> bool:
    if isinstance(data, dict):
        if key in data:
            return True
        return any(_contains_key(value, key) for value in data.values())
    if isinstance(data, list):
        return any(_contains_key(value, key) for value in data)

    return False


def parse_transcript_panel(
    panel_data: dict[str, Any],
) -> tuple[list[TranscriptSegment], list[TranscriptChapter]]:
    '''
    Extract the transcript from a ``get_panel`` response

    :param panel_data: the JSON response of ``get_panel``
    :returns: tuple with the segments and the chapters, both ordered by
    start time. Segments are ``{'start_time': <seconds>, 'text': str}``,
    chapters are ``{'start_time': <seconds>, 'title': str}``. YouTube
    only provides start times with a resolution of one second.
    '''

    segments: list[TranscriptSegment] = []
    chapters: list[TranscriptChapter] = []
    _collect(panel_data, segments, chapters)

    # The panel lists some chapter headings out of order
    segments.sort(key=lambda segment: segment['start_time'])
    chapters.sort(key=lambda chapter: chapter['start_time'])

    return segments, chapters


def _collect(
    data: Any, segments: list[TranscriptSegment],
    chapters: list[TranscriptChapter],
) -> None:
    if isinstance(data, list):
        for value in data:
            _collect(value, segments, chapters)
        return
    if not isinstance(data, dict):
        return

    item: Any = data.get('macroMarkersPanelItemViewModel')
    if isinstance(item, dict):
        _collect_item(item, segments, chapters)
        return

    for value in data.values():
        _collect(value, segments, chapters)


def _collect_item(
    item: dict[str, Any], segments: list[TranscriptSegment],
    chapters: list[TranscriptChapter],
) -> None:
    start_time: Any = (
        item.get('onTap', {})
        .get('innertubeCommand', {})
        .get('watchEndpoint', {})
        .get('startTimeSeconds')
    )
    if not isinstance(start_time, int) or isinstance(start_time, bool):
        return

    content: dict[str, Any] = item.get('item') or {}

    chapter: Any = content.get('timelineChapterViewModel')
    if isinstance(chapter, dict):
        title: Any = chapter.get('title')
        if isinstance(title, str) and title:
            chapters.append({'start_time': start_time, 'title': title})
        return

    timeline_item: Any = content.get('timelineItemViewModel')
    if not isinstance(timeline_item, dict):
        return

    for content_item in timeline_item.get('contentItems') or []:
        segment: Any = (
            content_item.get('transcriptSegmentViewModel')
            if isinstance(content_item, dict) else None
        )
        if not isinstance(segment, dict):
            continue
        text: Any = segment.get('simpleText')
        if isinstance(text, str) and text.strip():
            segments.append(
                {'start_time': start_time, 'text': text.strip()}
            )
