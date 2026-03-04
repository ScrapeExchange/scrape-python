#!/usr/bin/env python3

'''
Scrapes metadata for a YouTube video using the InnerTube API.

Collects: audio/video/muxed stream formats, chapters, subtitles and
auto-generated captions, view/like/comment counts, live status, and
content metadata (keywords, categories, tags).

Does not use yt-dlp. Uses the innertube library for InnerTube API calls
and AsyncYouTubeClient for supplementary HTML page fetches.

Usage:
    python tools/yt_vid_scrape.py <video_id> [--output FILE] [--log-level LVL]

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
import sys
import asyncio
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor

import innertube
import orjson

from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient

_LOGGER: logging.Logger = logging.getLogger(__name__)

VIDEO_URL: str = 'https://www.youtube.com/watch?v={video_id}'
EMBED_URL: str = 'https://www.youtube.com/embed/{video_id}'


# ---------------------------------------------------------------------------
# InnerTube calls — synchronous library, run in a thread-pool executor
# ---------------------------------------------------------------------------

def _innertube_player(video_id: str) -> dict:
    return innertube.InnerTube('WEB').player(video_id)


def _innertube_next(video_id: str) -> dict:
    return innertube.InnerTube('WEB').next(video_id)


async def _fetch_player(video_id: str, executor: ThreadPoolExecutor) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, _innertube_player, video_id)


async def _fetch_next(video_id: str, executor: ThreadPoolExecutor) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, _innertube_next, video_id)


# ---------------------------------------------------------------------------
# Text / count helpers
# ---------------------------------------------------------------------------

def _text(obj: dict | str | None) -> str | None:
    '''Extract plain text from a YouTube API text object.'''
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    if 'simpleText' in obj:
        return obj['simpleText']
    if 'runs' in obj:
        return ''.join(r.get('text', '') for r in obj['runs'])
    return None


def _parse_count(text: str | None) -> int | None:
    '''
    Parse a YouTube count string to int.
    Handles "1.2M", "42,069 likes", "1.5K views", plain integers.
    '''
    if not text:
        return None
    token = text.strip().split()[0].replace(',', '').upper()
    try:
        if token.endswith('K'):
            return int(float(token[:-1]) * 1_000)
        if token.endswith('M'):
            return int(float(token[:-1]) * 1_000_000)
        if token.endswith('B'):
            return int(float(token[:-1]) * 1_000_000_000)
        digits = re.sub(r'\D.*$', '', token)
        return int(digits) if digits else None
    except (ValueError, TypeError):
        return None


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _find(contents: list[dict], key: str) -> dict | None:
    '''Return the value for key in the first item of contents that has it.'''
    for item in contents:
        if key in item:
            return item[key]
    return None


# ---------------------------------------------------------------------------
# Stream format parsing
# ---------------------------------------------------------------------------

def _parse_mime(mime_type: str) -> tuple[str, str]:
    '''
    Split 'video/mp4; codecs="avc1.640028"' into ('video/mp4', 'avc1.640028').
    '''
    if not mime_type:
        return '', ''
    parts = mime_type.split(';', 1)
    base = parts[0].strip()
    codecs = ''
    if len(parts) > 1 and 'codecs=' in parts[1]:
        codecs = parts[1].split('codecs=', 1)[1].strip().strip('"')
    return base, codecs


def parse_formats(streaming_data: dict) -> list[dict]:
    '''
    Parse all audio and video tracks from streamingData.

    Both muxed formats (video+audio combined) and adaptive formats (separate
    video or audio) are included. The 'track_type' field is one of:
    'video', 'audio', 'muxed'.

    :param streaming_data: the streamingData dict from a player() response.
    :returns: list of dicts, one per track.
    '''

    def _one(fmt: dict, track_type: str) -> dict:
        mime_type = fmt.get('mimeType', '')
        base_mime, codecs = _parse_mime(mime_type)
        return {
            'itag': fmt.get('itag'),
            'track_type': track_type,
            'mime_type': base_mime,
            'codecs': codecs,
            'quality': fmt.get('quality'),
            'quality_label': fmt.get('qualityLabel'),
            'audio_quality': fmt.get('audioQuality'),
            'bitrate': fmt.get('bitrate'),
            'average_bitrate': fmt.get('averageBitrate'),
            'width': fmt.get('width'),
            'height': fmt.get('height'),
            'fps': fmt.get('fps'),
            'audio_sample_rate': _safe_int(fmt.get('audioSampleRate')),
            'audio_channels': fmt.get('audioChannels'),
            'loudness_db': fmt.get('loudnessDb'),
            'duration_ms': _safe_int(fmt.get('approxDurationMs')),
            'content_length_bytes': _safe_int(fmt.get('contentLength')),
            'has_drm': bool(fmt.get('drmFamilies')),
            # url absent when signatureCipher is present (requires deciphering)
            'url': fmt.get('url'),
            'has_cipher': 'signatureCipher' in fmt or 'cipher' in fmt,
        }

    result: list[dict] = []

    for fmt in streaming_data.get('formats', []):
        result.append(_one(fmt, 'muxed'))

    for fmt in streaming_data.get('adaptiveFormats', []):
        mime = fmt.get('mimeType', '')
        if mime.startswith('audio/'):
            track_type = 'audio'
        elif mime.startswith('video/'):
            track_type = 'video'
        else:
            track_type = 'adaptive'
        result.append(_one(fmt, track_type))

    return result


# ---------------------------------------------------------------------------
# Caption parsing
# ---------------------------------------------------------------------------

def parse_captions(captions_data: dict) -> dict[str, list[dict]]:
    '''
    Parse caption / subtitle tracks from the captions dict.

    :param captions_data: the captions dict from a player() response.
    :returns: dict with keys 'subtitles' and 'automatic_captions', each a
              list of {language_code, language_name, url, vss_id} dicts.
    '''
    subtitles: list[dict] = []
    auto_captions: list[dict] = []

    renderer = captions_data.get('playerCaptionsTracklistRenderer', {})
    for track in renderer.get('captionTracks', []):
        entry = {
            'language_code': track.get('languageCode', 'unknown'),
            'language_name': _text(track.get('name')),
            'url': track.get('baseUrl'),
            'vss_id': track.get('vssId'),
        }
        if track.get('kind') == 'asr':
            auto_captions.append(entry)
        else:
            subtitles.append(entry)

    return {
        'subtitles': subtitles,
        'automatic_captions': auto_captions,
    }


# ---------------------------------------------------------------------------
# Chapter parsing
# ---------------------------------------------------------------------------

def _parse_chapters_from_player(player_data: dict) -> list[dict]:
    '''
    Extract chapters from playerOverlays in a player() response.

    :returns: list of {title, start_seconds, thumbnail_url} dicts.
    '''
    chapters: list[dict] = []
    try:
        markers_map = (
            player_data
            .get('playerOverlays', {})
            .get('playerOverlayRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('playerBar', {})
            .get('multiMarkersPlayerBarRenderer', {})
            .get('markersMap', [])
        )
        for marker in markers_map:
            for chapter in marker.get('value', {}).get('chapters', []):
                r = chapter.get('chapterRenderer', {})
                title_runs = r.get('title', {}).get('runs', [])
                title = title_runs[0].get('text', '') if title_runs else ''
                start_ms = r.get('timeRangeStartMillis', 0)
                thumbs = r.get('thumbnail', {}).get('thumbnails', [])
                best_thumb = (
                    max(thumbs, key=lambda t: t.get('width', 0), default={})
                    .get('url')
                )
                chapters.append({
                    'title': title,
                    'start_seconds': start_ms / 1000,
                    'thumbnail_url': best_thumb,
                })
    except (KeyError, TypeError, IndexError):
        pass
    return chapters




# ---------------------------------------------------------------------------
# next() response parsing
# ---------------------------------------------------------------------------

def _extract_like_count(primary: dict) -> int | None:
    '''
    Try multiple known YouTube API paths to extract the like count from
    videoPrimaryInfoRenderer. YouTube reorganises this path frequently.
    '''
    try:
        buttons = (
            primary
            .get('videoActions', {})
            .get('menuRenderer', {})
            .get('topLevelButtons', [])
        )
        for btn in buttons:
            # Older path: segmentedLikeDislikeButtonRenderer
            sldb = btn.get('segmentedLikeDislikeButtonRenderer', {})
            like_toggle = sldb.get('likeButton', {}).get('toggleButtonRenderer', {})
            text = _text(like_toggle.get('defaultText'))
            if text:
                count = _parse_count(text)
                if count is not None:
                    return count

            # Newer path: segmentedLikeDislikeButtonViewModel
            vm = (
                btn
                .get('segmentedLikeDislikeButtonViewModel', {})
                .get('likeButtonViewModel', {})
                .get('likeButtonViewModel', {})
                .get('toggleButtonViewModel', {})
                .get('toggleButtonViewModel', {})
                .get('defaultButtonViewModel', {})
                .get('buttonViewModel', {})
            )
            label = vm.get('accessibilityText', '')
            if label:
                return _parse_count(label)
    except (KeyError, TypeError):
        pass
    return None







# ---------------------------------------------------------------------------
# Top-level scrape
# ---------------------------------------------------------------------------

async def scrape_video(video_id: str) -> dict:
    '''
    Scrape all available metadata for a YouTube video.

    Fires player() and next() InnerTube API calls concurrently in a thread
    pool (the innertube library is synchronous), alongside an async watch-page
    fetch via AsyncYouTubeClient.

    :param video_id: YouTube video ID.
    :returns: dict with all collected metadata.
    :raises RuntimeError: if the player endpoint call fails.
    '''



    # Chapters: prefer next() (has human-readable time text), fall back to
    # playerOverlays embedded in the player() response

    return {
        'like_count': next_parsed.get('like_count'),
        'comment_count': comment_count,

        # Live status
        'is_live': bool(video_details.get('isLive', False)),
        # isLiveContent is true for both currently-live and past-live videos
        'was_live': bool(video_details.get('isLiveContent', False)),

        # Content metadata
        'keywords': video_details.get('keywords', []),
        # tags is a separate field some videos populate (often overlaps keywords)
        'tags': video_details.get('tags', []),
        'categories': categories,
        # YouTube discontinued in-video annotations in January 2019
        'annotations': [],

        # Safety / availability
        'is_family_safe': microformat.get('isFamilySafe'),
        'is_age_restricted': playability.get('status') == 'LOGIN_REQUIRED',
        'availability_status': playability.get('status'),

        # Thumbnails (from microformat; highest-res last)
        'thumbnails': [
            {
                'url': t.get('url'),
                'width': t.get('width'),
                'height': t.get('height'),
            }
            for t in microformat.get('thumbnail', {}).get('thumbnails', [])
        ],

        # Stream formats (audio, video, muxed)
        'formats': parse_formats(streaming_data),

        # Captions / subtitles
        'subtitles': captions['subtitles'],
        'automatic_captions': captions['automatic_captions'],

        # Chapters
        'chapters': chapters,
    }

