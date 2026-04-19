'''
Uses InnerTube to parse data about a video. It is separate from YouTubeVideo class
to keep the source files to a managable length

:author     : boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import re
import time

from datetime import datetime
from innertube import InnerTube
from innertube.errors import RequestError as InnerTubeRequestError

from dateutil import parser as dateutil_parser

from .youtube_caption import YouTubeCaption
from scrape_exchange.worker_id import get_worker_id
from .youtube_client import METRIC_YT_REQUEST_DURATION
from .youtube_format import YouTubeFormat
from .youtube_cookiejar import YouTubeCookieJar
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_videochapter import YouTubeVideoChapter

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .youtube_video import YouTubeVideo



def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_timestamp(value: str) -> datetime | None:
    try:
        return dateutil_parser.parse(value)
    except (ValueError, TypeError):
        return None


def _text(obj: dict | str | None) -> str | None:
    '''
    Extract plain text from a YouTube API text object.
    '''
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
    token: str = text.strip().split()[0].replace(',', '').upper()
    try:
        if token.endswith('K'):
            return int(float(token[:-1]) * 1_000)
        if token.endswith('M'):
            return int(float(token[:-1]) * 1_000_000)
        if token.endswith('B'):
            return int(float(token[:-1]) * 1_000_000_000)
        digits: str = re.sub(r'\D.*$', '', token)
        return int(digits) if digits else None
    except (ValueError, TypeError):
        return None


def _find(contents: list[dict], key: str) -> dict | None:
    '''
    Return the value for key in the first item of contents that has it.
    '''
    for item in contents:
        if key in item:
            return item[key]
    return None


def _extract_comment_count(next_data: dict) -> int | None:
    '''
    Try to extract the comment count from engagementPanels.
    '''

    for panel in next_data.get('engagementPanels', []):
        renderer = panel.get('engagementPanelSectionListRenderer', {})
        if 'comments' not in renderer.get('panelIdentifier', '').lower():
            continue
        header = renderer.get('header', {})
        for header_key in (
            'engagementPanelTitleHeaderRenderer',
            'commentsEntryPointHeaderRenderer',
        ):
            count_text: str | None = _text(
                header.get(header_key, {}).get('contextualInfo')
            )
            if count_text:
                return _parse_count(count_text)
    return None


class InnerTubeVideoParser:
    def __init__(self, video: YouTubeVideo, innertube: InnerTube | None = None,
                 proxy: str | None = None) -> None:
        self.video: YouTubeVideo | None = video

        self.innertube: InnerTube
        if innertube:
            self.innertube = innertube
        else:
            if not proxy:
                logging.warning(
                    'No proxy configured, proceeding without proxy'
                )
            self.innertube = InnerTube('WEB', proxies=proxy)
            YouTubeCookieJar.get().load_into_session(
                self.innertube.adaptor.session, proxy
            )

    @staticmethod
    async def scrape(video: YouTubeVideo, innertube: InnerTube | None = None,
                     proxy: str | None = None,
                     max_retries: int = 4) -> None:
        self = InnerTubeVideoParser(video, innertube, proxy)
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()

        _PLAYER_PENALTY_INITIAL: float = 4.0
        _PLAYER_PENALTY_MAX: float = 300.0
        penalty: float = _PLAYER_PENALTY_INITIAL

        player_data: dict | None = None
        for attempt in range(1, max_retries + 1):
            proxy = await limiter.acquire(
                YouTubeCallType.PLAYER, proxy=proxy
            )
            start: float = time.monotonic()
            try:
                player_data = self.innertube.player(video.video_id)
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - start)
                break
            except InnerTubeRequestError as exc:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class=(
                        '4xx'
                        if exc.error.code == 429
                        else 'error'
                    ),
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - start)
                if exc.error.code == 429:
                    await limiter.penalise(
                        YouTubeCallType.PLAYER, proxy, penalty
                    )
                    await limiter.penalise(
                        YouTubeCallType.NEXT, proxy, penalty
                    )
                    logging.warning(
                        'InnerTube PLAYER 429 for %s (attempt %d/%d), '
                        'penalty %.1fs',
                        video.video_id, attempt, max_retries, penalty,
                    )
                    penalty = min(penalty * 2, _PLAYER_PENALTY_MAX)
                    if attempt == max_retries:
                        raise RuntimeError(
                            f'InnerTube PLAYER rate-limited after '
                            f'{max_retries} attempts: {exc}'
                        )
                    await asyncio.sleep(penalty)
                else:
                    raise RuntimeError(
                        f'InnerTube API call failed: {exc}'
                    )
            except Exception as exc:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - start)
                raise RuntimeError(f'InnerTube API call failed: {exc}')

        video_details: dict = player_data.get('videoDetails', {})
        captions_data: dict = player_data.get('captions', {})
        microformat: dict = player_data.get(
            'microformat', {}
        ).get(
            'playerMicroformatRenderer', {}
        )

        video.title = video_details.get('title', video.title)
        video.description = video_details.get(
            'shortDescription', video.description
        )
        video.url = microformat.get('canonicalUrl', video.url)
        video.embed_url = microformat.get(
            'embed', {}
        ).get('iframeUrl', video.embed_url)

        video.duration = _safe_int(video_details.get('lengthSeconds'))

        if microformat.get('category'):
            video.categories.add(microformat['category'])

        video.published_timestamp = _safe_timestamp(
            microformat.get('publishDate')
        )
        video.uploaded_timestamp = _safe_timestamp(
            microformat.get('uploadDate')
        )

        video.channel_id = video_details.get('channelId', video.channel_id)
        video.channel_name = video_details.get('author', video.channel_name)
        video.channel_url = microformat.get(
            'ownerProfileUrl', video.channel_url
        )
        video.is_live = bool(
            video_details.get('isLiveContent', video.is_live)
        )
        video.was_live = bool(
            video_details.get('isLiveContent', video.was_live)
        )
        if microformat.get('isUnlisted', False):
            video.privacy_status = 'private'
        else:
            video.privacy_status = 'public'

        video.keywords |= set(video_details.get('keywords', {}))
        video.tags |= set(video_details.get('tags', {}))
        video.available_country_codes |= set(
            microformat.get('availableCountries', [])
        )
        video.is_family_safe = microformat.get(
            'isFamilySafe', video.is_family_safe
        )

        video.view_count = _safe_int(video_details.get('viewCount'))
        video.like_count = _safe_int(microformat.get('likeCount'))
        playability: dict = player_data.get('playabilityStatus', {})
        video.age_restricted = (
            playability.get('status') == 'LOGIN_REQUIRED'
        )
        video.is_tv_film_video = video_details.get(
            'isTvFilmVideo', video.is_tv_film_video
        )
        thumbnails_data = video_details.get(
            'thumbnail', {}
        ).get('thumbnails', [])
        for thumbnail_data in thumbnails_data:
            thumbnail = YouTubeThumbnail(thumbnail_data)
            label: str = (
                thumbnail.id or f'{thumbnail.width}x{thumbnail.height}'
            )
            if label and label not in video.thumbnails:
                video.thumbnails[label] = thumbnail

        # Have not seen values for chapters in InnerTube output.
        # This is untested
        for chapter_data in video_details.get('chapters') or []:
            chapter = YouTubeVideoChapter(chapter_data)
            video.chapters.append(chapter)

        # Have not seen formats in InnerTube output. This is untested
        for format_data in video_details.get('formats') or []:
            video.formats.add(YouTubeFormat(format_data))

        if captions_data:
            parsed_captions: dict = InnerTubeVideoParser.parse_captions(
                captions_data
            )
            for entry in parsed_captions.get('subtitles', []):
                lang: str = entry.get('language_code', 'unknown')
                video.subtitles[lang] = YouTubeCaption(lang, entry)
            for entry in parsed_captions.get('automatic_captions', []):
                lang: str = entry.get('language_code', 'unknown')
                video.automatic_captions[lang] = YouTubeCaption(lang, entry)

        _next_penalty: float = _PLAYER_PENALTY_INITIAL
        for attempt in range(1, max_retries + 1):
            await limiter.acquire(YouTubeCallType.NEXT, proxy=proxy)
            next_start: float = time.monotonic()
            try:
                next_data: dict[str, any] = self.innertube.next(
                    video.video_id
                )
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - next_start)
                self._parse_next_data(next_data)
                break
            except InnerTubeRequestError as exc:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class=(
                        '4xx'
                        if exc.error.code == 429
                        else 'error'
                    ),
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - next_start)
                if exc.error.code == 429:
                    await limiter.penalise(
                        YouTubeCallType.NEXT, proxy, _next_penalty
                    )
                    await limiter.penalise(
                        YouTubeCallType.PLAYER, proxy, _next_penalty
                    )
                    logging.warning(
                        'InnerTube NEXT 429 for %s (attempt %d/%d), '
                        'penalty %.1fs',
                        video.video_id, attempt, max_retries, _next_penalty,
                    )
                    _next_penalty = min(
                        _next_penalty * 2, _PLAYER_PENALTY_MAX
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(_next_penalty)
                else:
                    break  # non-429 error on NEXT: skip silently
            except Exception:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                ).observe(time.monotonic() - next_start)
                break  # NEXT is best-effort; never fail the whole scrape

        return video

    @staticmethod
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
            entry: dict[str, any] = {
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

    @staticmethod
    def parse_innertube_chapters(player_data: dict
                                 ) -> list[YouTubeVideoChapter]:
        markers_map: list[dict[str, any]] = (
            player_data
            .get('playerOverlays', {})
            .get('playerOverlayRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('playerBar', {})
            .get('multiMarkersPlayerBarRenderer', {})
            .get('markersMap', [])
        )

        chapters: list[YouTubeVideoChapter] = []

        for marker in markers_map:
            for chapter in marker.get('value', {}).get('chapters', []):
                r: dict = chapter.get('chapterRenderer', {})
                title_runs: list = r.get('title', {}).get('runs', [])
                title: str = \
                    title_runs[0].get('text', '') if title_runs else ''
                start_ms: int = r.get('timeRangeStartMillis', 0)
                thumbs: list = r.get('thumbnail', {}).get('thumbnails', [])
                best_thumb_url: str = (
                    max(
                        thumbs, key=lambda t: t.get('width', 0), default={}
                    ).get('url')
                )
                if not (title and start_ms):
                    continue

                chapter = YouTubeVideoChapter(
                    {
                        'title': title,
                        'start_time': start_ms/1000,
                        'thumbnail_url': best_thumb_url
                    }
                )
                chapters.append(chapter)

        return chapters

    @staticmethod
    def _extract_like_count(primary: dict) -> int | None:
        '''
        Try multiple known YouTube API paths to extract the like count from
        videoPrimaryInfoRenderer. YouTube reorganises this path frequently.
        '''
        buttons = primary.get(
            'videoActions', {}
        ).get(
            'menuRenderer', {}
        ).get(
            'topLevelButtons', []
        )
        for btn in buttons:
            # Older path: segmentedLikeDislikeButtonRenderer
            like_toggle: str | None = _text(
                btn.get(
                    'segmentedLikeDislikeButtonRenderer', {}
                ).get(
                    'likeButton', {}
                ).get(
                    'toggleButtonRenderer', {}
                ).get(
                    'defaultText'
                )
            )
            if like_toggle:
                count: int | None = _parse_count(like_toggle)
                if count is not None:
                    return count

            # Newer path: segmentedLikeDislikeButtonViewModel
            label: str = btn.get(
                'segmentedLikeDislikeButtonViewModel', {}
            ).get(
                'likeButtonViewModel', {}
            ).get(
                'likeButtonViewModel', {}
            ).get(
                'toggleButtonViewModel', {}
            ).get(
                'toggleButtonViewModel', {}
            ).get(
                'defaultButtonViewModel', {}
            ).get(
                'buttonViewModel', {}
            ).get(
                'accessibilityText', ''
            )
            if label:
                prefix: str = 'like this video along with '
                if label.startswith(prefix):
                    return _parse_count(label[len(prefix):])

        return None

    def _parse_next_data(self, next_data: dict) -> None:
        '''
        Parse the next() API response for supplementary metadata.

        :returns: dict with like_count, comment_count, chapters, categories.
        '''

        def _find(contents: list[dict], key: str) -> dict | None:
            '''Return the value for key in the first item of contents that has it.'''
            for item in contents:
                if key in item:
                    return item[key]
            return None

        contents: dict | None = next_data.get(
            'contents', {}
        ).get(
            'twoColumnWatchNextResults', {}
        ).get(
            'results', {}
        ).get(
            'results', {}
        ).get(
            'contents'
        )
        if not contents:
            return
        primary: dict | None = _find(contents, 'videoPrimaryInfoRenderer')
        if primary:
            self.video.like_count = InnerTubeVideoParser._extract_like_count(
                primary
            )

        secondary = _find(contents, 'videoSecondaryInfoRenderer')
        if secondary:
            rows = (
                secondary
                .get('metadataRowContainer', {})
                .get('metadataRowContainerRenderer', {})
                .get('rows', [])
            )
            for row in rows:
                r = row.get('metadataRowRenderer', {})
                if 'categor' in (_text(r.get('title')) or '').lower():
                    for content in r.get('contents', []):
                        cat: str | None = _text(content)
                        if cat:
                            self.video.categories.add(cat)
        self.video.comment_count = \
            _extract_comment_count(next_data) or self.video.comment_count
        self.video.chapters = InnerTubeVideoParser._parse_chapters_from_next(
            next_data
        )

    @staticmethod
    def _parse_chapters_from_next(next_data: dict) -> list[dict]:
        '''
        Extract chapters from engagementPanels in a next() response.

        :returns: list of {title, time_text, start_seconds} dicts.
        '''
        chapters: list[dict] = []
        for panel in next_data.get('engagementPanels', []):
            renderer = panel.get('engagementPanelSectionListRenderer', {})
            panel_id = renderer.get('panelIdentifier', '')
            if 'chapters' not in panel_id and 'macro-markers' not in panel_id:
                continue
            contents = (
                renderer
                .get('content', {})
                .get('sectionListRenderer', {})
                .get('contents', [])
            )
            for section in contents:
                for item in section.get('itemSectionRenderer', {}).get('contents', []):
                    r = item.get('macroMarkersListItemRenderer', {})
                    if not r:
                        continue
                    start_secs = (
                        r.get('onTap', {})
                        .get('watchEndpoint', {})
                        .get('startTimeSeconds', 0)
                    )
                    chapters.append({
                        'title': _text(r.get('title')),
                        'time_text': _text(r.get('timeDescription')),
                        'start_seconds': float(start_secs),
                    })
        return chapters
