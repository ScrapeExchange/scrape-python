#!/usr/bin/env python3
'''
Standalone script to scrape all video metadata from the "Live" tab of a
YouTube channel page.

Does NOT depend on any code in the scrape_exchange package.

Usage:
    python tools/yt_channel_videos.py --channel NASA
    python tools/yt_channel_videos.py --channel NASA --max-pages 2
    python tools/yt_channel_videos.py --channel NASA --output nasa_videos.json

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
import sys
import json
import argparse
import logging
from typing import Any
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

_LOGGER = logging.getLogger(__name__)

CHANNEL_VIDEOS_URL = 'https://www.youtube.com/@{channel}/streams'
BROWSE_API_URL = 'https://www.youtube.com/youtubei/v1/browse'

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/131.0.0.0 Safari/537.36'
)

INNERTUBE_CLIENT_VERSION = '2.20250219.01.00'
INNERTUBE_API_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8'


def _build_headers() -> dict[str, str]:
    return {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/json',
    }


def _fetch_url(url: str, data: bytes | None = None,
               headers: dict[str, str] | None = None) -> str:
    '''
    Fetch a URL and return the response body as a string.
    '''

    hdrs = _build_headers()
    if headers:
        hdrs.update(headers)

    req = Request(url, data=data, headers=hdrs)
    if data:
        req.add_header('Content-Type', 'application/json')

    with urlopen(req, timeout=30) as resp:
        return resp.read().decode('utf-8')


def _extract_initial_data(html: str) -> dict[str, Any]:
    '''
    Extract ytInitialData JSON from a YouTube HTML page.
    '''

    patterns = [
        r'var\s+ytInitialData\s*=\s*({.*?})\s*;',
        r'ytInitialData\s*=\s*({.*?})\s*;',
        r'window\["ytInitialData"\]\s*=\s*({.*?})\s*;',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                continue

    raise ValueError('Could not extract ytInitialData from page HTML')


def _extract_api_key(html: str) -> str:
    '''
    Try to pull the innertube API key from the page; fall back to
    a well-known default.
    '''

    match = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"', html)
    if match:
        return match.group(1)
    return INNERTUBE_API_KEY


def _extract_client_version(html: str) -> str:
    '''
    Try to pull the innertube client version from the page.
    '''

    match = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"', html)
    if match:
        return match.group(1)
    return INNERTUBE_CLIENT_VERSION


def _parse_video_renderer(renderer: dict[str, Any]) -> dict[str, Any] | None:
    '''
    Parse a richItemRenderer / gridVideoRenderer / videoRenderer into a
    flat video-metadata dict.
    '''

    video: dict[str, Any] | None = None

    # richItemRenderer wraps a videoRenderer
    if 'richItemRenderer' in renderer:
        content = renderer['richItemRenderer'].get('content', {})
        video = content.get('videoRenderer')
    elif 'gridVideoRenderer' in renderer:
        video = renderer['gridVideoRenderer']
    elif 'videoRenderer' in renderer:
        video = renderer['videoRenderer']

    if not video:
        return None

    video_id = video.get('videoId')
    if not video_id:
        return None

    title_runs = video.get('title', {}).get('runs', [])
    title = ''.join(r.get('text', '') for r in title_runs) if title_runs else \
        video.get('title', {}).get('simpleText')

    view_text = (
        video.get('viewCountText', {}).get('simpleText')
        or video.get('shortViewCountText', {}).get('simpleText')
        or ''
    )

    published_text = video.get('publishedTimeText', {}).get('simpleText')

    length_text = (
        video.get('lengthText', {}).get('simpleText')
        or video.get('thumbnailOverlays', [{}])[0]
        .get('thumbnailOverlayTimeStatusRenderer', {})
        .get('text', {}).get('simpleText')
        if video.get('thumbnailOverlays')
        else None
    )
    duration_seconds = _parse_duration(length_text) if length_text else None

    thumbnails = [
        {
            'url': t.get('url', ''),
            'width': t.get('width'),
            'height': t.get('height'),
        }
        for t in video.get('thumbnail', {}).get('thumbnails', [])
    ]

    return {
        'video_id': video_id,
        'title': title,
        'url': f'https://www.youtube.com/watch?v={video_id}',
        'view_count_text': view_text,
        'published_text': published_text,
        'duration_seconds': duration_seconds,
        'duration_text': length_text,
        'thumbnails': thumbnails,
    }


def _parse_duration(text: str) -> int | None:
    '''
    Convert a duration string like "3:42" or "1:02:15" to seconds.
    '''

    parts = text.strip().split(':')
    try:
        parts = [int(p) for p in parts]
    except ValueError:
        return None

    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 60 + parts[1]
    elif len(parts) == 1:
        return parts[0]
    return None


def _find_video_items_and_continuation(
    data: dict[str, Any]
) -> tuple[list[dict], str | None]:
    '''
    Walk the ytInitialData (or browse API response) to find the list of
    video renderers and an optional continuation token.
    '''

    items: list[dict] = []
    continuation_token: str | None = None

    # --- Initial page load path ---
    tabs = (
        data.get('contents', {})
        .get('twoColumnBrowseResultsRenderer', {})
        .get('tabs', [])
    )
    for tab in tabs:
        tab_renderer = tab.get('tabRenderer', {})
        # The "Videos" tab has endpoint with params
        if tab_renderer.get('selected'):
            section_contents = (
                tab_renderer.get('content', {})
                .get('richGridRenderer', {})
                .get('contents', [])
            )
            if not section_contents:
                # Alternate layout via sectionListRenderer
                section_list = (
                    tab_renderer.get('content', {})
                    .get('sectionListRenderer', {})
                    .get('contents', [])
                )
                for section in section_list:
                    grid = (
                        section.get('itemSectionRenderer', {})
                        .get('contents', [{}])[0]
                        .get('gridRenderer', {})
                    )
                    section_contents = grid.get('items', [])
                    cont = grid.get('continuations', [{}])[0]
                    token = cont.get(
                        'nextContinuationData', {}
                    ).get('continuation')
                    if token:
                        continuation_token = token

            for item in section_contents:
                if 'continuationItemRenderer' in item:
                    continuation_token = (
                        item['continuationItemRenderer']
                        .get('continuationEndpoint', {})
                        .get('continuationCommand', {})
                        .get('token')
                    )
                else:
                    items.append(item)
            if items:
                break

    # --- Continuation response path ---
    if not items:
        actions = data.get('onResponseReceivedActions', [])
        for action in actions:
            append_items = (
                action.get('appendContinuationItemsAction', {})
                .get('continuationItems', [])
            )
            for item in append_items:
                if 'continuationItemRenderer' in item:
                    continuation_token = (
                        item['continuationItemRenderer']
                        .get('continuationEndpoint', {})
                        .get('continuationCommand', {})
                        .get('token')
                    )
                else:
                    items.append(item)

    return items, continuation_token


def _fetch_continuation(
    continuation_token: str,
    api_key: str,
    client_version: str,
) -> dict[str, Any]:
    '''
    Call the YouTube browse API with a continuation token to get the
    next page of videos.
    '''

    payload = json.dumps({
        'context': {
            'client': {
                'clientName': 'WEB',
                'clientVersion': client_version,
                'hl': 'en',
                'gl': 'US',
            }
        },
        'continuation': continuation_token,
    }).encode('utf-8')

    url = f'{BROWSE_API_URL}?key={api_key}'
    body = _fetch_url(url, data=payload)
    return json.loads(body)


def scrape_channel_videos(
    channel: str,
    max_pages: int = 0,
) -> list[dict[str, Any]]:
    '''
    Scrape the "Live" tab for a YouTube channel and return a list of
    video-metadata dicts.

    :param channel: channel vanity name (without @)
    :param max_pages: maximum number of pages to fetch (0 = unlimited)
    :returns: list of video metadata dicts
    '''

    url = CHANNEL_VIDEOS_URL.format(channel=channel)
    _LOGGER.info('Fetching %s', url)
    html = _fetch_url(url)

    initial_data = _extract_initial_data(html)
    api_key = _extract_api_key(html)
    client_version = _extract_client_version(html)

    all_videos: list[dict[str, Any]] = []
    page = 1

    items, continuation = _find_video_items_and_continuation(initial_data)
    for item in items:
        parsed = _parse_video_renderer(item)
        if parsed:
            all_videos.append(parsed)

    _LOGGER.info(
        'Page %d: found %d videos (continuation=%s)',
        page, len(all_videos), continuation is not None
    )

    while continuation:
        if max_pages and page >= max_pages:
            _LOGGER.info('Reached max_pages=%d, stopping', max_pages)
            break

        page += 1
        resp = _fetch_continuation(continuation, api_key, client_version)
        items, continuation = _find_video_items_and_continuation(resp)

        count_before = len(all_videos)
        for item in items:
            parsed = _parse_video_renderer(item)
            if parsed:
                all_videos.append(parsed)

        _LOGGER.info(
            'Page %d: found %d new videos (total=%d, continuation=%s)',
            page, len(all_videos) - count_before,
            len(all_videos), continuation is not None
        )

    return all_videos


def process_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Scrape video list from a YouTube channel Live tab'
    )
    parser.add_argument(
        '--channel', required=True,
        help='Channel vanity name (without @), e.g. NASA'
    )
    parser.add_argument(
        '--max-pages', type=int, default=0,
        help='Max number of pages to fetch (0 = all)'
    )
    parser.add_argument(
        '--output', '-o', default=None,
        help='Output JSON file path (default: stdout)'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Enable debug logging'
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    args = process_arguments(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    videos = scrape_channel_videos(
        channel=args.channel,
        max_pages=args.max_pages,
    )

    result = {
        'channel': args.channel,
        'scraped_at': datetime.now(tz=timezone.utc).isoformat(),
        'video_count': len(videos),
        'videos': videos,
    }

    output = json.dumps(result, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f'Wrote {len(videos)} videos to {args.output}')
    else:
        print(output)


if __name__ == '__main__':
    main()
