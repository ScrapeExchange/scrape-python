'''
Schema for one entry in ``channels.lst``.

Canonical line format is a single JSON object on one line with the
keys (in this exact order)::

    channel_id, channel_handle, title, status[, comment]

Missing values are explicit ``null``. The tool accepts raw lines
(handle, channel id, URL, title) on input — those are canonicalised
on the next write. See ``CONTEXT.md`` for the broader entity model.
'''

import json
import re
from dataclasses import dataclass
from typing import Literal

from scrape_exchange.youtube.youtube_channel import YouTubeChannel

# YouTube channel ids are 24-char base64-ish strings starting "UC".
_URL_HANDLE_RE: re.Pattern[str] = re.compile(
    r'^https?://(?:www\.)?youtube\.com/@(?P<handle>[^/?#]+)/?$',
)
_URL_CHANNEL_RE: re.Pattern[str] = re.compile(
    r'^https?://(?:www\.)?youtube\.com/channel/'
    r'(?P<channel_id>UC[\w-]{22})/?$',
)


Status = Literal['scraped', 'new']


@dataclass(slots=True)
class ChannelListRecord:
    channel_id: str | None
    channel_handle: str | None
    title: str | None
    status: Status = 'new'
    comment: str | None = None


def _looks_like_handle(value: str) -> bool:
    '''Hard handle rules: no whitespace, no slash, non-empty.'''
    return bool(value) and ' ' not in value and '/' not in value


def parse_line(line: str) -> ChannelListRecord | None:
    '''Return a record, or ``None`` for blank/comment lines.'''
    stripped: str = line.strip()
    if not stripped:
        return None
    if stripped.startswith('{'):
        return _parse_jsonl(stripped)
    return _parse_raw(stripped)


def _parse_jsonl(line: str) -> ChannelListRecord:
    data: dict = json.loads(line)
    return ChannelListRecord(
        channel_id=data.get('channel_id'),
        channel_handle=data.get('channel_handle'),
        title=data.get('title'),
        status=data.get('status', 'new'),
        comment=data.get('comment'),
    )


def _parse_raw(value: str) -> ChannelListRecord:
    '''Classify a non-JSON entry: id, handle, URL, or title.'''
    m_id: re.Match[str] | None = YouTubeChannel.CHANNEL_ID_REGEX_MATCH.match(
        value
    )
    if m_id:
        return ChannelListRecord(
            channel_id=value,
            channel_handle=None,
            title=None,
        )
    m_url_handle: re.Match[str] | None = _URL_HANDLE_RE.match(
        value,
    )
    if m_url_handle:
        return ChannelListRecord(
            channel_id=None,
            channel_handle=m_url_handle.group('handle'),
            title=None,
        )
    m_url_channel: re.Match[str] | None = _URL_CHANNEL_RE.match(
        value,
    )
    if m_url_channel:
        return ChannelListRecord(
            channel_id=m_url_channel.group('channel_id'),
            channel_handle=None,
            title=None,
        )
    bare: str = value.lstrip('@')
    if _looks_like_handle(bare):
        return ChannelListRecord(
            channel_id=None,
            channel_handle=bare,
            title=None,
        )
    return ChannelListRecord(
        channel_id=None,
        channel_handle=None,
        title=value,
    )


def format_line(record: ChannelListRecord) -> str:
    '''Serialise to canonical JSONL line (no trailing newline).'''
    payload: dict = {
        'channel_id': record.channel_id,
        'channel_handle': record.channel_handle,
        'title': record.title,
        'status': record.status,
    }
    if record.comment is not None:
        payload['comment'] = record.comment
    return json.dumps(
        payload, ensure_ascii=False, separators=(',', ':')
    )
