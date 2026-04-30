'''
Shared helpers for parsing and de-duplicating entries in the
``channels.lst``-style file that the YouTube channel scraper reads
at startup and the operator-run cleanup tool canonicalises.

Two callers, same rules:

* :mod:`tools.yt_channel_scrape` reads the file and turns each
  surviving line into a candidate handle to scrape.
* :mod:`tools.cleanup_channel_list` rewrites the file in place,
  resolving display names via the NameMap, dropping duplicates,
  and replacing channel IDs with handles where the CreatorMap
  knows the mapping.

Keeping the URL-form recognition, JSONL extraction, and
case-insensitive dedup in one module avoids the previous bug
shape where cleanup recognised a URL form the scraper's reader
silently mishandled.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re

import orjson


# A YouTube channel ID is "UC" + 22 base64url chars.
CHANNEL_PREFIX: str = 'channel/'
_UC_ID_RE: re.Pattern = re.compile(
    r'^UC[A-Za-z0-9_-]{22}$',
)

# YouTube channel URL forms that show up in copy-paste channel
# lists. The path forms that point at a canonical channel are
# matched; video / playlist / search URLs are left alone.
_YT_CHANNEL_URL_RE: re.Pattern = re.compile(
    r'^https?://'
    r'(?:www\.|m\.|music\.)?'
    r'youtube\.com/'
    r'(?:@(?P<at>[A-Za-z0-9._-]+)'
    r'|c/(?P<custom>[A-Za-z0-9._-]+)'
    r'|user/(?P<user>[A-Za-z0-9._-]+)'
    r'|channel/(?P<chan>UC[A-Za-z0-9_-]{22}))'
    r'(?:[/?#].*)?$',
    re.IGNORECASE,
)


def is_channel_id(name: str) -> bool:
    '''
    Return True when *name* is a bare YouTube channel ID
    (``UC`` + 22 chars). Mirrors
    :meth:`YouTubeChannel.is_channel_id` but lives here so this
    module has no dependency on the platform model — it can run
    in tooling that doesn't import YouTube models.
    '''
    if not name:
        return False
    return bool(_UC_ID_RE.match(name))


def is_uc_id_path(name: str) -> bool:
    '''
    Return True when *name* is a ``channel/UC...`` URL path —
    a YouTube channel page identified only by its bare channel
    id with no custom handle. These pages cannot be followed by
    the channel / RSS scrapers (they need handles), so the
    discover BFS treats them as feed-style seed pages: fetched
    once for their ``page_links`` harvest, never recorded in
    the discovered set.
    '''
    if not name.startswith(CHANNEL_PREFIX):
        return False
    return bool(
        _UC_ID_RE.match(name[len(CHANNEL_PREFIX):]),
    )


def uc_id_path_to_seed_url(name: str) -> str:
    '''
    Convert a ``channel/UC...`` link path into the full
    ``https://www.youtube.com/channel/UC...`` URL form used by
    feed-style seeds. Caller must already have established that
    *name* matches :func:`is_uc_id_path`.
    '''
    return f'https://www.youtube.com/{name}'


def extract_url_canonical(entry: str) -> str | None:
    '''
    When *entry* is a YouTube channel URL, return the bare
    canonical form — the channel_id for ``/channel/UC...`` URLs,
    or the handle (without leading ``@``) for ``/@h``, ``/c/h``,
    and ``/user/h`` URLs (with optional ``www.``, ``m.``, or
    ``music.`` subdomain, ``http`` or ``https``, and trailing
    path / query / fragment). Returns ``None`` for anything
    that isn't a recognised channel URL.
    '''
    match: re.Match[str] | None = _YT_CHANNEL_URL_RE.match(entry)
    if match is None:
        return None
    chan: str | None = match.group('chan')
    if chan:
        return chan
    return (
        match.group('at')
        or match.group('custom')
        or match.group('user')
    )


def is_json_entry(line: str) -> bool:
    '''
    Return True if *line* parses as a JSON object or list.
    Cheap brace-pair check first so the orjson decode is only
    paid when the line plausibly looks like JSON.
    '''
    if not (
        (line.startswith('{') and line.endswith('}'))
        or (line.startswith('[') and line.endswith(']'))
    ):
        return False
    try:
        orjson.loads(line)
    except orjson.JSONDecodeError:
        return False
    return True


def jsonl_channel_field(entry: str) -> str | None:
    '''
    Return the value of the ``channel`` field when *entry* is a
    JSON object containing a non-empty string ``channel`` value,
    or ``None`` otherwise. Used so a JSONL line and a plain
    handle line that point at the same channel collapse to one
    entry during dedup.
    '''
    if not (entry.startswith('{') and entry.endswith('}')):
        return None
    try:
        data: object = orjson.loads(entry)
    except orjson.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    channel: object = data.get('channel')
    if isinstance(channel, str) and channel:
        return channel
    return None


def entry_handle_part(entry: str) -> str | None:
    '''
    When *entry* is not a JSON line but contains a comma,
    return the part before the first ``,`` (whitespace-stripped).
    Used so dedup keys ``<handle>,<extra>`` lines on the leading
    handle. Returns ``None`` when *entry* is JSON, has no comma,
    or has an empty leading part.
    '''
    if is_json_entry(entry):
        return None
    if ',' not in entry:
        return None
    head: str = entry.split(',', 1)[0].strip()
    return head or None


def entry_dedupe_key(entry: str) -> str:
    '''
    Comparison key used by :func:`dedupe_preserving_case`. A
    JSONL entry with a ``channel`` field keys on that value
    (lower-cased); a YouTube channel URL keys on the handle or
    channel_id extracted from the path (lower-cased); a plain
    line containing a comma keys on the part before the first
    comma (lower-cased); every other entry keys on its own
    lower-cased text.
    '''
    channel: str | None = jsonl_channel_field(entry)
    if channel is not None:
        return channel.lower()
    url_canonical: str | None = extract_url_canonical(entry)
    if url_canonical is not None:
        return url_canonical.lower()
    head: str | None = entry_handle_part(entry)
    if head is not None:
        return head.lower()
    return entry.lower()


def entry_dedupe_rank(entry: str) -> int:
    '''
    Preference rank for :func:`dedupe_preserving_case`. Higher
    wins. ``2`` for a JSONL line with a ``channel`` field. For
    plain lines (including ``<handle>,<extra>`` lines) the rank
    is ``1`` when the handle part contains an upper-case
    character and ``0`` when fully lower-case. URLs rank ``-1``
    so any non-URL form for the same channel wins.
    '''
    if jsonl_channel_field(entry) is not None:
        return 2
    if extract_url_canonical(entry) is not None:
        return -1
    head: str | None = entry_handle_part(entry)
    target: str = head if head is not None else entry
    if target != target.lower():
        return 1
    return 0


def dedupe_preserving_case(entries: list[str]) -> list[str]:
    '''
    Group *entries* by :func:`entry_dedupe_key` and keep the
    highest-ranked variant per group (see
    :func:`entry_dedupe_rank`). Ties are broken by first
    appearance, and the relative order of groups follows the
    first appearance of each key in the input.

    The result list has at most one entry per dedup key; the
    surviving entry preserves its original casing and form
    (URL, JSONL, comma-suffixed, etc.).
    '''
    chosen: dict[str, str] = {}
    order: list[str] = []

    for entry in entries:
        key: str = entry_dedupe_key(entry)
        if key not in chosen:
            chosen[key] = entry
            order.append(key)
            continue
        existing: str = chosen[key]
        if entry_dedupe_rank(entry) > entry_dedupe_rank(existing):
            chosen[key] = entry

    return [chosen[k] for k in order]


def _normalise_jsonl(line: str) -> str | None:
    '''
    If *line* is a JSON object with a ``channel`` field, return
    the field's value (whitespace-stripped). Otherwise return
    *line* unchanged. Returns ``None`` when the line should be
    skipped entirely (e.g. JSONL with empty ``channel`` value).
    '''
    channel: str | None = jsonl_channel_field(line)
    if channel is None:
        return line
    stripped: str = channel.strip()
    return stripped or None


def _split_canonical(
    canonical: str,
) -> tuple[str | None, str | None]:
    '''Map a URL-extracted canonical to ``(handle, channel_id)``.'''
    if is_channel_id(canonical):
        return None, canonical
    return canonical, None


def _try_uc_id(line: str) -> tuple[str | None, str | None] | None:
    '''
    Recognise a bare YouTube channel id, accepting either the
    ``UC`` or the lower-case ``uc`` prefix. Returns the
    ``(None, channel_id)`` tuple when matched, ``None`` otherwise.
    '''
    if not line.lower().startswith('uc'):
        return None
    candidate: str = line[:2].upper() + line[2:]
    if is_channel_id(candidate):
        return None, candidate
    return None


def _try_handle_token(
    candidate: str,
) -> tuple[str | None, str | None] | None:
    '''
    Final-stage handle extractor: strip a leading ``@`` and
    reject empty / display-name-shaped tokens. Returns
    ``(handle, None)`` on success, ``None`` to indicate the
    caller should fall through (and ultimately skip).
    '''
    handle: str = candidate.lstrip('@').strip()
    if not handle:
        return None
    if looks_like_display_name(handle):
        return None
    return handle, None


def _parse_url_form(
    line: str,
) -> tuple[str | None, str | None] | None:
    '''
    Recognise URL-shaped lines:

    * Recognised YouTube channel URL → ``(handle, id)`` tuple
    * Unrecognised ``http(s)://…`` → ``(None, None)`` (skip)
    * Anything else → ``None`` (caller continues)
    '''
    canonical: str | None = extract_url_canonical(line)
    if canonical is not None:
        return _split_canonical(canonical)
    if line.startswith(('http://', 'https://')):
        return None, None
    return None


def _strip_channel_prefix(
    line: str,
) -> tuple[
    tuple[str | None, str | None] | None,
    str,
]:
    '''
    Recognise the ``channel/...`` path form. Returns
    ``((None, channel_id), line)`` when *line* is a valid
    ``channel/UC…`` and the caller should return the tuple, or
    ``(None, line_without_prefix)`` when *line* had a generic
    ``channel/`` prefix that should be stripped before
    continuing. Otherwise ``(None, line)`` unchanged.
    '''
    if is_uc_id_path(line):
        return (None, line[len(CHANNEL_PREFIX):]), line
    if line.startswith(CHANNEL_PREFIX):
        return None, line[len(CHANNEL_PREFIX):]
    return None, line


def _parse_handle_or_id(
    line: str,
) -> tuple[str | None, str | None]:
    '''
    Tail-stage parser: line is no longer URL-shaped and the
    ``channel/`` prefix has been stripped. Try the bare-id
    form, then the comma form, then the tab form, then the
    bare-handle fallback. Anything that doesn't match is
    skipped.
    '''
    uc_match: tuple[str | None, str | None] | None = _try_uc_id(
        line,
    )
    if uc_match is not None:
        return uc_match

    head: str | None = entry_handle_part(line)
    if head is not None:
        return _try_handle_token(head) or (None, None)

    if '\t' in line:
        parts: list[str] = line.split('\t')
        if len(parts) >= 2:
            tab_match = _try_handle_token(parts[1])
            if tab_match is not None:
                return tab_match
        return None, None

    return _try_handle_token(line) or (None, None)


def parse_channel_handle(
    raw_line: str,
) -> tuple[str | None, str | None]:
    '''
    Parse one raw line from the channels.lst-style file. Returns
    a ``(handle, unresolved_channel_id)`` tuple where:

    * ``handle`` is a bare channel handle string (no leading
      ``@``) when the line resolved to a known handle form.
    * ``unresolved_channel_id`` is a bare ``UC...`` channel ID
      when the line resolved to a channel ID with no associated
      handle. The caller is expected to look it up in its
      ``CreatorMap`` and either replace it with a handle or
      enqueue it for resolution.

    Both fields are ``None`` for lines that should be skipped:
    blanks, comments (``#``), display names (lines containing
    whitespace), and unrecognised URLs.

    Recognised forms (matched in order):

    * ``#…`` comment, blank, or whitespace-containing line → skip
    * JSON object with a ``channel`` field → use that value
    * YouTube channel URL (``@h``, ``c/h``, ``user/h``,
      ``channel/UC…``, with optional subdomain and trailing
      path / query) → handle or channel_id from the path
    * Unrecognised ``http(s)://…`` → skip
    * ``channel/UC…`` path form → channel_id
    * Bare ``UC…`` (24 chars) → channel_id
    * ``<handle>,<extra>`` → leading part as handle (matches the
      cleanup tool's dedup-key direction)
    * ``<id>\\t<handle>`` → second column as handle
    * Bare handle (with or without leading ``@``) → handle
    '''
    line: str = raw_line.strip()
    if not line or line.startswith('#'):
        return None, None

    unwrapped: str | None = _normalise_jsonl(line)
    if unwrapped is None:
        return None, None
    line = unwrapped

    url_match: tuple[str | None, str | None] | None = (
        _parse_url_form(line)
    )
    if url_match is not None:
        return url_match

    prefix_match, line = _strip_channel_prefix(line)
    if prefix_match is not None:
        return prefix_match

    return _parse_handle_or_id(line)


def looks_like_display_name(entry: str) -> bool:
    '''
    Return True when *entry* should be looked up in the NameMap
    rather than treated as a handle or channel id. Whitespace
    and runs of four or more dots both indicate a free-text
    display name rather than a YouTube handle. Used by
    :mod:`tools.cleanup_channel_list` to decide whether to feed
    a line through the NameMap resolver, and internally by
    :func:`parse_channel_handle` to skip display-name lines
    when reading the same file.
    '''
    return any(c.isspace() for c in entry) or '....' in entry
