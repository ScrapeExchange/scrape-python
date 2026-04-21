'''
Docstring for scrape_exchange.util
'''

import re
from datetime import datetime

from .datatypes import IngestStatus
from .file_management import AssetFileManagement

_PROXY_HOST_RE: re.Pattern[str] = re.compile(r'^[A-Za-z0-9._-]+$')


def convert_number_string(number_text: str | int) -> int | None:
    '''
    Converts a number with optional appendix of m, k, to an integer

    :param number_text: The number as a string, e.g. '1.2M', '3K', '500'
    :returns: The number as an integer, e.g. 1200000, 3000, 500
    :raises ValueError: If the input string is not in a valid format
    '''

    if isinstance(number_text, bool):
        return None

    if not number_text or isinstance(number_text, int):
        return number_text

    words: list[str] = number_text.split(' ')
    number_text = words[0].strip()

    multiplier: str = number_text[-1].upper()
    if not multiplier.isnumeric():
        multipliers: dict[str, int] = {
            'K': 1000,
            'M': 1000000,
            'B': 1000000000,
        }
        count_pre: float = float(number_text[:-1])
        count = int(
            count_pre * multipliers[multiplier]
        )
    else:
        number_text = number_text.replace(',', '')
        count = int(number_text)

    return count


def get_imported_assets(
    save_dir: str | None = None,
) -> dict[str, tuple[IngestStatus, datetime]]:
    '''
    Read *save_dir* (and its ``uploaded/`` subdirectory) for already-ingested
    YouTube videos and return a mapping of bare video IDs to their ingest
    status and modification timestamp.

    Asset discovery is delegated to
    :class:`scrape_exchange.file_management.AssetFileManagement`, which
    enumerates ``video-*-{id}.json.br`` files across both the base and
    uploaded directories.  When the same video ID appears in multiple
    locations the ``UPLOADED`` status takes precedence over ``SCRAPED``;
    within the same status, the most recent modification time wins.

    :param save_dir: The directory where scraped video data is saved.
    :returns: Dict mapping bare video IDs (no prefix, no suffix) to a
        ``(IngestStatus, datetime)`` tuple.
    :raises RuntimeError: If *save_dir* cannot be read.
    '''
    if not save_dir:
        return {}

    try:
        afm = AssetFileManagement(save_dir)
    except OSError as exc:
        raise RuntimeError(
            f'Failed to access asset directory {save_dir}: {exc}'
        ) from exc

    imported_assets: dict[str, tuple[IngestStatus, datetime]] = {}
    try:
        for video_id, is_uploaded, mtime in afm.iter_assets('video'):
            new_status: IngestStatus = (
                IngestStatus.UPLOADED if is_uploaded else IngestStatus.SCRAPED
            )
            new_ts: datetime = datetime.fromtimestamp(mtime)
            existing = imported_assets.get(video_id)
            if (existing is None
                    or _should_replace(existing, new_status, new_ts)):
                imported_assets[video_id] = (new_status, new_ts)
    except OSError as exc:
        raise RuntimeError(
            f'Failed to read imported assets from {save_dir}: {exc}'
        ) from exc

    return imported_assets


def _should_replace(
    existing: tuple[IngestStatus, datetime],
    new_status: IngestStatus,
    new_ts: datetime,
) -> bool:
    '''
    Decide whether *new_status*/*new_ts* should overwrite *existing* in the
    imported-assets map: ``UPLOADED`` always wins over ``SCRAPED``; within the
    same status the newer timestamp wins.
    '''
    old_status, old_ts = existing
    if (new_status == IngestStatus.UPLOADED
            and old_status != IngestStatus.UPLOADED):
        return True
    if new_status == old_status and new_ts > old_ts:
        return True
    return False


def split_quoted_string(text: str, delimiters: str = ', ') -> set[str]:
    '''
    Split a string on delimiters (commas and spaces by default) while
    preserving quoted substrings.

    Quoted substrings (single or double quotes) are kept intact and
    returned without their surrounding quotes. Multiple consecutive
    delimiters are treated as a single delimiter.

    Args:
        text: The string to split
        delimiters: Characters to use as delimiters (default: ', ')

    Returns:
        List of split strings with quotes removed from quoted substrings

    Examples:
        >>> split_quoted_string('foo, bar, "hello world", baz')
        ['foo', 'bar', 'hello world', 'baz']

        >>> split_quoted_string('"test one" test2 "test three"')
        ['test one', 'test2', 'test three']

        >>> split_quoted_string("'single' and 'double quotes' work")
        ['single', 'and', 'double quotes', 'work']
    '''

    if not text:
        return set()

    result: set[str] = set()
    current_token: list[str] = []
    in_quotes: bool = False
    quote_char: str | None = None

    for i, char in enumerate(text):
        # Check if we're entering or exiting quotes
        if char in ('"', "'") and (i == 0 or text[i-1] != '\\'):
            if not in_quotes:
                # Starting a quoted section
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                # Ending the quoted section
                in_quotes = False
                quote_char = None
            else:
                # Different quote type inside quotes, treat as regular char
                current_token.append(char)

        # If we're in quotes, add everything to current token
        elif in_quotes:
            current_token.append(char)

        # If we hit a delimiter outside quotes
        elif char in delimiters:
            # Save current token if it has content
            if current_token:
                result.add(''.join(current_token))
                current_token = []

        # Regular character outside quotes
        else:
            current_token.append(char)

    # Don't forget the last token
    if current_token:
        result.add(''.join(current_token))

    return result


def extract_proxy_ip(proxy: str) -> str:
    '''
    Extracts the IP address (or hostname) from a proxy URL for use as a
    Prometheus label value or a log field. Strips the scheme and any
    user:password@ prefix, then drops the port. Keeping only the host
    portion avoids leaking proxy credentials into metrics and logs and
    keeps Prometheus label cardinality bounded by the proxy pool size.

    Examples::

        extract_proxy_ip('http://127.0.0.1:8080')            -> '127.0.0.1'
        extract_proxy_ip('http://user:pass@127.0.0.1:8080')  -> '127.0.0.1'
        extract_proxy_ip('http://user:pass@127.0.0.1')       -> '127.0.0.1'
        extract_proxy_ip('socks5://proxy.example:1080')      -> 'proxy.example'

    :param proxy: The proxy URL.
    :returns: Host portion of the URL.
    :raises ValueError: On an IPv6 address (not yet supported), an
        unparseable URL, or a host portion that doesn't look like a
        valid hostname or IPv4 address.
    '''

    if proxy.count(':') > 3 or '[' in proxy:
        raise ValueError(f'IPv6 addresses are not supported: {proxy}')

    remainder: str = proxy
    # Strip scheme.
    if '://' in remainder:
        remainder = remainder.split('://', 1)[1]
    # Strip user:pass@ prefix before splitting off the port, so
    # the ':' in 'user:pass' doesn't steal the first segment.
    if '@' in remainder:
        remainder = remainder.split('@', 1)[1]
    # Drop :port if present.
    host: str = remainder.split(':', 1)[0]
    if not host or not _PROXY_HOST_RE.match(host):
        raise ValueError(f'Invalid proxy URL: {proxy!r}')
    return host
