'''
Docstring for scrape_exchange.util
'''

from datetime import datetime

from .datatypes import IngestStatus
from .file_management import AssetFileManagement


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
