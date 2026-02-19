'''
Docstring for scrape_exchange.util
'''

from os import path
from enum import Enum
from datetime import datetime
import os

from .datatypes import IngestStatus


def convert_number_string(number_text: str | int) -> int:
    '''
    Converts a number with optional appendix of m, k, to an integer

    :param number_text: The number as a string, e.g. '1.2M', '3K', '500'
    :returns: The number as an integer, e.g. 1200000, 3000, 500
    :raises ValueError: If the input string is not in a valid format
    '''

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


def get_imported_assets(save_dir: str | None = None,
                        uploaded_dir: str | None = None
                        ) -> dict[str, tuple[Enum, datetime]]:
    '''
    Reads the save directory for already ingested videos and returns a mapping
    of video IDs to their ingest status and published timestamp.

    :param save_dir: The directory where scraped video data is saved
    :param uploaded_dir: Optional directory where uploaded video data is saved.
    If not provided, uploaded videos will be read from <save_dir>/uploaded
    :returns: A dictionary mapping YouTube video IDs to a tuple of ingest
              status and published timestamp
    :raises: RuntimeError if the save directory cannot be read
    '''

    imported_assets: dict[str, tuple[IngestStatus, datetime]] = {}

    def _get_assets(directory: str, ingest_status: IngestStatus) -> None:
        try:
            asset_id: str
            entry: os.DirEntry
            for entry in os.scandir(directory):
                if entry.is_file() and entry.name.endswith('.json'):
                    asset_id, _ = path.splitext(entry.name)[0]
                    timestamp: datetime = entry.stat().st_mtime
                    imported_assets[asset_id] = (
                        ingest_status, datetime.fromtimestamp(timestamp)
                    )
        except Exception as exc:
            raise RuntimeError(
                f'Failed to read imported assets from {directory}: {exc}'
            ) from exc

    if save_dir:
        _get_assets(save_dir, IngestStatus.SCRAPED)

        if not uploaded_dir:
            uploaded_dir = f'{save_dir}/uploaded'

        _get_assets(uploaded_dir, IngestStatus.UPLOADED)

    return imported_assets


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
        return []

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
