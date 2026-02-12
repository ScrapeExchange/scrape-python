'''
Docstring for scrape_exchange.util
'''


from enum import Enum


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


class IngestStatus(Enum):
    # flake8: noqa=E221
    NONE            = None
    EXTERNAL        = 'external'
    UPLOADED        = 'uploaded'
    ENCODING        = 'encoding'
    DONE            = 'done'
    PUBLISHED       = 'published'
    STARTING        = 'starting'
    DOWNLOADING     = 'downloading'
    PACKAGING       = 'packaging'
    UPLOADING       = 'uploading'
    INGESTED        = 'ingested'
    QUEUED_START    = 'queued_start'
    UNAVAILABLE     = 'unavailable'
