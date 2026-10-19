'''
Parse localized YouTube subscriber counts.

YouTube writes subscriber counts in the interface language (``hl``) of
the request: ``41.8K subscribers``, ``41,8 tys. subskrybentów``,
``4.18万位订阅者``, ``64.7 लाख सदस्य``, ``elfu 41.8 wanafuatilia``. A
count is a number plus an optional multiplier word that sits before or
after it, depending on the language. The multiplier tables below were
derived from YouTube search results in all of its interface languages
and checked against the en-US counts of the same channels (see
tests/collateral/subscriber_count_samples.json).

With a multiplier the number is rounded to three significant digits
and the last ``.`` or ``,`` is the decimal point; without one the
number is exact and every separator groups thousands (``128.000``,
``41,800``, ``1 350``).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
import unicodedata

K: int = 1_000
TEN_K: int = 10_000
LAKH: int = 100_000
M: int = 1_000_000
CRORE: int = 10_000_000
HUNDRED_M: int = 100_000_000
B: int = 1_000_000_000

_LATIN: tuple[tuple[str, int], ...] = (('k', K), ('m', M), ('b', B))

# Multiplier words per interface language. Matching is
# case-insensitive and the longest word wins.
MULTIPLIERS: dict[str, tuple[tuple[str, int], ...]] = {
    'af': (('k', K), ('m', M)),
    'am': (('ሺ', K), ('ሚ', M)),
    'ar': (('ألف', K), ('آلاف', K), ('مليون', M), ('ملايين', M)),
    'as': (('হাজাৰ', K), ('লাখ', LAKH), ('নিযুত', M)),
    'az': (('k', K), ('mln', M), ('mlrd', B)),
    'be': (('тыс.', K), ('млн', M), ('млрд', B)),
    'bg': (('хил.', K), ('млн.', M), ('млрд.', B)),
    'bn': (('হা', K), ('লা', LAKH), ('কো', CRORE)),
    'bs': (('hilj.', K), ('mil.', M), ('mlr.', B)),
    'ca': (('k', K), ('m', M)),
    'cs': (('tis.', K), ('mil.', M), ('mld.', B)),
    'da': (('t.', K), ('mio.', M), ('mia.', B)),
    'de': (('tsd.', K), ('mio.', M), ('mrd.', B)),
    'el': (('χιλ.', K), ('εκ.', M), ('δισ.', B)),
    'en': _LATIN,
    'en-IN': (('k', K), ('lakh', LAKH), ('crore', CRORE), ('m', M)),
    'es': (('k', K), ('mil', K), ('m', M)),
    'et': (('tuh', K), ('mln', M), ('mld', B)),
    'eu': (('k', K), ('m', M)),
    'fa': (('هزار', K), ('میلیون', M), ('میلیارد', B)),
    'fi': (('t.', K), ('milj.', M), ('mrd.', B)),
    'fil': _LATIN,
    'fr': (('k', K), ('m', M), ('md', B)),
    'gl': (('k', K), ('m', M)),
    'gu': (('હજાર', K), ('લાખ', LAKH), ('કરોડ', CRORE)),
    'he': _LATIN,
    'hi': (
        ('हज़ार', K), ('हजार', K), ('लाख', LAKH), ('क॰', CRORE),
        ('करोड़', CRORE), ('करोड', CRORE),
    ),
    'hr': (('tis.', K), ('mil.', M), ('mlr.', B)),
    'hu': (('e', K), ('m', M), ('mrd', B)),
    'hy': (('հզր', K), ('մլն', M), ('մլրդ', B)),
    'id': (('rb', K), ('jt', M), ('m', B)),
    'is': (('þ.', K), ('m.', M), ('ma.', B)),
    'it': (('k', K), ('mln', M), ('mld', B)),
    'ja': (('千', K), ('万', TEN_K), ('億', HUNDRED_M)),
    'ka': (('ათ.', K), ('მლნ.', M), ('მლრდ.', B)),
    'kk': (('мың', K), ('м.', K), ('млн', M), ('млрд', B)),
    'km': (('ពាន់', K), ('ម៉ឺន', TEN_K), ('សែន', LAKH), ('លាន', M)),
    'kn': (('ಸಾವಿರ', K), ('ಲಕ್ಷ', LAKH), ('ಕೋಟಿ', CRORE)),
    'ko': (('천', K), ('만', TEN_K), ('억', HUNDRED_M)),
    'ky': (('миң', K), ('млн', M), ('млд', B)),
    'lo': (('ພັນ', K), ('ໝື່ນ', TEN_K), ('ແສນ', LAKH), ('ລ້ານ', M)),
    'lt': (('tūkst.', K), ('mln.', M), ('mlrd.', B)),
    'lv': (('tūkst.', K), ('milj.', M), ('mljrd.', B)),
    'mk': (('илј.', K), ('мил.', M), ('милј.', B)),
    'ml': (('ആയിരം', K), ('ലക്ഷം', LAKH), ('കോടി', CRORE)),
    'mn': (('мян', K), ('сая', M), ('тэрбум', B)),
    'mr': (('ह', K), ('लाख', LAKH), ('कोटी', CRORE)),
    'ms': (('k', K), ('j', M), ('b', B)),
    'my': (
        ('ထောင်', K), ('သောင်း', TEN_K), ('သိန်း', LAKH),
        ('သန်း', M), ('ကုဋေ', CRORE),
    ),
    'ne': (('हजार', K), ('लाख', LAKH), ('करोड', CRORE)),
    'nl': (('k', K), ('mln.', M), ('mld.', B)),
    'no': (('k', K), ('mill.', M), ('mrd.', B)),
    'or': (('ହ', K), ('ନି', M)),
    'pa': (('ਹਜ਼ਾਰ', K), ('ਲੱਖ', LAKH), ('ਕਰੋੜ', CRORE)),
    'pl': (('tys.', K), ('mln', M), ('mld', B)),
    'pt': (('mil', K), ('mi', M), ('bi', B)),
    'pt-PT': (('mil', K), ('m', M)),
    'ro': (('k', K), ('mil.', M), ('mld.', B)),
    'ru': (('тыс.', K), ('млн', M), ('млрд', B)),
    'si': (('ද', K), ('මි', M), ('බි', B)),
    'sk': (('tis.', K), ('mil.', M), ('mld.', B)),
    'sl': (('tis.', K), ('mio.', M), ('mrd.', B)),
    'sq': (('mijë', K), ('mln', M), ('mld', B)),
    'sr': (('хиљ.', K), ('мил.', M), ('млрд.', B)),
    'sr-Latn': (('hilj.', K), ('mil.', M), ('mlrd.', B)),
    'sv': (('tn', K), ('mn', M), ('md', B)),
    'sw': (('elfu', K), ('m', M), ('b', B)),
    'ta': (('ஆயிரம்', K), ('லட்சம்', LAKH), ('கோடி', CRORE)),
    'te': (
        ('వేలు', K), ('లక్షలు', LAKH), ('లక్ష', LAKH),
        ('కోట్లు', CRORE), ('కోటి', CRORE),
    ),
    'th': (('พัน', K), ('หมื่น', TEN_K), ('แสน', LAKH), ('ล้าน', M)),
    'tr': (('b', K), ('mn', M), ('mr', B)),
    'uk': (('тис.', K), ('млн', M), ('млрд', B)),
    'ur': (('ہزار', K), ('لاکھ', LAKH), ('کروڑ', CRORE)),
    'uz': (('ming', K), ('mln', M), ('mlrd', B)),
    'vi': (('n', K), ('tr', M), ('t', B)),
    'zh-CN': (('千', K), ('万', TEN_K), ('亿', HUNDRED_M)),
    'zh-HK': (('k', K), ('m', M), ('萬', TEN_K), ('億', HUNDRED_M)),
    'zh-TW': (('千', K), ('萬', TEN_K), ('億', HUNDRED_M)),
    'zu': _LATIN,
}

# Multipliers that mean the same in every language, for an unknown
# ``hl``. 'b' is left out: thousand in Turkish, billion in English.
_SAFE: tuple[tuple[str, int], ...] = (
    ('k', K), ('m', M), ('lakh', LAKH), ('crore', CRORE),
    ('万', TEN_K), ('萬', TEN_K), ('亿', HUNDRED_M), ('億', HUNDRED_M),
)

_INVISIBLE: dict[int, None] = dict.fromkeys(
    map(ord, '​‌‍‎‏‪‫‬‭'
             '‮⁦⁧⁨⁩﻿'),
    None,
)
_NUMBER: re.Pattern[str] = re.compile(
    r'\d+(?:[.,]\d+| \d{3}(?!\d))*',
)
# Scripts that separate words with spaces: a multiplier written in one
# of them must stand alone ('N' in Vietnamese is not the 'n' of
# 'người').
_SPACED_SCRIPTS: tuple[str, ...] = (
    'LATIN', 'CYRILLIC', 'GREEK', 'ARMENIAN', 'GEORGIAN',
)


def _normalize(text: str) -> str:
    '''Drop bidi and zero-width marks, turn every space into ' ' and
    every decimal digit into ASCII.'''
    out: list[str] = []
    char: str
    for char in text.translate(_INVISIBLE):
        if char.isspace():
            out.append(' ')
        elif char.isdigit() and not char.isascii():
            digit: int = unicodedata.digit(char, -1)
            out.append(str(digit) if digit >= 0 else char)
        elif char == '٫':
            out.append('.')
        elif char == '٬':
            out.append(',')
        else:
            out.append(char)
    return ''.join(out)


def _table(hl: str | None) -> tuple[tuple[str, int], ...]:
    if hl:
        if hl in MULTIPLIERS:
            return MULTIPLIERS[hl]
        base: str = hl.split('-')[0].split('_')[0].lower()
        if base in MULTIPLIERS:
            return MULTIPLIERS[base]
    return _SAFE


def _needs_boundary(word: str) -> bool:
    return any(
        name.startswith(_SPACED_SCRIPTS)
        for name in (unicodedata.name(c, '') for c in word if c.isalpha())
    )


def _matches_after(text: str, word: str) -> bool:
    if not text.lower().startswith(word.lower()):
        return False
    rest: str = text[len(word):]
    return not (
        _needs_boundary(word) and rest and rest[0].isalnum()
    )


def _matches_before(text: str, word: str) -> bool:
    if not text.lower().endswith(word.lower()):
        return False
    head: str = text[:len(text) - len(word)]
    return not (
        _needs_boundary(word) and head and head[-1].isalnum()
    )


def _multiplier(
    before: str, after: str, table: tuple[tuple[str, int], ...],
) -> int | None:
    after = after.lstrip()
    before = before.rstrip()
    best: tuple[int, int] | None = None
    word: str
    value: int
    for word, value in table:
        if _matches_after(after, word) or _matches_before(before, word):
            if best is None or len(word) > best[0]:
                best = (len(word), value)
    return best[1] if best else None


def parse_subscriber_count(text: str | None, hl: str | None) -> int | None:
    '''The subscriber count in a YouTube label written for interface
    language *hl*, or None when *text* holds no count (for example a
    channel handle such as ``@nomorevideos3``).'''
    if not text:
        return None
    clean: str = _normalize(text).strip()
    if not clean or clean.startswith('@'):
        return None
    match: re.Match[str] | None = _NUMBER.search(clean)
    if match is None:
        return None
    number: str = match.group(0)
    before: str = clean[:match.start()]
    after: str = clean[match.end():]
    table: tuple[tuple[str, int], ...] = _table(hl)
    multiplier: int | None = _multiplier(before, after, table)
    if multiplier is None and table is not _SAFE:
        # Some interfaces show Latin K/M instead of their own words;
        # the language's own words take precedence (Indonesian 'M' is
        # a billion).
        multiplier = _multiplier(before, after, _SAFE)
    digits: str
    if multiplier is None:
        # An exact count only groups thousands, three digits at a
        # time; '41.8' without a known multiplier is a rounded count
        # whose unit we cannot tell.
        groups: list[str] = re.split(r'[., ]', number)[1:]
        if any(len(group) != 3 for group in groups):
            return None
        digits = re.sub(r'[., ]', '', number)
        return int(digits)
    number = number.replace(' ', '')
    last: int = max(number.rfind('.'), number.rfind(','))
    if last >= 0:
        digits = (
            re.sub(r'[.,]', '', number[:last]) + '.' + number[last + 1:]
        )
    else:
        digits = number
    return int(round(float(digits) * multiplier))
