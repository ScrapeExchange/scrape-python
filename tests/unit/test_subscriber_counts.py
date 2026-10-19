'''
Unit tests for scrape_exchange.youtube.subscriber_counts.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import unittest
from pathlib import Path
from typing import Any

from scrape_exchange.youtube.subscriber_counts import (
    parse_subscriber_count,
)

SAMPLES: Path = Path(__file__).parent.parent / 'collateral' / (
    'subscriber_count_samples.json'
)


class TestRealSamples(unittest.TestCase):
    '''Every collected YouTube text parses to the channel's en-US
    count, for all YouTube interface languages.'''

    def test_all_languages(self) -> None:
        data: dict[str, Any] = json.loads(SAMPLES.read_text())
        hl: str
        cases: Any
        checked: int = 0
        for hl, cases in data.items():
            if hl.startswith('_'):
                continue
            text: str
            expected: int
            for text, expected in cases:
                with self.subTest(hl=hl, text=text):
                    got: int | None = parse_subscriber_count(text, hl)
                    self.assertIsNotNone(got)
                    assert got is not None
                    # Both sides are rounded to three significant
                    # digits, in different units (K vs 万 vs lakh).
                    self.assertLessEqual(
                        abs(got - expected), max(1, expected * 0.01),
                    )
                checked += 1
        self.assertGreater(checked, 3000)


class TestEdgeCases(unittest.TestCase):

    def test_multipliers_without_ground_truth(self) -> None:
        cases: list[tuple[str, str, int]] = [
            ('ผู้ติดตาม 5 พัน คน', 'th', 5000),
            ('구독자 4.36천명', 'ko', 4360),
            ('စာရင်းသွင်းသူ 8.03\xa0ထောင်', 'my', 8030),
            ('3.64\xa0K suscriptores', 'es-419', 3640),
        ]
        text: str
        hl: str
        expected: int
        for text, hl, expected in cases:
            with self.subTest(hl=hl):
                self.assertEqual(parse_subscriber_count(text, hl), expected)

    def test_hundred_million_units(self) -> None:
        self.assertEqual(
            parse_subscriber_count('チャンネル登録者数 4.04億人', 'ja'),
            404_000_000,
        )
        self.assertEqual(
            parse_subscriber_count('4.04亿位订阅者', 'zh-CN'), 404_000_000,
        )
        self.assertEqual(
            parse_subscriber_count('4.04億位訂閱者', 'zh-TW'), 404_000_000,
        )
        self.assertEqual(
            parse_subscriber_count('구독자 4.04억명', 'ko'), 404_000_000,
        )

    def test_grouping_without_multiplier(self) -> None:
        self.assertEqual(
            parse_subscriber_count('128.000 Abonnenten', 'de'), 128_000,
        )
        self.assertEqual(
            parse_subscriber_count('1\xa0350 prenumeranter', 'sv'), 1350,
        )
        self.assertEqual(
            parse_subscriber_count('41,800 వరిక్కాർ', 'te'), 41_800,
        )

    def test_decimal_comma_with_multiplier(self) -> None:
        self.assertEqual(
            parse_subscriber_count('41,8\xa0k\xa0abonnés', 'fr'), 41_800,
        )
        self.assertEqual(
            parse_subscriber_count('1,7\xa0млн подписчиков', 'ru'),
            1_700_000,
        )

    def test_turkish_b_is_thousand_english_b_is_billion(self) -> None:
        self.assertEqual(
            parse_subscriber_count('41,8\xa0B abone', 'tr'), 41_800,
        )
        self.assertEqual(
            parse_subscriber_count('1.2B subscribers', 'en-US'),
            1_200_000_000,
        )

    def test_native_digits(self) -> None:
        self.assertEqual(
            parse_subscriber_count('٤١٫٨ ألف مشترك', 'ar'), 41_800,
        )
        self.assertEqual(
            parse_subscriber_count('६४.७ लाख सदस्य', 'hi'), 6_470_000,
        )

    def test_handle_and_empty_are_unknown(self) -> None:
        self.assertIsNone(parse_subscriber_count('@nomorevideos3', 'en'))
        self.assertIsNone(parse_subscriber_count('', 'en-US'))
        self.assertIsNone(parse_subscriber_count(None, 'en-US'))
        self.assertIsNone(parse_subscriber_count('subscribers', 'en-US'))

    def test_unknown_language_uses_safe_multipliers_only(self) -> None:
        self.assertEqual(
            parse_subscriber_count('2.15M subscribers', 'xx'), 2_150_000,
        )
        # 'B' means thousand in Turkish, billion in English: unknown.
        self.assertIsNone(parse_subscriber_count('41.8 B abone', 'xx'))

    def test_latin_k_m_understood_in_any_language(self) -> None:
        # Some interfaces show Latin K/M instead of their own words.
        self.assertEqual(
            parse_subscriber_count('12M subscribers', 'hi'), 12_000_000,
        )
        self.assertEqual(
            parse_subscriber_count('5K подписчиков', 'ru'), 5000,
        )

    def test_language_word_wins_over_fallback(self) -> None:
        # Indonesian 'M' is miliar (billion), not million.
        self.assertEqual(
            parse_subscriber_count('1,2\xa0M subscriber', 'id'),
            1_200_000_000,
        )

    def test_language_variants_fall_back_to_base(self) -> None:
        self.assertEqual(
            parse_subscriber_count('41,8\xa0tys. subskrybentów', 'pl-PL'),
            41_800,
        )


if __name__ == '__main__':
    unittest.main()
