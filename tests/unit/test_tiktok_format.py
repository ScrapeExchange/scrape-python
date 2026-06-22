'''
Unit tests for TikTokFormat.from_api() and
formats_from_payload().
'''

import unittest

from scrape_exchange.tiktok.tiktok_format import (
    TikTokFormat,
    formats_from_payload,
)


_ENTRY: dict = {
    'GearName': 'adapt_540_1',
    'Bitrate': 789012,
    'QualityType': 28,
    'CodecType': 'bytevc1',
    'MVMAF': '{"v1.0": {"srv1": {"v540p": 79.1}}}',
    'PlayAddr': {
        'DataSize': 2761542,
        'FileCs': 'c:0-9931-2c93',
        'FileHash': 'abc0123456789abcdef0123456789abc',
        'Height': 1024,
        'Uri': 'v09044g40000sample1',
        'UrlKey': 'v09044g40000sample1_bytevc1_540p_789012',
        'UrlList': [
            'https://v16-webapp-prime.tiktok.com/video/tos/'
            'sample1/?mime_type=video_mp4&expire=1767139200'
            '&signature=0000',
            'https://v19-webapp-prime.tiktok.com/video/tos/'
            'sample1/?mime_type=video_mp4&expire=1767139200'
            '&signature=0000',
        ],
        'Width': 576,
    },
}


class TestTikTokFormat(unittest.TestCase):

    def test_from_api_full_entry(self) -> None:
        fmt: TikTokFormat | None = TikTokFormat.from_api(_ENTRY)
        self.assertIsNotNone(fmt)
        self.assertEqual(fmt.gear_name, 'adapt_540_1')
        self.assertEqual(fmt.bitrate, 789012)
        self.assertEqual(fmt.quality_type, 28)
        self.assertEqual(fmt.codec, 'bytevc1')
        self.assertEqual(fmt.width, 576)
        self.assertEqual(fmt.height, 1024)
        self.assertEqual(fmt.data_size, 2761542)
        self.assertEqual(fmt.uri, 'v09044g40000sample1')
        self.assertEqual(
            fmt.url_key,
            'v09044g40000sample1_bytevc1_540p_789012',
        )
        self.assertEqual(len(fmt.urls), 2)
        self.assertTrue(
            fmt.urls[0].startswith('https://v16-webapp-prime'),
        )
        self.assertEqual(
            fmt.file_hash,
            'abc0123456789abcdef0123456789abc',
        )

    def test_from_api_missing_gear_name_returns_none(
        self,
    ) -> None:
        entry: dict = dict(_ENTRY)
        del entry['GearName']
        self.assertIsNone(TikTokFormat.from_api(entry))

    def test_from_api_non_dict_returns_none(self) -> None:
        self.assertIsNone(TikTokFormat.from_api('bogus'))
        self.assertIsNone(TikTokFormat.from_api(None))

    def test_from_api_missing_play_addr(self) -> None:
        entry: dict = {
            'GearName': 'normal_540_0',
            'Bitrate': 1132103,
        }
        fmt: TikTokFormat | None = TikTokFormat.from_api(entry)
        self.assertIsNotNone(fmt)
        self.assertEqual(fmt.gear_name, 'normal_540_0')
        self.assertEqual(fmt.bitrate, 1132103)
        self.assertIsNone(fmt.width)
        self.assertEqual(fmt.urls, [])

    def test_from_api_filters_non_string_urls(self) -> None:
        entry: dict = {
            'GearName': 'normal_540_0',
            'PlayAddr': {
                'UrlList': ['https://ok.example/v', None, 7, ''],
            },
        }
        fmt: TikTokFormat | None = TikTokFormat.from_api(entry)
        self.assertEqual(fmt.urls, ['https://ok.example/v'])

    def test_formats_from_payload(self) -> None:
        video_block: dict = {
            'bitrateInfo': [
                _ENTRY,
                {'no_gear': True},
                'bogus',
            ],
        }
        formats: dict[str, TikTokFormat] = formats_from_payload(
            video_block,
        )
        self.assertEqual(list(formats.keys()), ['adapt_540_1'])

    def test_formats_from_payload_missing_or_malformed(
        self,
    ) -> None:
        self.assertEqual(formats_from_payload({}), {})
        self.assertEqual(
            formats_from_payload({'bitrateInfo': 'bogus'}), {},
        )


if __name__ == '__main__':
    unittest.main()
