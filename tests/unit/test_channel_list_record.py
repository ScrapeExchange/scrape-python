import unittest

from tools._channel_list_record import (
    ChannelListRecord,
    parse_line,
    format_line,
)


class TestParseLine(unittest.TestCase):
    def test_parses_full_jsonl(self) -> None:
        rec = parse_line(
            '{"channel_id":"UC1","channel_handle":"Foo",'
            '"title":"Foo Title","status":"scraped"}'
        )
        self.assertIsInstance(rec, ChannelListRecord)
        self.assertEqual(rec.channel_id, 'UC1')
        self.assertEqual(rec.channel_handle, 'Foo')
        self.assertEqual(rec.title, 'Foo Title')
        self.assertEqual(rec.status, 'scraped')

    def test_preserves_comment(self) -> None:
        rec = parse_line(
            '{"channel_id":"UC1","channel_handle":"Foo",'
            '"title":null,"status":"new","comment":"hold"}'
        )
        self.assertEqual(rec.comment, 'hold')

    def test_parses_raw_handle_with_at(self) -> None:
        rec = parse_line('@LinusTechTips')
        self.assertIsNone(rec.channel_id)
        self.assertEqual(rec.channel_handle, 'LinusTechTips')

    def test_parses_raw_channel_id(self) -> None:
        rec = parse_line('UCXuqSBlHAE6Xw-yeJA0Tunw')
        self.assertEqual(rec.channel_id, 'UCXuqSBlHAE6Xw-yeJA0Tunw')

    def test_parses_url_handle(self) -> None:
        rec = parse_line('https://youtube.com/@LinusTechTips')
        self.assertEqual(rec.channel_handle, 'LinusTechTips')

    def test_parses_url_channel_id(self) -> None:
        rec = parse_line(
            'https://youtube.com/channel/UCXuqSBlHAE6Xw-yeJA0Tunw'
        )
        self.assertEqual(rec.channel_id, 'UCXuqSBlHAE6Xw-yeJA0Tunw')

    def test_raw_string_with_spaces_treated_as_title(self) -> None:
        rec = parse_line('Linus Tech Tips')
        self.assertIsNone(rec.channel_handle)
        self.assertEqual(rec.title, 'Linus Tech Tips')


class TestFormatLine(unittest.TestCase):
    def test_canonical_field_order(self) -> None:
        rec = ChannelListRecord(
            channel_id='UC1',
            channel_handle='Foo',
            title='FooTitle',
            status='scraped',
        )
        self.assertEqual(
            format_line(rec),
            '{"channel_id":"UC1","channel_handle":"Foo",'
            '"title":"FooTitle","status":"scraped"}',
        )

    def test_nulls_explicit(self) -> None:
        rec = ChannelListRecord(
            channel_id=None,
            channel_handle='Foo',
            title=None,
            status='new',
        )
        self.assertEqual(
            format_line(rec),
            '{"channel_id":null,"channel_handle":"Foo",'
            '"title":null,"status":"new"}',
        )

    def test_comment_preserved(self) -> None:
        rec = ChannelListRecord(
            channel_id='UC1',
            channel_handle=None,
            title=None,
            status='new',
            comment='manual hold',
        )
        self.assertIn('"comment":"manual hold"', format_line(rec))


if __name__ == '__main__':
    unittest.main()
