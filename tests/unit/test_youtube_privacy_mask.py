'''
Masking of emails, phone numbers, street addresses and personal URLs
in YouTube descriptions.
'''

import unittest

from scrape_exchange.youtube.privacy import (
    HARDCODED_ALLOWED_DOMAINS,
    PRIVACY_MASKED_ITEMS,
    mask_text,
)


A: frozenset[str] = HARDCODED_ALLOWED_DOMAINS


class TestMaskEmail(unittest.TestCase):

    def test_plain_and_written_out(self) -> None:
        raw: str
        for raw in ('jan@mail.example', 'Jan.Smit+yt@Mail.Example.org',
                    'jan [at] gmail [dot] com', 'jan(at)gmail(dot)com',
                    'jan at gmail dot com', 'jan {at} gmail {dot} com',
                    'jan[at]gmail.com', 'jan @ gmail . com'):
            with self.subTest(raw=raw):
                self.assertEqual(
                    mask_text(f'mail {raw} now', A),
                    'mail <email hidden> now')

    def test_prose_with_at_is_kept(self) -> None:
        text: str = (
            'see you at noon. meet at the park dot. look at me @home'
        )
        self.assertEqual(mask_text(text, A), text)

    def test_handle_mention_is_kept(self) -> None:
        text: str = 'thanks @someone and @other.channel for the collab'
        self.assertEqual(mask_text(text, A), text)


class TestMaskPhone(unittest.TestCase):

    def test_formats(self) -> None:
        raw: str
        for raw in ('+31 6 12345678', '+1 (555) 123-4567',
                    '06-12345678', '020 123 4567', '+44 20 7946 0958',
                    '555.123.4567', '0612345678'):
            with self.subTest(raw=raw):
                self.assertEqual(
                    mask_text(f'call {raw} today', A),
                    'call <phone hidden> today')

    def test_not_phones(self) -> None:
        text: str = (
            '00:00 Intro 1:23:45 end $1,299.99 1999-2024 '
            '2026-10-22 22/10/2026 1.000.000 views 12345 subs'
        )
        self.assertEqual(mask_text(text, A), text)

    def test_chapter_list_kept(self) -> None:
        text: str = '0:00 Start\n2:15 Part one\n10:42 Part two\n1:02:03 End'
        self.assertEqual(mask_text(text, A), text)


class TestMaskAddress(unittest.TestCase):

    def test_addresses(self) -> None:
        raw: str
        for raw in ('221B Baker Street', 'Kerkstraat 12',
                    '12 rue de la Paix', 'Hauptstrasse 5',
                    '1600 Pennsylvania Avenue', 'Calle Mayor 7',
                    'Dorpsweg 3a'):
            with self.subTest(raw=raw):
                masked: str = mask_text(f'visit us at {raw}, ok', A)
                self.assertIn('<address hidden>', masked)
                self.assertNotIn(raw, masked)

    def test_prose_kept(self) -> None:
        text: str = (
            'Top 10 street food videos, 5 road trips and 3 avenues '
            'to explore'
        )
        self.assertEqual(mask_text(text, A), text)


class TestMaskUrl(unittest.TestCase):

    def test_personal_site_masked_social_kept(self) -> None:
        text: str = (
            'https://myblog.example/about www.myshop.example '
            'myband.example/tour https://instagram.com/me'
        )
        self.assertEqual(
            mask_text(text, A),
            '<url withheld> <url withheld> <url withheld> '
            'https://instagram.com/me')

    def test_trailing_punctuation_kept(self) -> None:
        self.assertEqual(
            mask_text('Site: https://myblog.example/about.', A),
            'Site: <url withheld>.')

    def test_redirect_wrapper(self) -> None:
        bad: str = (
            'https://www.youtube.com/redirect?event=x'
            '&q=https%3A%2F%2Fmyblog.example%2F'
        )
        ok: str = (
            'https://www.youtube.com/redirect?event=x'
            '&q=https%3A%2F%2Flinktr.ee%2Fme'
        )
        self.assertEqual(mask_text(bad, A), '<url withheld>')
        self.assertEqual(mask_text(ok, A), ok)

    def test_email_then_own_site(self) -> None:
        self.assertEqual(
            mask_text('me@mysite.example, see mysite.example', A),
            '<email hidden>, see <url withheld>')

    def test_url_with_digits_not_a_phone(self) -> None:
        text: str = 'https://youtube.com/watch?v=12345678901'
        self.assertEqual(mask_text(text, A), text)

    def test_file_names_kept(self) -> None:
        text: str = 'Node.js tutorial, see README.md and index.html'
        self.assertEqual(mask_text(text, A), text)

    def test_country_domains_masked(self) -> None:
        self.assertEqual(
            mask_text('bestel via mijnwinkel.nl of shop.de/jan', A),
            'bestel via <url withheld> of <url withheld>')

    def test_version_numbers_and_abbreviations_kept(self) -> None:
        text: str = 'Python 3.14 is out, e.g. this i.e. that, vs. 2.0'
        self.assertEqual(mask_text(text, A), text)


class TestMaskEmpty(unittest.TestCase):

    def test_none_and_empty(self) -> None:
        self.assertIsNone(mask_text(None, A))
        self.assertEqual(mask_text('', A), '')


class TestMaskMetrics(unittest.TestCase):

    def test_counts_masked_items(self) -> None:
        before: float = PRIVACY_MASKED_ITEMS.labels(
            kind='email')._value.get()
        mask_text('a@b.example and c@d.example', A)
        after: float = PRIVACY_MASKED_ITEMS.labels(
            kind='email')._value.get()
        self.assertEqual(after - before, 2)


if __name__ == '__main__':
    unittest.main()
