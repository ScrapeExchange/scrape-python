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

    def test_title_case_phrases_kept(self) -> None:
        text: str
        for text in ('5 Tips To Find Your Way',
                     '10 Steps To The Right Place',
                     'Win 3 Tickets To Madison Square',
                     '2 Hours Of Night Drive',
                     '1 Night At The Supreme Court'):
            with self.subTest(text=text):
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



class TestReviewFalsePositives(unittest.TestCase):
    '''Ordinary description text that must stay unmasked.'''

    def _assert_kept(self, *texts: str) -> None:
        text: str
        for text in texts:
            with self.subTest(text=text):
                self.assertEqual(mask_text(text, A), text)

    def test_missing_space_after_full_stop(self) -> None:
        self._assert_kept(
            'Thanks for watching.Me and my friends.',
            'Enjoy.In this video. Great.No way. Wow.It works.',
            'Built with ASP.NET, Vb.net and Socket.io',
            'Danke.Es war gut. Merci.De rien.',
        )

    def test_address_lookalikes(self) -> None:
        self._assert_kept(
            'Spring 2026 collection',
            'Engineering 101 and Mastering 3',
            'Up to 100 via PayPal.',
            'Step 2 via Zapier',
            'Send 5 via email',
            '3 place cards',
            'Top 10 Best Street food',
            'Top 10 Best Road Trips',
            '10 Best Lane changes',
        )

    def test_number_lists(self) -> None:
        self._assert_kept(
            'Open Mon-Fri 10.00-18.00',
            'Specs: 1.5 2.0 3.5 4.0',
            'scores 3-2 1-0 2-2 4-1',
            'Resolution 1920 1080',
            'build 20261022',
            'order 12345678',
        )

    def test_real_contacts_still_masked(self) -> None:
        masked: str = mask_text(
            'call +31 6 12345678 or 020 123 4567, shop at '
            'mijnwinkel.nl or myblog.example/about, visit '
            'Kerkstraat 12 or 221B Baker Street', A,
        )
        self.assertEqual(
            masked,
            'call <phone hidden> or <phone hidden>, shop at '
            '<url withheld> or <url withheld>, visit '
            '<address hidden> or <address hidden>',
        )


class TestReviewRobustness(unittest.TestCase):

    def test_private_use_characters_do_not_crash(self) -> None:
        text: str = 'a \ue000X\ue001 b \ue000\ue100\ue001'
        self.assertEqual(mask_text(text, A), 'a X b ')

    def test_nested_redirect_is_unwrapped(self) -> None:
        nested: str = (
            'https://www.youtube.com/redirect?q=https%3A%2F%2F'
            'www.youtube.com%2Fredirect%3Fq%3Dhttps%253A%252F%252F'
            'evil.example'
        )
        self.assertEqual(mask_text(nested, A), '<url withheld>')


if __name__ == '__main__':
    unittest.main()
