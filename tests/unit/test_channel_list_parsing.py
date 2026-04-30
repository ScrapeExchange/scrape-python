'''
Unit tests for the shared channels.lst parsing / dedup helpers
in :mod:`scrape_exchange.channel_list_parsing`. These rules are
load-bearing for both the scraper's startup read and the
operator-run cleanup tool — a mismatch between them caused the
"comma direction" and "URL form" bugs that motivated the shared
module, so each rule has at least one regression test here.
'''

import unittest

from scrape_exchange.channel_list_parsing import (
    dedupe_preserving_case,
    entry_dedupe_key,
    entry_dedupe_rank,
    entry_handle_part,
    extract_url_canonical,
    is_channel_id,
    is_json_entry,
    is_uc_id_path,
    jsonl_channel_field,
    parse_channel_handle,
    uc_id_path_to_seed_url,
)


_VALID_UC: str = 'UCkYQyvc_i9hXEo4xic9Hh2g'


class TestUrlExtraction(unittest.TestCase):

    def test_at_handle_url(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                'https://www.youtube.com/@HistoryMatters',
            ),
            'HistoryMatters',
        )

    def test_at_handle_with_subpath(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                'https://www.youtube.com/@HistoryMatters/videos',
            ),
            'HistoryMatters',
        )

    def test_at_handle_with_query(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                'https://www.youtube.com/@history_matters?si=abc',
            ),
            'history_matters',
        )

    def test_legacy_c_form(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                'https://www.youtube.com/c/SomeChannel',
            ),
            'SomeChannel',
        )

    def test_legacy_user_form(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                'https://www.youtube.com/user/legacyName',
            ),
            'legacyName',
        )

    def test_channel_id_form(self) -> None:
        self.assertEqual(
            extract_url_canonical(
                f'https://www.youtube.com/channel/{_VALID_UC}',
            ),
            _VALID_UC,
        )

    def test_subdomain_variants(self) -> None:
        for url in (
            'https://m.youtube.com/@HistoryMatters',
            'https://music.youtube.com/@HistoryMatters',
            'http://www.youtube.com/@HistoryMatters',
            'http://youtube.com/@HistoryMatters',
        ):
            self.assertEqual(
                extract_url_canonical(url),
                'HistoryMatters',
                msg=url,
            )

    def test_non_channel_youtube_url_rejected(self) -> None:
        for url in (
            'https://www.youtube.com/watch?v=abc',
            'https://www.youtube.com/playlist?list=PL123',
            'https://www.youtube.com/feed/trending',
        ):
            self.assertIsNone(extract_url_canonical(url), msg=url)

    def test_non_youtube_url_rejected(self) -> None:
        self.assertIsNone(
            extract_url_canonical('https://example.com/@foo'),
        )


class TestUcIdPath(unittest.TestCase):

    def test_valid_path(self) -> None:
        self.assertTrue(is_uc_id_path(f'channel/{_VALID_UC}'))

    def test_short_id_rejected(self) -> None:
        self.assertFalse(is_uc_id_path('channel/UC123'))

    def test_too_long_id_rejected(self) -> None:
        self.assertFalse(
            is_uc_id_path(f'channel/{_VALID_UC}XX'),
        )

    def test_no_prefix_rejected(self) -> None:
        self.assertFalse(is_uc_id_path(_VALID_UC))

    def test_to_seed_url(self) -> None:
        self.assertEqual(
            uc_id_path_to_seed_url(f'channel/{_VALID_UC}'),
            f'https://www.youtube.com/channel/{_VALID_UC}',
        )


class TestJsonHelpers(unittest.TestCase):

    def test_is_json_entry_object(self) -> None:
        self.assertTrue(is_json_entry('{"channel":"foo"}'))

    def test_is_json_entry_list(self) -> None:
        self.assertTrue(is_json_entry('[1, 2, 3]'))

    def test_is_json_entry_invalid(self) -> None:
        self.assertFalse(is_json_entry('{not valid'))
        self.assertFalse(is_json_entry('plain'))

    def test_jsonl_channel_field_extracts(self) -> None:
        self.assertEqual(
            jsonl_channel_field(
                '{"channel":"HistoryMatters","subs":1234}'
            ),
            'HistoryMatters',
        )

    def test_jsonl_channel_field_missing(self) -> None:
        self.assertIsNone(
            jsonl_channel_field('{"foo":"bar"}'),
        )

    def test_jsonl_channel_field_non_string(self) -> None:
        self.assertIsNone(
            jsonl_channel_field('{"channel":42}'),
        )


class TestHandlePart(unittest.TestCase):

    def test_returns_part_before_comma(self) -> None:
        self.assertEqual(
            entry_handle_part('MyChannel,extra'),
            'MyChannel',
        )

    def test_returns_none_for_no_comma(self) -> None:
        self.assertIsNone(entry_handle_part('MyChannel'))

    def test_returns_none_for_json(self) -> None:
        self.assertIsNone(
            entry_handle_part('{"channel":"foo,bar"}'),
        )

    def test_strips_whitespace(self) -> None:
        self.assertEqual(
            entry_handle_part('  Spaced  ,extra'),
            'Spaced',
        )


class TestDedupe(unittest.TestCase):

    def test_case_insensitive_dedup_keeps_uppercase(self) -> None:
        result: list[str] = dedupe_preserving_case(
            ['mychannel', 'MyChannel'],
        )
        self.assertEqual(result, ['MyChannel'])

    def test_jsonl_wins_over_plain(self) -> None:
        result: list[str] = dedupe_preserving_case([
            'MyChannel',
            '{"channel":"MyChannel","subs":1000}',
        ])
        self.assertEqual(
            result,
            ['{"channel":"MyChannel","subs":1000}'],
        )

    def test_url_loses_to_handle(self) -> None:
        result: list[str] = dedupe_preserving_case([
            'https://www.youtube.com/@MyChannel',
            'MyChannel',
        ])
        self.assertEqual(result, ['MyChannel'])

    def test_url_only_survives(self) -> None:
        '''A URL with no plain-handle counterpart stays.'''
        result: list[str] = dedupe_preserving_case(
            ['https://www.youtube.com/@OnlyAUrl'],
        )
        self.assertEqual(
            result, ['https://www.youtube.com/@OnlyAUrl'],
        )

    def test_url_subpaths_dedup(self) -> None:
        result: list[str] = dedupe_preserving_case([
            'https://www.youtube.com/@HistoryMatters',
            'https://www.youtube.com/@HistoryMatters/videos',
        ])
        self.assertEqual(len(result), 1)

    def test_comma_form_dedups_with_plain_handle(self) -> None:
        result: list[str] = dedupe_preserving_case([
            'MyChannel,2026-04-01',
            'mychannel',
        ])
        # Comma-form's handle part is "MyChannel" (mixed case),
        # ranked higher than the lowercase plain entry.
        self.assertEqual(result, ['MyChannel,2026-04-01'])

    def test_preserves_first_appearance_order(self) -> None:
        result: list[str] = dedupe_preserving_case([
            'A', 'B', 'a', 'C', 'b',
        ])
        # Both 'A' and 'a' have rank 0/1 → uppercase wins,
        # preserves insertion order of groups
        self.assertEqual(result, ['A', 'B', 'C'])


class TestEntryDedupeKey(unittest.TestCase):

    def test_url_keys_on_extracted(self) -> None:
        self.assertEqual(
            entry_dedupe_key(
                'https://www.youtube.com/@HistoryMatters',
            ),
            'historymatters',
        )

    def test_uc_url_keys_on_id(self) -> None:
        self.assertEqual(
            entry_dedupe_key(
                f'https://www.youtube.com/channel/{_VALID_UC}',
            ),
            _VALID_UC.lower(),
        )

    def test_jsonl_keys_on_channel_field(self) -> None:
        self.assertEqual(
            entry_dedupe_key('{"channel":"HistoryMatters"}'),
            'historymatters',
        )

    def test_comma_keys_on_leading_part(self) -> None:
        self.assertEqual(
            entry_dedupe_key('MyChannel,extra'),
            'mychannel',
        )

    def test_plain_keys_on_lower(self) -> None:
        self.assertEqual(
            entry_dedupe_key('MyChannel'),
            'mychannel',
        )


class TestEntryDedupeRank(unittest.TestCase):

    def test_ranks(self) -> None:
        self.assertEqual(
            entry_dedupe_rank('{"channel":"X"}'), 2,
        )
        self.assertEqual(
            entry_dedupe_rank(
                'https://www.youtube.com/@HistoryMatters'
            ),
            -1,
        )
        self.assertEqual(entry_dedupe_rank('MyChannel'), 1)
        self.assertEqual(entry_dedupe_rank('mychannel'), 0)


class TestParseChannelHandle(unittest.TestCase):

    def test_blank_skipped(self) -> None:
        self.assertEqual(parse_channel_handle(''), (None, None))
        self.assertEqual(
            parse_channel_handle('   '), (None, None),
        )

    def test_comment_skipped(self) -> None:
        self.assertEqual(
            parse_channel_handle('# header'),
            (None, None),
        )

    def test_display_name_skipped(self) -> None:
        '''Lines with whitespace look like display names.'''
        h, u = parse_channel_handle('Some Display Name')
        self.assertEqual((h, u), (None, None))

    def test_bare_handle(self) -> None:
        self.assertEqual(
            parse_channel_handle('HistoryMatters'),
            ('HistoryMatters', None),
        )

    def test_handle_with_at(self) -> None:
        self.assertEqual(
            parse_channel_handle('@HistoryMatters'),
            ('HistoryMatters', None),
        )

    def test_bare_uc_id(self) -> None:
        h, u = parse_channel_handle(_VALID_UC)
        self.assertIsNone(h)
        self.assertEqual(u, _VALID_UC)

    def test_lowercase_uc_id(self) -> None:
        '''Tolerate lowercase ``uc`` prefix on channel ids.'''
        h, u = parse_channel_handle(_VALID_UC[:2].lower() + _VALID_UC[2:])
        self.assertIsNone(h)
        self.assertEqual(u, _VALID_UC)

    def test_channel_uc_path(self) -> None:
        h, u = parse_channel_handle(f'channel/{_VALID_UC}')
        self.assertIsNone(h)
        self.assertEqual(u, _VALID_UC)

    def test_at_url(self) -> None:
        self.assertEqual(
            parse_channel_handle(
                'https://www.youtube.com/@HistoryMatters',
            ),
            ('HistoryMatters', None),
        )

    def test_c_url(self) -> None:
        '''/c/Foo URLs were NOT recognised by the scraper before.'''
        self.assertEqual(
            parse_channel_handle(
                'https://www.youtube.com/c/Foo',
            ),
            ('Foo', None),
        )

    def test_user_url(self) -> None:
        self.assertEqual(
            parse_channel_handle(
                'https://www.youtube.com/user/legacy',
            ),
            ('legacy', None),
        )

    def test_channel_uc_url(self) -> None:
        h, u = parse_channel_handle(
            f'https://www.youtube.com/channel/{_VALID_UC}',
        )
        self.assertIsNone(h)
        self.assertEqual(u, _VALID_UC)

    def test_mobile_subdomain_url(self) -> None:
        self.assertEqual(
            parse_channel_handle(
                'https://m.youtube.com/@Foo',
            ),
            ('Foo', None),
        )

    def test_jsonl(self) -> None:
        self.assertEqual(
            parse_channel_handle(
                '{"channel":"HistoryMatters","subs":1234}',
            ),
            ('HistoryMatters', None),
        )

    def test_jsonl_with_url_inside(self) -> None:
        '''If JSONL's channel field is itself a URL, recurse.'''
        h, u = parse_channel_handle(
            '{"channel":"https://www.youtube.com/@Foo"}'
        )
        self.assertEqual((h, u), ('Foo', None))

    def test_comma_takes_leading_part(self) -> None:
        '''Critical regression: the scraper used to take the
        AFTER-comma half. Now it takes BEFORE, matching cleanup.'''
        self.assertEqual(
            parse_channel_handle('HistoryMatters,2026-04-01'),
            ('HistoryMatters', None),
        )

    def test_tab_takes_second_column(self) -> None:
        self.assertEqual(
            parse_channel_handle('UC123\tHistoryMatters'),
            ('HistoryMatters', None),
        )

    def test_unrecognised_url_skipped(self) -> None:
        h, u = parse_channel_handle(
            'https://example.com/@foo',
        )
        self.assertEqual((h, u), (None, None))

    def test_strips_at_in_comma_form(self) -> None:
        self.assertEqual(
            parse_channel_handle('@MyChannel,extra'),
            ('MyChannel', None),
        )


class TestIsChannelId(unittest.TestCase):

    def test_valid(self) -> None:
        self.assertTrue(is_channel_id(_VALID_UC))

    def test_short(self) -> None:
        self.assertFalse(is_channel_id('UC123'))

    def test_empty(self) -> None:
        self.assertFalse(is_channel_id(''))


if __name__ == '__main__':
    unittest.main()
