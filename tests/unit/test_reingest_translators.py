'''
Unit tests for the field-mapping translators in
tools/reingest_from_archive.py. The translator functions encode the
decisions made when collapsing the legacy ``channel`` / ``title`` /
``canonical_handle`` / ``channel_name`` fields into the new
``channel_handle`` / ``title`` vocabulary.
'''

import tempfile
import unittest

from pathlib import Path

from tools.reingest_from_archive import (
    _enumerate_archive,
    _unwrap_server_envelope,
    _unwrap_unknown_video,
    _video_dest_filename,
    _video_target_prefix,
    translate_channel,
    translate_video,
)


_TEST_CHANNEL_ID: str = 'UCabcdefghijklmnopqrstuv'
_TEST_VIDEO_ID: str = 'dQw4w9WgXcQ'


class TestTranslateChannelHandlePreference(unittest.TestCase):
    '''
    The new ``channel_handle`` slot collapses three legacy slots:
    ``canonical_handle`` (most authoritative), ``channel`` (URL slug
    or fallback), and ``channel_name`` (older name). Resolution prefers
    them in that order. We never derive a handle from ``title``: a
    record without an authoritative handle is dropped.
    '''

    def test_prefers_canonical_handle(self) -> None:
        old: dict = {
            'canonical_handle': 'HistoryMatters',
            'channel': 'historymatters',
            'channel_name': 'history-matters-old',
            'title': 'History Matters',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'HistoryMatters')
        self.assertEqual(new['title'], 'History Matters')

    def test_falls_back_to_channel_when_no_canonical(self) -> None:
        old: dict = {
            'channel': 'HistoryMatters',
            'title': 'History Matters',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'HistoryMatters')

    def test_falls_back_to_channel_name_when_no_channel(self) -> None:
        old: dict = {
            'channel_name': 'historymatters',
            'title': 'History Matters',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'historymatters')

    def test_returns_none_when_only_title_present(self) -> None:
        '''Title alone is not enough — we never derive the handle
        from the display name.'''
        old: dict = {
            'title': '@LegacyChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_no_handle_or_title(self) -> None:
        old: dict = {'channel_id': _TEST_CHANNEL_ID}
        self.assertIsNone(translate_channel(old))

    def test_strips_leading_at_from_handle(self) -> None:
        old: dict = {
            'canonical_handle': '@HistoryMatters',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'HistoryMatters')


class TestTranslateChannelRequiresChannelId(unittest.TestCase):
    '''
    A translated channel record is only emitted when both the
    handle and the channel_id are known. Either alone yields None.
    '''

    def test_returns_none_when_channel_id_missing(self) -> None:
        old: dict = {'canonical_handle': 'HistoryMatters'}
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_channel_id_empty(self) -> None:
        old: dict = {
            'canonical_handle': 'HistoryMatters',
            'channel_id': '',
        }
        self.assertIsNone(translate_channel(old))


class TestTranslateChannelHandleLooksLikeHandle(unittest.TestCase):
    '''
    The resolved handle must match YouTube's documented handle
    format. Legacy slots sometimes held a display name (``"My
    Channel Name"``) or other junk; those records are dropped
    rather than re-ingested with an invalid handle.
    '''

    def test_returns_none_when_handle_has_space(self) -> None:
        old: dict = {
            'canonical_handle': 'My Channel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_handle_has_slash(self) -> None:
        old: dict = {
            'channel': 'foo/bar',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_handle_too_short(self) -> None:
        old: dict = {
            'canonical_handle': 'ab',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_handle_too_long(self) -> None:
        old: dict = {
            'canonical_handle': 'a' * 31,
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_accepts_handle_with_period_underscore_hyphen(
        self,
    ) -> None:
        old: dict = {
            'canonical_handle': 'a.b_c-d',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'a.b_c-d')

    def test_accepts_url_encoded_handle_lowercase(self) -> None:
        '''
        Some legacy archive files store the handle URL-encoded.
        The translator must percent-decode before validating, so a
        record whose ``channel`` slot holds the percent-encoded
        UTF-8 bytes of ``ΕκδόσειςΊτανος`` round-trips to that
        Greek handle.
        '''

        encoded: str = (
            '%ce%95%ce%ba%ce%b4%cf%8c%cf%83%ce%b5%ce%b9%cf%82'
            '%ce%8a%cf%84%ce%b1%ce%bd%ce%bf%cf%82'
        )
        old: dict = {
            'channel': encoded,
            'channel_id': 'UCFGTVIo4sa1XFBKhKUlbNLA',
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(
            new['channel_handle'], 'ΕκδόσειςΊτανος',
        )

    def test_accepts_url_encoded_handle_uppercase(self) -> None:
        '''
        YouTube URLs themselves use uppercase percent-encoding
        (e.g. ``%CE%95``). ``unquote`` is case-insensitive but
        pinning the uppercase form prevents a future regression.
        '''

        encoded: str = (
            '%CE%95%CE%BA%CE%B4%CF%8C%CF%83%CE%B5%CE%B9%CF%82'
            '%CE%8A%CF%84%CE%B1%CE%BD%CE%BF%CF%82'
        )
        old: dict = {
            'channel': encoded,
            'channel_id': 'UCFGTVIo4sa1XFBKhKUlbNLA',
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(
            new['channel_handle'], 'ΕκδόσειςΊτανος',
        )

    def test_accepts_handle_with_trademark_symbol(self) -> None:
        '''
        YouTube accepts symbol characters (``™``, ``®``, ``©``, …)
        in handles even though they're outside Unicode word
        categories. Pinned by the real handle ``@THENvsNOW™``,
        confirmed by browsing.
        '''

        old: dict = {
            'canonical_handle': 'THENvsNOW™',
            'channel_id': 'UCpQL1LXdI_2pnzrRqk1APWw',
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'THENvsNOW™')

    def test_returns_none_when_handle_has_question_mark(self) -> None:
        old: dict = {
            'canonical_handle': 'foo?bar',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_handle_has_hash(self) -> None:
        old: dict = {
            'canonical_handle': 'foo#bar',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_returns_none_when_handle_has_internal_at(self) -> None:
        '''A leading ``@`` is stripped before validation, but ``@``
        in the middle still indicates junk.'''
        old: dict = {
            'canonical_handle': 'foo@bar',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))

    def test_accepts_unicode_handle(self) -> None:
        '''
        YouTube accepts handles in non-Latin scripts (Cyrillic,
        Greek, CJK, accented Latin, …). The pattern uses ``\\w``
        so any Unicode letter/digit qualifies. Pinned by the
        real-world handle ``@ГеоргиГеоргиев-ь4ч``.
        '''

        old: dict = {
            'canonical_handle': 'ГеоргиГеоргиев-ь4ч',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(
            new['channel_handle'], 'ГеоргиГеоргиев-ь4ч',
        )

    def test_falls_through_when_canonical_invalid(
        self,
    ) -> None:
        '''A record where the most authoritative slot is junk
        but a lower-priority slot is valid is still dropped:
        we always honour the priority order, so junk in
        ``canonical_handle`` is what we get.'''
        old: dict = {
            'canonical_handle': 'has space',
            'channel': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_channel(old))


class TestTranslateChannelObsoleteSlotsDropped(unittest.TestCase):
    '''
    The legacy slots that conflated handle/title must not leak into
    the translated record.
    '''

    def test_drops_legacy_slots(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel': 'xchannel',
            'channel_name': 'x_old',
            'title': 'X Display',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        for legacy in ('canonical_handle', 'channel', 'channel_name'):
            self.assertNotIn(legacy, new)
        # ``title`` is the new schema's display-name field and must
        # be preserved, not stripped.
        self.assertEqual(new['title'], 'X Display')


class TestTranslateChannelCategory(unittest.TestCase):
    '''
    The channel schema has an optional ``category`` (single string)
    slot. Legacy records that over-modelled it as a list collapse
    to the first entry; an empty list becomes ``None``.
    '''

    def test_categories_list_collapses_to_first_entry(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'categories': ['Music'],
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')
        self.assertNotIn('categories', new)

    def test_empty_categories_list_yields_none(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'categories': [],
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertIsNone(new['category'])
        self.assertNotIn('categories', new)

    def test_multiple_categories_takes_first(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'categories': ['Music', 'Entertainment'],
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')

    def test_missing_categories_leaves_record_alone(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertNotIn('category', new)
        self.assertNotIn('categories', new)


class TestTranslateChannelLinks(unittest.TestCase):
    '''
    Featured/related channel entries also use the new vocabulary.
    '''

    def test_renames_channel_links_channel_name(self) -> None:
        old: dict = {
            'channel': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'channel_links': [
                {'channel_name': 'FeaturedChannel', 'subscriber_count': 100},
                {'channel_name': 'AnotherOne', 'subscriber_count': 200},
            ],
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(len(new['channel_links']), 2)
        for link in new['channel_links']:
            self.assertIn('channel_handle', link)
            self.assertNotIn('channel_name', link)

    def test_passes_through_already_renamed_links(self) -> None:
        '''
        If an archive file was partially-converted, links already
        carrying ``channel_handle`` should be preserved unchanged.
        '''
        old: dict = {
            'channel': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'channel_links': [
                {'channel_handle': 'AlreadyDone', 'subscriber_count': 1},
            ],
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(
            new['channel_links'][0]['channel_handle'], 'AlreadyDone',
        )

    def test_missing_channel_links_is_empty_list(self) -> None:
        new: dict | None = translate_channel({
            'channel': 'XChannel', 'channel_id': _TEST_CHANNEL_ID,
        })
        assert new is not None
        self.assertEqual(new['channel_links'], [])


class TestTranslateChannelPassThrough(unittest.TestCase):
    '''
    Fields outside the renamed set must round-trip unchanged.
    '''

    def test_channel_id_and_video_ids_preserved(self) -> None:
        old: dict = {
            'channel_id': _TEST_CHANNEL_ID,
            'channel': 'XChannel',
            'video_ids': ['v1', 'v2', 'v3'],
            'subscriber_count': 1000,
            'description': 'A channel.',
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)
        self.assertEqual(new['video_ids'], ['v1', 'v2', 'v3'])
        self.assertEqual(new['subscriber_count'], 1000)
        self.assertEqual(new['description'], 'A channel.')

    def test_does_not_mutate_input(self) -> None:
        old: dict = {
            'channel': 'XChannel', 'title': 'X Display',
            'channel_id': _TEST_CHANNEL_ID,
            'channel_links': [
                {'channel_name': 'F', 'subscriber_count': 1},
            ],
        }
        original = dict(old)
        original_link = dict(old['channel_links'][0])
        translate_channel(old)
        self.assertEqual(old, original)
        self.assertEqual(old['channel_links'][0], original_link)


class TestTranslateVideo(unittest.TestCase):
    '''
    Per-video records: ``channel_name`` → ``channel_handle``; both
    ``channel_handle`` and ``channel_id`` must be present or the
    record is dropped.
    '''

    def test_renames_channel_name(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'title': 'A Video',
            'channel_id': _TEST_CHANNEL_ID,
            'channel_name': 'HistoryMatters',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'HistoryMatters')
        self.assertNotIn('channel_name', new)
        self.assertEqual(new['title'], 'A Video')
        self.assertEqual(new['video_id'], _TEST_VIDEO_ID)

    def test_passes_through_when_already_renamed(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'XChannel')

    def test_strips_handle_when_file_handle_is_junk(self) -> None:
        '''
        A junk file handle (whitespace, URL-special chars, etc.)
        is dropped from the output rather than written through.
        With a valid ``channel_id`` and no CreatorMap recovery,
        the record is kept without a ``channel_handle`` field.
        '''

        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'has space',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertNotIn('channel_handle', new)
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)

    def test_keeps_record_without_handle_when_channel_id_present(
        self,
    ) -> None:
        '''
        Videos with a valid ``channel_id`` but no resolvable handle
        (and no CreatorMap entry) are kept — the canonical handle
        can be filled in later. The output must NOT carry a
        ``channel_handle`` field in that case.
        '''

        old: dict = {
            'video_id': _TEST_VIDEO_ID, 'title': 'T',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)
        self.assertNotIn('channel_handle', new)

    def test_returns_none_when_channel_id_missing(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID, 'title': 'T',
            'channel_handle': 'HistoryMatters',
        }
        self.assertIsNone(translate_video(old))

    def test_returns_none_when_channel_id_empty(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'HistoryMatters',
            'channel_id': '',
        }
        self.assertIsNone(translate_video(old))


class TestTranslateVideoCategory(unittest.TestCase):
    '''
    YouTube assigns each video a single category from a fixed
    vocabulary; the legacy list-shaped slot collapses to a string.
    '''

    def test_categories_list_collapses_to_first_entry(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
            'categories': ['Music'],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')
        self.assertNotIn('categories', new)

    def test_empty_categories_list_yields_none(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
            'categories': [],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertIsNone(new['category'])
        self.assertNotIn('categories', new)

    def test_missing_categories_leaves_record_alone(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID, 'title': 'T',
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertNotIn('category', new)
        self.assertNotIn('categories', new)

    def test_multiple_categories_takes_first(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
            'categories': ['Music', 'Entertainment'],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')


class TestTranslateChannelCreatorMapRecovery(unittest.TestCase):
    '''
    When the in-file record carries only one of (channel_id,
    channel_handle), the translator consults the CreatorMap-derived
    lookup tables to fill in the missing half. A junk in-file handle
    is also overridden by the map's value when channel_id is known
    (per project policy).
    '''

    def test_fills_handle_from_id_when_handle_missing(self) -> None:
        old: dict = {
            'channel_id': _TEST_CHANNEL_ID, 'title': 'X Display',
        }
        # Without map: dropped because no handle.
        self.assertIsNone(translate_channel(old))
        # With forward map: handle recovered.
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'XChannel',
        }
        new: dict | None = translate_channel(
            old, id_to_handle, {},
        )
        assert new is not None
        self.assertEqual(new['channel_handle'], 'XChannel')
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)

    def test_overrides_junk_handle_when_id_present(self) -> None:
        old: dict = {
            'canonical_handle': 'has space',
            'channel_id': _TEST_CHANNEL_ID,
        }
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'XChannel',
        }
        new: dict | None = translate_channel(
            old, id_to_handle, {},
        )
        assert new is not None
        self.assertEqual(new['channel_handle'], 'XChannel')

    def test_fills_id_from_handle_when_id_missing(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel', 'title': 'X Display',
        }
        # Without map: dropped because no channel_id.
        self.assertIsNone(translate_channel(old))
        # With reverse map: channel_id recovered.
        handle_to_id: dict[str, str] = {
            'XChannel': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(
            old, {}, handle_to_id,
        )
        assert new is not None
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)
        self.assertEqual(new['channel_handle'], 'XChannel')

    def test_drops_when_mapped_handle_also_invalid(self) -> None:
        old: dict = {'channel_id': _TEST_CHANNEL_ID}
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'has space',
        }
        self.assertIsNone(
            translate_channel(old, id_to_handle, {}),
        )

    def test_drops_when_lookup_misses(self) -> None:
        old: dict = {'channel_id': _TEST_CHANNEL_ID}
        id_to_handle: dict[str, str] = {
            'UCotherchannelxxxxxxxxxx': 'XChannel',
        }
        self.assertIsNone(
            translate_channel(old, id_to_handle, {}),
        )

    def test_does_not_consult_map_when_both_present(self) -> None:
        '''Map should not override valid in-file values.'''
        old: dict = {
            'canonical_handle': 'FromFile',
            'channel_id': _TEST_CHANNEL_ID,
        }
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'FromMap',
        }
        new: dict | None = translate_channel(
            old, id_to_handle, {},
        )
        assert new is not None
        self.assertEqual(new['channel_handle'], 'FromFile')


class TestTranslateVideoCreatorMapRecovery(unittest.TestCase):
    '''
    Same recovery rules as channels: fill the missing half of the
    (channel_id, channel_handle) pair from the CreatorMap when the
    file carries only one, and override a junk in-file handle when
    a valid mapped handle is available for the known channel_id.
    '''

    def test_fills_handle_from_id_when_handle_missing(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
        }
        # Without map: kept but channel_handle stripped (the
        # video data is still useful even when the handle is
        # unknown).
        baseline: dict | None = translate_video(old)
        assert baseline is not None
        self.assertNotIn('channel_handle', baseline)
        # With the map providing a valid handle: recovered.
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'XChannel',
        }
        new: dict | None = translate_video(old, id_to_handle, {})
        assert new is not None
        self.assertEqual(new['channel_handle'], 'XChannel')

    def test_overrides_junk_handle_when_id_present(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'has space',
            'channel_id': _TEST_CHANNEL_ID,
        }
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'XChannel',
        }
        new: dict | None = translate_video(old, id_to_handle, {})
        assert new is not None
        self.assertEqual(new['channel_handle'], 'XChannel')

    def test_fills_id_from_handle_when_id_missing(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID, 'channel_handle': 'XChannel',
        }
        self.assertIsNone(translate_video(old))
        handle_to_id: dict[str, str] = {
            'XChannel': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_video(old, {}, handle_to_id)
        assert new is not None
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)

    def test_strips_handle_when_mapped_handle_invalid(self) -> None:
        '''
        A junk value in the CreatorMap (e.g. ``'has space'``) is
        rejected by the validator, so the record is kept with a
        valid channel_id and no ``channel_handle`` — same as if
        the map lookup had missed entirely.
        '''

        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
        }
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'has space',
        }
        new: dict | None = translate_video(old, id_to_handle, {})
        assert new is not None
        self.assertNotIn('channel_handle', new)
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)


class TestChannelIdPattern(unittest.TestCase):
    '''
    Both translators must reject records whose ``channel_id`` does
    not match YouTube's ``UC`` + 22 base64url-character format. A
    junk channel_id is treated as missing — recovery via
    handle_to_id is allowed, but the bare junk value is never
    written through.
    '''

    def test_channel_translator_drops_short_channel_id(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel', 'channel_id': 'UCabc',
        }
        self.assertIsNone(translate_channel(old))

    def test_channel_translator_drops_non_uc_prefix(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            # 24 chars but not UC-prefixed
            'channel_id': 'XYabcdefghijklmnopqrstuv',
        }
        self.assertIsNone(translate_channel(old))

    def test_channel_translator_drops_disallowed_chars(self) -> None:
        old: dict = {
            'canonical_handle': 'XChannel',
            # contains '+' which is not URL-safe base64
            'channel_id': 'UCabcdefghijklmnopqrst+v',
        }
        self.assertIsNone(translate_channel(old))

    def test_video_translator_drops_short_channel_id(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'XChannel',
            'channel_id': 'UCabc',
        }
        self.assertIsNone(translate_video(old))

    def test_recovers_id_when_file_id_invalid(self) -> None:
        '''An invalid file channel_id is treated as missing, so
        the handle_to_id reverse lookup can still rescue the
        record.'''
        old: dict = {
            'canonical_handle': 'XChannel',
            'channel_id': 'UCabc',  # junk
        }
        handle_to_id: dict[str, str] = {
            'XChannel': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_channel(
            old, {}, handle_to_id,
        )
        assert new is not None
        self.assertEqual(new['channel_id'], _TEST_CHANNEL_ID)

    def test_rejects_invalid_id_from_reverse_map(self) -> None:
        '''A reverse-map result that is itself a malformed
        channel_id must not be accepted.'''
        old: dict = {'canonical_handle': 'XChannel'}
        handle_to_id: dict[str, str] = {'XChannel': 'UCabc'}
        self.assertIsNone(
            translate_channel(old, {}, handle_to_id),
        )


class TestTranslateVideoUrl(unittest.TestCase):
    '''
    Every YouTube video has the same canonical watch URL shape
    (``https://www.youtube.com/watch?v=<video_id>``). The
    translator fills it in when the source record didn't carry
    a ``url`` field, but leaves an existing one alone.
    '''

    def test_fills_missing_url(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(
            new['url'],
            f'https://www.youtube.com/watch?v={_TEST_VIDEO_ID}',
        )

    def test_preserves_existing_url(self) -> None:
        existing: str = (
            'https://music.youtube.com/watch?v=' + _TEST_VIDEO_ID
        )
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'url': existing,
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['url'], existing)

    def test_fills_when_existing_url_is_empty(self) -> None:
        '''
        Empty / null URL slots count as "missing" — fill them in
        with the canonical form rather than writing junk through.
        '''

        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
            'url': '',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(
            new['url'],
            f'https://www.youtube.com/watch?v={_TEST_VIDEO_ID}',
        )


class TestVideoIdPattern(unittest.TestCase):
    '''
    The video translator must reject records whose ``video_id``
    is missing or doesn't match the YouTube 11-char URL-safe base64
    format. Without a valid video_id, the record can't be uniquely
    keyed in the data directory.
    '''

    def test_drops_missing_video_id(self) -> None:
        old: dict = {
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_video(old))

    def test_drops_short_video_id(self) -> None:
        old: dict = {
            'video_id': 'abc',
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_video(old))

    def test_drops_long_video_id(self) -> None:
        old: dict = {
            'video_id': 'a' * 12,
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_video(old))

    def test_drops_disallowed_char_video_id(self) -> None:
        old: dict = {
            # contains '+' which is not URL-safe base64
            'video_id': 'abc+efghijk',
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_video(old))

    def test_accepts_valid_video_id(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_handle': 'XChannel',
            'channel_id': _TEST_CHANNEL_ID,
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['video_id'], _TEST_VIDEO_ID)


class TestUnwrapServerEnvelope(unittest.TestCase):
    '''
    Files served by scrape.exchange wrap the actual scraped record
    inside a ``data`` field at the top level, with envelope
    metadata (``platform_content_id``, ``platform_creator_id``,
    ``source_url``) sitting alongside it. The unwrap helper makes
    those records look like scraper-format records to the
    translators.
    '''

    def test_unwraps_when_envelope_keys_present(self) -> None:
        inner: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'XChannel',
        }
        wrapped: dict = {
            'platform_content_id': _TEST_VIDEO_ID,
            'platform_creator_id': _TEST_CHANNEL_ID,
            'source_url': 'https://www.youtube.com/watch?v=' + _TEST_VIDEO_ID,
            'data': inner,
        }
        self.assertIs(_unwrap_server_envelope(wrapped), inner)

    def test_unwraps_with_only_one_envelope_marker(self) -> None:
        '''A single envelope marker (any of the recognised keys)
        is enough to fire the unwrap.'''
        inner: dict = {'channel_id': _TEST_CHANNEL_ID}
        wrapped: dict = {
            'platform_creator_id': _TEST_CHANNEL_ID,
            'data': inner,
        }
        self.assertIs(_unwrap_server_envelope(wrapped), inner)

    def test_does_not_unwrap_without_envelope_markers(self) -> None:
        '''A scraper-format file that happens to carry a ``data``
        key (no envelope markers) must be left alone.'''
        scraper_format: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'data': {'irrelevant': True},
        }
        self.assertIs(
            _unwrap_server_envelope(scraper_format), scraper_format,
        )

    def test_does_not_unwrap_when_data_not_dict(self) -> None:
        wrapped: dict = {
            'platform_content_id': _TEST_VIDEO_ID,
            'data': 'not a dict',
        }
        self.assertIs(_unwrap_server_envelope(wrapped), wrapped)

    def test_does_not_unwrap_when_no_data_key(self) -> None:
        wrapped: dict = {
            'platform_content_id': _TEST_VIDEO_ID,
            'platform_creator_id': _TEST_CHANNEL_ID,
        }
        self.assertIs(_unwrap_server_envelope(wrapped), wrapped)


class TestEnumerateArchive(unittest.TestCase):
    '''
    ``_enumerate_archive`` recursively finds matching files under
    the archive root and follows directory symlinks (so an archive
    spread across mounted disks via symlinked subtrees is walked
    end to end).
    '''

    def test_finds_files_under_real_subdir(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root: Path = Path(d)
            (root / 'sub').mkdir()
            (root / 'sub' / 'channel-a.json.br').write_text('x')
            found: list[str] = _enumerate_archive(root, 'channel-')
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0].endswith('sub/channel-a.json.br'))

    def test_follows_internal_directory_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root: Path = Path(d)
            (root / 'real').mkdir()
            (root / 'real' / 'channel-a.json.br').write_text('x')
            (root / 'link').symlink_to(
                root / 'real', target_is_directory=True,
            )
            found: list[str] = sorted(
                _enumerate_archive(root, 'channel-'),
            )
        # File reachable via both real and link paths.
        self.assertEqual(len(found), 2)
        self.assertTrue(any('real/channel-a.json.br' in p for p in found))
        self.assertTrue(any('link/channel-a.json.br' in p for p in found))

    def test_follows_external_directory_symlink(self) -> None:
        with (
            tempfile.TemporaryDirectory() as archive_d,
            tempfile.TemporaryDirectory() as external_d,
        ):
            root: Path = Path(archive_d)
            external: Path = Path(external_d)
            (external / 'channel-ext.json.br').write_text('x')
            (root / 'extlink').symlink_to(
                external, target_is_directory=True,
            )
            found: list[str] = _enumerate_archive(root, 'channel-')
        self.assertEqual(len(found), 1)
        self.assertTrue(
            found[0].endswith('extlink/channel-ext.json.br'),
        )

    def test_filters_by_prefix(self) -> None:
        '''
        ``_enumerate_archive`` matches ``<prefix>*.json.br`` so the
        channel dispatch only sees ``channel-*.json.br`` and the
        video dispatch only sees ``video-*.json.br``, even when the
        two record types share an archive root.
        '''

        with tempfile.TemporaryDirectory() as d:
            root: Path = Path(d)
            (root / 'channel-a.json.br').write_text('x')
            (root / 'video-a.json.br').write_text('y')
            found: list[str] = _enumerate_archive(root, 'channel-')
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0].endswith('channel-a.json.br'))


class TestVideoUnknownHandling(unittest.TestCase):
    '''
    ``video-unknown-*`` archive files get an additional unwrap (the
    payload may be wrapped as ``{"data": {…}}`` without server
    envelope markers) and their output filename uses ``video-dlp-``
    or ``video-min-`` based on whether the payload carries a
    non-empty ``formats`` list.
    '''

    def test_unwrap_takes_inner_data_when_dict(self) -> None:
        inner: dict = {'video_id': _TEST_VIDEO_ID}
        wrapped: dict = {'data': inner, 'unrelated': 1}
        self.assertIs(_unwrap_unknown_video(wrapped), inner)

    def test_unwrap_passes_through_when_no_data_key(self) -> None:
        plain: dict = {'video_id': _TEST_VIDEO_ID}
        self.assertIs(_unwrap_unknown_video(plain), plain)

    def test_unwrap_passes_through_when_data_not_dict(self) -> None:
        not_a_dict: dict = {'data': 'string-not-dict'}
        self.assertIs(_unwrap_unknown_video(not_a_dict), not_a_dict)

    def test_target_prefix_dlp_when_formats_non_empty(self) -> None:
        payload: dict = {'formats': [{'url': 'https://x'}]}
        self.assertEqual(_video_target_prefix(payload), 'video-dlp-')

    def test_target_prefix_min_when_formats_empty(self) -> None:
        payload: dict = {'formats': []}
        self.assertEqual(_video_target_prefix(payload), 'video-min-')

    def test_target_prefix_min_when_formats_missing(self) -> None:
        payload: dict = {'video_id': _TEST_VIDEO_ID}
        self.assertEqual(_video_target_prefix(payload), 'video-min-')

    def test_target_prefix_min_when_formats_not_list(self) -> None:
        '''Defensive: a stray non-list value in ``formats`` falls
        back to the minimal prefix rather than crashing.'''
        payload: dict = {'formats': 'something'}
        self.assertEqual(_video_target_prefix(payload), 'video-min-')

    def test_dest_filename_dlp_when_formats_present(self) -> None:
        payload: dict = {
            'video_id': _TEST_VIDEO_ID,
            'formats': [{'url': 'https://x'}],
        }
        self.assertEqual(
            _video_dest_filename(payload),
            f'video-dlp-{_TEST_VIDEO_ID}.json.br',
        )

    def test_dest_filename_min_when_no_formats(self) -> None:
        payload: dict = {'video_id': _TEST_VIDEO_ID}
        self.assertEqual(
            _video_dest_filename(payload),
            f'video-min-{_TEST_VIDEO_ID}.json.br',
        )

    def test_dest_filename_uses_payload_id_not_source_name(self) -> None:
        '''
        Output filename derives entirely from payload, regardless
        of what the source filename looked like — so a legacy
        ``video-{channel_id}-{video_id}`` source still produces
        the canonical ``{prefix}{video_id}.json.br`` form.
        '''

        payload: dict = {
            'video_id': _TEST_VIDEO_ID,
            'formats': [{'url': 'https://x'}],
        }
        self.assertEqual(
            _video_dest_filename(payload),
            f'video-dlp-{_TEST_VIDEO_ID}.json.br',
        )

    def test_envelope_keys_do_not_leak_into_output(self) -> None:
        '''
        For a ``video-unknown-`` file with a ``.data`` payload,
        only fields from inside ``.data`` reach the written record.
        Outer envelope keys must be dropped end-to-end.
        '''

        parsed: dict = {
            'envelope_field_a': 'should-not-leak',
            'envelope_field_b': 12345,
            'data': {
                'video_id': _TEST_VIDEO_ID,
                'channel_id': _TEST_CHANNEL_ID,
                'channel_handle': 'XChannel',
                'title': 'Some Title',
                'formats': [{'url': 'https://x'}],
            },
        }
        old: dict = _unwrap_unknown_video(parsed)
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertNotIn('envelope_field_a', new)
        self.assertNotIn('envelope_field_b', new)
        self.assertNotIn('data', new)
        self.assertEqual(new['video_id'], _TEST_VIDEO_ID)
        self.assertEqual(new['title'], 'Some Title')

    def test_dest_filename_downgrades_dlp_source_when_payload_lacks_formats(
        self,
    ) -> None:
        '''
        The destination derives from payload, not source name. So
        a ``video-dlp-`` source whose payload has no formats
        downgrades to ``video-min-`` on output. Trust the payload
        over the source filename.
        '''

        payload: dict = {
            'video_id': _TEST_VIDEO_ID, 'formats': [],
        }
        self.assertEqual(
            _video_dest_filename(payload),
            f'video-min-{_TEST_VIDEO_ID}.json.br',
        )


if __name__ == '__main__':
    unittest.main()
