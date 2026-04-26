'''
Unit tests for the field-mapping translators in
tools/reingest_from_archive.py. The translator functions encode the
decisions made when collapsing the legacy ``channel`` / ``title`` /
``canonical_handle`` / ``channel_name`` fields into the new
``channel_handle`` / ``title`` vocabulary.
'''

import unittest

from tools.reingest_from_archive import (
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

    def test_returns_none_when_handle_is_junk(self) -> None:
        '''
        Per project policy, ``_HANDLE_PATTERN`` applies to video
        records too: a channel_handle that contains whitespace or
        other URL-incompatible junk is rejected.
        '''
        old: dict = {
            'video_id': _TEST_VIDEO_ID,
            'channel_id': _TEST_CHANNEL_ID,
            'channel_handle': 'has space',
        }
        self.assertIsNone(translate_video(old))

    def test_returns_none_when_channel_handle_missing(self) -> None:
        old: dict = {
            'video_id': _TEST_VIDEO_ID, 'title': 'T',
            'channel_id': _TEST_CHANNEL_ID,
        }
        self.assertIsNone(translate_video(old))

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
        old: dict = {'video_id': _TEST_VIDEO_ID, 'channel_id': _TEST_CHANNEL_ID}
        self.assertIsNone(translate_video(old))
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

    def test_drops_when_mapped_handle_invalid(self) -> None:
        old: dict = {'video_id': _TEST_VIDEO_ID, 'channel_id': _TEST_CHANNEL_ID}
        id_to_handle: dict[str, str] = {
            _TEST_CHANNEL_ID: 'has space',
        }
        self.assertIsNone(
            translate_video(old, id_to_handle, {}),
        )


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


if __name__ == '__main__':
    unittest.main()
