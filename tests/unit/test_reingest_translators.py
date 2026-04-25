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
            'channel_id': 'UCabc',
            'channel': 'XChannel',
            'video_ids': ['v1', 'v2', 'v3'],
            'subscriber_count': 1000,
            'description': 'A channel.',
        }
        new: dict | None = translate_channel(old)
        assert new is not None
        self.assertEqual(new['channel_id'], 'UCabc')
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
            'video_id': 'abc123',
            'title': 'A Video',
            'channel_id': 'UCabc',
            'channel_name': 'HistoryMatters',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'HistoryMatters')
        self.assertNotIn('channel_name', new)
        self.assertEqual(new['title'], 'A Video')
        self.assertEqual(new['video_id'], 'abc123')

    def test_passes_through_when_already_renamed(self) -> None:
        old: dict = {
            'video_id': 'abc',
            'channel_id': 'UCabc',
            'channel_handle': 'X',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['channel_handle'], 'X')

    def test_returns_none_when_channel_handle_missing(self) -> None:
        old: dict = {
            'video_id': 'abc', 'title': 'T',
            'channel_id': 'UCabc',
        }
        self.assertIsNone(translate_video(old))

    def test_returns_none_when_channel_id_missing(self) -> None:
        old: dict = {
            'video_id': 'abc', 'title': 'T',
            'channel_handle': 'HistoryMatters',
        }
        self.assertIsNone(translate_video(old))

    def test_returns_none_when_channel_id_empty(self) -> None:
        old: dict = {
            'video_id': 'abc',
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
            'video_id': 'v',
            'channel_id': 'UCabc',
            'channel_handle': 'X',
            'categories': ['Music'],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')
        self.assertNotIn('categories', new)

    def test_empty_categories_list_yields_none(self) -> None:
        old: dict = {
            'video_id': 'v',
            'channel_id': 'UCabc',
            'channel_handle': 'X',
            'categories': [],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertIsNone(new['category'])
        self.assertNotIn('categories', new)

    def test_missing_categories_leaves_record_alone(self) -> None:
        old: dict = {
            'video_id': 'v', 'title': 'T',
            'channel_id': 'UCabc',
            'channel_handle': 'X',
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertNotIn('category', new)
        self.assertNotIn('categories', new)

    def test_multiple_categories_takes_first(self) -> None:
        old: dict = {
            'video_id': 'v',
            'channel_id': 'UCabc',
            'channel_handle': 'X',
            'categories': ['Music', 'Entertainment'],
        }
        new: dict | None = translate_video(old)
        assert new is not None
        self.assertEqual(new['category'], 'Music')


if __name__ == '__main__':
    unittest.main()
