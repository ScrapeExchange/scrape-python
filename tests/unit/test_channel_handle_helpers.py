'''Unit tests for canonical_handle_from_browse and fallback_handle.'''

import unittest

from scrape_exchange.youtube.youtube_channel import (
    canonical_handle_from_browse,
    fallback_handle,
)


class TestFallbackHandle(unittest.TestCase):
    def test_strips_at_sign(self) -> None:
        self.assertEqual(fallback_handle('@History'), 'history')

    def test_strips_whitespace(self) -> None:
        self.assertEqual(fallback_handle('  History  '), 'history')

    def test_lowercases(self) -> None:
        self.assertEqual(fallback_handle('HISTORY'), 'history')

    def test_preserves_spaces_inside(self) -> None:
        self.assertEqual(
            fallback_handle('History Matters'),
            'history matters',
        )

    def test_combined(self) -> None:
        self.assertEqual(
            fallback_handle('  @History_MATTERS  '),
            'history_matters',
        )

    def test_empty_raises(self) -> None:
        with self.assertRaises(ValueError):
            fallback_handle('')

    def test_only_at_sign_raises(self) -> None:
        with self.assertRaises(ValueError):
            fallback_handle('@')

    def test_replaces_forward_slash(self) -> None:
        '''``/`` is the POSIX path separator and turns a
        handle into a sub-path when used as a filename. Replace
        with ``_`` so the value still round-trips through Path
        and identifies the channel.'''
        self.assertEqual(
            fallback_handle('FAIR/PLAY'), 'fair_play',
        )

    def test_replaces_multiple_slashes(self) -> None:
        self.assertEqual(
            fallback_handle('A/B/C'), 'a_b_c',
        )

    def test_strips_nul_bytes(self) -> None:
        '''POSIX file APIs reject embedded NULs outright.'''
        self.assertEqual(
            fallback_handle('foo\x00bar'), 'foobar',
        )
        self.assertEqual(
            fallback_handle('\x00foo'), 'foo',
        )

    def test_strips_leading_dots(self) -> None:
        '''Leading ``.`` turns the file into a Unix hidden
        file — easy to miss in directory listings and globs.'''
        self.assertEqual(
            fallback_handle('.history'), 'history',
        )
        self.assertEqual(
            fallback_handle('..history'), 'history',
        )

    def test_strips_leading_dot_after_at(self) -> None:
        '''``@`` strip happens first, then ``.`` so ``@.foo``
        and ``.@foo`` both collapse to ``foo``.'''
        self.assertEqual(
            fallback_handle('@.foo'), 'foo',
        )

    def test_only_dots_raises(self) -> None:
        with self.assertRaises(ValueError):
            fallback_handle('...')

    def test_slash_only_raises(self) -> None:
        '''``/`` becomes ``_`` but the result is still
        meaningless — must not crash, but ``'_'`` is a valid
        non-empty result, so this returns it rather than
        raising. Documenting current behavior.'''
        self.assertEqual(fallback_handle('/'), '_')


class TestCanonicalHandleFromBrowse(unittest.TestCase):
    def _browse_data(self, vanity_url: str | None) -> dict:
        meta: dict[str, str] = {}
        if vanity_url is not None:
            meta['vanityChannelUrl'] = vanity_url
        return {
            'metadata': {
                'channelMetadataRenderer': meta,
            },
        }

    def test_normal_handle(self) -> None:
        data: dict = self._browse_data(
            'http://www.youtube.com/@HistoryMatters',
        )
        self.assertEqual(
            canonical_handle_from_browse(data), 'HistoryMatters',
        )

    def test_handle_with_trailing_slash(self) -> None:
        data: dict = self._browse_data(
            'http://www.youtube.com/@HistoryMatters/',
        )
        self.assertEqual(
            canonical_handle_from_browse(data), 'HistoryMatters',
        )

    def test_handle_with_query_string(self) -> None:
        data: dict = self._browse_data(
            'http://www.youtube.com/@HistoryMatters?foo=bar',
        )
        self.assertEqual(
            canonical_handle_from_browse(data), 'HistoryMatters',
        )

    def test_vanity_url_missing_returns_none(self) -> None:
        data: dict = self._browse_data(None)
        self.assertIsNone(canonical_handle_from_browse(data))

    def test_vanity_url_without_at_returns_none(self) -> None:
        data: dict = self._browse_data(
            'http://www.youtube.com/channel/UC123',
        )
        self.assertIsNone(canonical_handle_from_browse(data))

    def test_metadata_key_missing_returns_none(self) -> None:
        self.assertIsNone(canonical_handle_from_browse({}))

    def test_channel_metadata_renderer_missing_returns_none(
        self,
    ) -> None:
        self.assertIsNone(
            canonical_handle_from_browse({'metadata': {}}),
        )
