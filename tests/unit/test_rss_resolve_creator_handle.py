'''The RSS creator-handle resolution must never crash on a missing
handle and must not fabricate a handle from the channel_id placeholder.
'''

import unittest


_CID: str = 'UCaaaaaaaaaaaaaaaaaaaaaa'


class TestResolveCreatorHandle(unittest.TestCase):

    def test_canonical_wins(self) -> None:
        from tools.yt_rss_scrape import _resolve_creator_handle
        self.assertEqual(
            _resolve_creator_handle('canon', 'input', _CID),
            'canon',
        )

    def test_real_input_handle_used_when_no_canonical(self) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            fallback_handle,
        )
        from tools.yt_rss_scrape import _resolve_creator_handle
        self.assertEqual(
            _resolve_creator_handle(None, 'RealHandle', _CID),
            fallback_handle('RealHandle'),
        )

    def test_none_handle_returns_none(self) -> None:
        from tools.yt_rss_scrape import _resolve_creator_handle
        # No canonical, no input handle: nothing to write, no crash.
        self.assertIsNone(
            _resolve_creator_handle(None, None, _CID),
        )

    def test_channel_id_placeholder_returns_none(self) -> None:
        from tools.yt_rss_scrape import _resolve_creator_handle
        # The E1 seed label may be the channel_id itself; that is not a
        # real handle and must not be written to creator_map.
        self.assertIsNone(
            _resolve_creator_handle(None, _CID, _CID),
        )


if __name__ == '__main__':
    unittest.main()
