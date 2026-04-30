'''Tests that the channel scraper uses channel_handle for upload.'''

import unittest
from unittest.mock import AsyncMock

from scrape_exchange.creator_map import NullCreatorMap
from scrape_exchange.name_map import NullNameMap


class TestChannelScraperHandleResolution(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_channel_handle_used_and_mapped(self) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='InputCasing',
        )
        channel.channel_id = 'UC1234567890abcdefghij'

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        nm: NullNameMap = NullNameMap()
        nm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(
            channel, cm, nm,
        )

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'InputCasing',
        )
        # No title set on the channel, so name_map is not written.
        nm.put.assert_not_awaited()

    async def test_no_creator_map_write_without_channel_id(
        self,
    ) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='InputCasing',
        )
        channel.channel_id = None

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        nm: NullNameMap = NullNameMap()
        nm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(
            channel, cm, nm,
        )

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_not_awaited()
        nm.put.assert_not_awaited()


class TestChannelListWriteBack(unittest.IsolatedAsyncioTestCase):
    '''
    The scraper now writes the deduplicated channel list back
    to disk, mirroring what tools/cleanup_channel_list.py does
    so duplicate lines don't survive across runs.
    '''

    async def test_read_splits_header_and_entries(self) -> None:
        '''Header (leading comments/blanks) is captured separately
        from the data entries.'''
        import tempfile
        from pathlib import Path
        from tools.yt_channel_scrape import _read_channel_list_file

        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channels.lst'
            path.write_text(
                '# top comment\n'
                '\n'
                '# another\n'
                'MyChannel\n'
                '\n'
                'OtherChannel\n'
                '# trailing comment between entries dropped\n'
                'mychannel\n',
            )
            header: list[str]
            entries: list[str]
            header, entries = await _read_channel_list_file(
                str(path),
            )
            self.assertEqual(
                header,
                ['# top comment', '', '# another'],
            )
            self.assertEqual(
                entries,
                ['MyChannel', 'OtherChannel', 'mychannel'],
            )

    async def test_persist_creates_backup_and_writes(
        self,
    ) -> None:
        '''Write-back creates .bak first, then atomically replaces
        the original with header + deduped entries.'''
        import tempfile
        from pathlib import Path
        from tools.yt_channel_scrape import (
            _persist_deduped_channel_list,
        )

        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channels.lst'
            path.write_text('orig content\n')

            await _persist_deduped_channel_list(
                str(path),
                ['# header'],
                ['MyChannel', 'OtherChannel'],
            )

            backup: Path = Path(base) / 'channels.lst.bak'
            self.assertTrue(backup.is_file())
            self.assertEqual(
                backup.read_text(),
                'orig content\n',
            )
            self.assertEqual(
                path.read_text(),
                '# header\nMyChannel\nOtherChannel\n',
            )

    async def test_persist_no_header(self) -> None:
        '''A file with no leading comments writes back without
        a leading newline gap.'''
        import tempfile
        from pathlib import Path
        from tools.yt_channel_scrape import (
            _persist_deduped_channel_list,
        )

        with tempfile.TemporaryDirectory() as base:
            path: Path = Path(base) / 'channels.lst'
            path.write_text('foo\nbar\n')
            await _persist_deduped_channel_list(
                str(path), [], ['Foo', 'Bar'],
            )
            self.assertEqual(path.read_text(), 'Foo\nBar\n')
