'''The periodically re-scraping tools (yt_channel_scrape, tw_creator_scrape,
yt_rss_scrape) must not let files in ``uploaded/`` influence what they
scrape: an earlier upload is no reason to skip a re-scrape.'''

import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileIdentityError,
)
from tools import tw_creator_scrape, yt_rss_scrape
from tools import yt_channel_scrape as mod

CHANNEL_ID: str = 'UCabcdefghijklmnopqrstuv'


def _channel_filename() -> str:
    return mod.get_channel_filename(CHANNEL_ID)


class TestChannelScraperIgnoresUploadedDir(
    unittest.IsolatedAsyncioTestCase,
):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(self._tmp.name)
        self.filename: str = _channel_filename()
        self.base_path: Path = self.fm.base_dir / self.filename

    async def _write_uploaded_copy(self) -> None:
        await self.fm.write_file(self.filename, {'channel_id': CHANNEL_ID})
        await self.fm.mark_uploaded(self.filename)
        self.assertTrue(self.fm.was_uploaded(self.filename))

    async def test_uploaded_copy_does_not_skip_the_scrape(self) -> None:
        await self._write_uploaded_copy()
        skipped: bool = await mod._skip_due_to_existing_state(
            self.fm, self.filename, self.base_path, {},
        )
        self.assertFalse(skipped)

    async def test_failed_marker_is_not_cleared_by_uploaded_copy(
        self,
    ) -> None:
        await self._write_uploaded_copy()
        failed_path: Path = self.fm.marker_path(self.filename, '.failed')
        failed_path.write_text('')
        skipped: bool = await mod._skip_due_to_existing_state(
            self.fm, self.filename, self.base_path, {},
        )
        self.assertTrue(skipped)
        self.assertTrue(failed_path.exists())

    async def test_newer_base_file_still_clears_failed_marker(self) -> None:
        failed_path: Path = self.fm.marker_path(self.filename, '.failed')
        failed_path.write_text('')
        os.utime(failed_path, (1_000_000, 1_000_000))
        await self.fm.write_file(self.filename, {'channel_id': CHANNEL_ID})
        skipped: bool = await mod._skip_due_to_existing_state(
            self.fm, self.filename, self.base_path, {},
        )
        self.assertFalse(skipped)
        self.assertFalse(failed_path.exists())

    async def test_uploaded_copy_excludes_a_candidate(self) -> None:
        # Discovery: a channel with a local file, uploaded or not, is
        # already known and not new.
        await self._write_uploaded_copy()

        async def resolver(identifier, identity_store, fm):
            return CHANNEL_ID

        creator_map = mock.AsyncMock()
        creator_map.get.return_value = None
        with mock.patch.object(
            mod, '_resolve_handle_to_channel_id', new=resolver,
        ):
            candidates: list[str] = await mod._filter_unscraped_candidates(
                ['@someone'], self.fm, set(), mock.Mock(), creator_map,
                max_candidates=5,
            )
        self.assertEqual(candidates, [])

    async def test_stale_file_of_listed_channel_is_deleted(self) -> None:
        await self.fm.write_file(self.filename, {'channel_id': CHANNEL_ID})

        async def resolver(identifier, identity_store, fm):
            return CHANNEL_ID

        creator_map = mock.AsyncMock()
        creator_map.get.return_value = '@listed'
        with mock.patch.object(
            mod, '_resolve_handle_to_channel_id', new=resolver,
        ):
            candidates: list[str] = await mod._filter_unscraped_candidates(
                ['@listed'], self.fm, {'@listed'}, mock.Mock(),
                creator_map, max_candidates=5,
            )
        self.assertEqual(candidates, [])
        self.assertFalse(self.base_path.exists())
        # Deleted, not moved into uploaded/.
        self.assertFalse(self.fm.was_uploaded(self.filename))


class TestTwitchScraperIgnoresUploadedDir(unittest.IsolatedAsyncioTestCase):

    def _creator(self, user_id: str) -> TwitchCreator:
        return TwitchCreator(
            username='someone',
            url='https://www.twitch.tv/someone',
            scraped_timestamp=datetime.now(UTC),
            user_id=user_id,
        )

    async def test_uploaded_copy_is_not_used_for_identity_check(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fm: AssetFileManagement = AssetFileManagement(directory)
            filename: str = f'{tw_creator_scrape.PREFIX}someone.json.br'
            await fm.write_file(filename, self._creator('111').to_dict())
            await fm.mark_uploaded(filename)

            # The uploaded copy carries another user id; it is ignored.
            await tw_creator_scrape.save_creator(self._creator('222'), fm)

            self.assertTrue((fm.base_dir / filename).exists())

    async def test_local_copy_still_guards_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fm: AssetFileManagement = AssetFileManagement(directory)
            filename: str = f'{tw_creator_scrape.PREFIX}someone.json.br'
            await fm.write_file(filename, self._creator('111').to_dict())

            with self.assertRaises(ProfileIdentityError):
                await tw_creator_scrape.save_creator(
                    self._creator('222'), fm,
                )


class TestRssScraperDoesNotSeedFromUploadedDir(unittest.TestCase):

    def test_seed_from_uploaded_is_gone(self) -> None:
        self.assertFalse(
            hasattr(yt_rss_scrape, '_seed_queue_from_uploaded_channels')
        )
        self.assertNotIn(
            'rss_seed_queue_from_uploaded',
            yt_rss_scrape.RssSettings.model_fields,
        )


if __name__ == '__main__':
    unittest.main()
