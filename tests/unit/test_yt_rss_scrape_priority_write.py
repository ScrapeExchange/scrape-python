'''Tests for the producer-side priority write helper.'''

import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

import brotli
import orjson


def _load_yt_rss_scrape() -> ModuleType:
    '''Load tools/yt_rss_scrape.py under the bare module
    name ``yt_rss_scrape``. Checks sys.modules first so
    that repeated test runs within the same process do not
    trigger prometheus duplicate-registry errors.'''
    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']
    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_rss_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


class TestWriteChannelPriority(
    unittest.IsolatedAsyncioTestCase,
):
    '''_write_channel_priority writes a brotli-compressed
    JSON file under the resolved priority directory with the
    canonical channel-rss-<handle>.json.br filename.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.priority: str = os.path.join(
            self.tmp, 'priority',
        )
        os.makedirs(self.priority, exist_ok=True)

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _settings(self) -> mock.MagicMock:
        s: mock.MagicMock = mock.MagicMock()
        s.channel_priority_directory_path = self.priority
        return s

    async def test_writes_correct_filename(self) -> None:
        record: dict = {
            'channel_handle': 'somehandle',
            'channel_id': 'UC_xyz',
            'url': (
                'https://www.youtube.com/channel/UC_xyz'
            ),
            'title': 'Some Channel',
            'subscriber_count': 100,
            'video_count': 5,
            'view_count': 12345,
            'description': '',
        }
        await yt_rss_scrape._write_channel_priority(
            record, self._settings(),
        )
        path: Path = (
            Path(self.priority)
            / 'channel-rss-somehandle.json.br'
        )
        self.assertTrue(path.exists(), msg=str(path))

    async def test_content_is_brotli_json(self) -> None:
        record: dict = {
            'channel_handle': 'abc',
            'channel_id': 'UC_abc',
            'url': (
                'https://www.youtube.com/channel/UC_abc'
            ),
            'title': 't',
            'subscriber_count': 0,
            'video_count': 0,
            'view_count': 0,
            'description': '',
        }
        await yt_rss_scrape._write_channel_priority(
            record, self._settings(),
        )
        path: Path = (
            Path(self.priority)
            / 'channel-rss-abc.json.br'
        )
        raw: bytes = path.read_bytes()
        decoded: dict = orjson.loads(
            brotli.decompress(raw),
        )
        self.assertEqual(decoded, record)

    async def test_os_error_propagates(self) -> None:
        '''If the underlying write fails (e.g. directory
        deleted between mkdir-check and write), the OSError
        propagates so the supervisor respawns the worker.'''
        s: mock.MagicMock = self._settings()
        s.channel_priority_directory_path = (
            '/nonexistent/path/that/cannot/be/written'
        )
        record: dict = {
            'channel_handle': 'x',
            'channel_id': 'UC_x',
            'url': 'u',
            'title': 't',
            'subscriber_count': 0,
            'video_count': 0,
            'view_count': 0,
            'description': '',
        }
        with self.assertRaises(OSError):
            await yt_rss_scrape._write_channel_priority(
                record, s,
            )


class TestEnsurePriorityDirectory(
    unittest.IsolatedAsyncioTestCase,
):
    '''_ensure_priority_directory creates the directory
    and verifies a temp write+unlink succeeds. Raises on
    unwritable paths.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.module: ModuleType = _load_yt_rss_scrape()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _settings(self, priority_path: str) -> mock.MagicMock:
        s: mock.MagicMock = mock.MagicMock()
        s.channel_priority_directory_path = priority_path
        return s

    async def test_creates_missing_directory(self) -> None:
        target: str = os.path.join(self.tmp, 'fresh')
        self.assertFalse(os.path.isdir(target))
        await self.module._ensure_priority_directory(
            self._settings(target),
        )
        self.assertTrue(os.path.isdir(target))

    async def test_succeeds_on_existing_writable_dir(
        self,
    ) -> None:
        # Should not raise.
        await self.module._ensure_priority_directory(
            self._settings(self.tmp),
        )

    async def test_raises_on_unwritable_path(self) -> None:
        # Make a directory read-only.
        ro: str = os.path.join(self.tmp, 'readonly')
        os.makedirs(ro, mode=0o500, exist_ok=True)
        try:
            with self.assertRaises(OSError):
                await self.module._ensure_priority_directory(
                    self._settings(ro),
                )
        finally:
            os.chmod(ro, 0o700)  # let teardown remove it


class TestUpdateChannelWritesPriority(
    unittest.IsolatedAsyncioTestCase,
):
    '''After the refactor, update_channel must call
    _write_channel_priority instead of
    client.enqueue_upload for the channel record. The
    METRIC_CHANNEL_PRIORITY_WRITES counter must
    increment on success.'''

    async def asyncSetUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()
        self.module: ModuleType = _load_yt_rss_scrape()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_update_channel_writes_to_priority(
        self,
    ) -> None:
        '''Verify _write_channel_priority is invoked
        when update_channel's validator passes. The
        record_dict argument carries the expected lite
        fields.'''
        settings: mock.MagicMock = mock.MagicMock()
        settings.channel_priority_directory_path = self.tmp
        client: mock.MagicMock = mock.MagicMock()
        client.enqueue_upload = mock.MagicMock()
        creator_map: mock.AsyncMock = mock.AsyncMock()
        name_map: mock.AsyncMock = mock.AsyncMock()
        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = None

        browse_result: dict = {
            'metadata': {
                'channelMetadataRenderer': {
                    'title': 'TestTitle',
                    'description': 'TestDesc',
                },
            },
        }
        with mock.patch.object(
            self.module, 'YouTubeChannelTabs',
        ) as tabs_cls, mock.patch.object(
            self.module.YouTubeChannel,
            'parse_subscriber_count',
            return_value=42,
        ), mock.patch.object(
            self.module.YouTubeChannel,
            'parse_view_count',
            return_value=100,
        ), mock.patch.object(
            self.module.YouTubeChannel,
            'parse_video_count',
            return_value=7,
        ), mock.patch.object(
            self.module,
            'canonical_handle_from_browse',
            return_value='canonhandle',
        ):
            tabs_cls.return_value.browse_channel = (
                mock.AsyncMock(return_value=browse_result)
            )
            ok, subs, resolved = (
                await self.module.update_channel(
                    client=client,
                    channel_handle='inputhandle',
                    channel_id='UC_xyz',
                    creator_map_backend=creator_map,
                    name_map_backend=name_map,
                    validator=validator,
                    proxy=None,
                    settings=settings,
                )
            )
        self.assertTrue(ok)
        self.assertEqual(subs, 42)
        self.assertEqual(resolved, 'canonhandle')
        path: Path = (
            Path(self.tmp)
            / 'channel-rss-canonhandle.json.br'
        )
        self.assertTrue(path.exists())
        client.enqueue_upload.assert_not_called()

    async def test_view_count_omitted_when_unparseable(
        self,
    ) -> None:
        '''YouTube's InnerTube browse response no longer
        contains a "views" metadataRow — that field moved to
        the About tab which the RSS scraper does not fetch.
        ``parse_view_count`` returns ``None``; the priority
        record must omit ``view_count`` entirely rather than
        write ``0`` (which is indistinguishable from a
        legitimately zero-view channel and would clobber the
        last-known value on the server).'''
        settings: mock.MagicMock = mock.MagicMock()
        settings.channel_priority_directory_path = self.tmp
        client: mock.MagicMock = mock.MagicMock()
        creator_map: mock.AsyncMock = mock.AsyncMock()
        name_map: mock.AsyncMock = mock.AsyncMock()
        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = None

        browse_result: dict = {
            'metadata': {
                'channelMetadataRenderer': {
                    'title': 'NoViews',
                    'description': '',
                },
            },
        }
        with mock.patch.object(
            self.module, 'YouTubeChannelTabs',
        ) as tabs_cls, mock.patch.object(
            self.module.YouTubeChannel,
            'parse_subscriber_count',
            return_value=42,
        ), mock.patch.object(
            self.module.YouTubeChannel,
            'parse_view_count',
            return_value=None,
        ), mock.patch.object(
            self.module.YouTubeChannel,
            'parse_video_count',
            return_value=7,
        ), mock.patch.object(
            self.module,
            'canonical_handle_from_browse',
            return_value='noviews',
        ):
            tabs_cls.return_value.browse_channel = (
                mock.AsyncMock(return_value=browse_result)
            )
            ok, _subs, _resolved = (
                await self.module.update_channel(
                    client=client,
                    channel_handle='noviews',
                    channel_id='UC_nv',
                    creator_map_backend=creator_map,
                    name_map_backend=name_map,
                    validator=validator,
                    proxy=None,
                    settings=settings,
                )
            )
        self.assertTrue(ok)
        record_dict: dict = (
            validator.validate.call_args[0][0]
        )
        self.assertNotIn('view_count', record_dict)
        # subscriber_count / video_count parsed fine so they
        # must remain present.
        self.assertEqual(record_dict['subscriber_count'], 42)
        self.assertEqual(record_dict['video_count'], 7)
        path: Path = (
            Path(self.tmp)
            / 'channel-rss-noviews.json.br'
        )
        self.assertTrue(path.exists())
        decoded: dict = orjson.loads(
            brotli.decompress(path.read_bytes()),
        )
        self.assertNotIn('view_count', decoded)


if __name__ == '__main__':
    unittest.main()
