'''Tests for the consumer-side priority sweep helpers.'''

import asyncio
import contextlib
import importlib.util
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


def _load_yt_channel_scrape() -> ModuleType:
    '''Load tools/yt_channel_scrape.py under the bare
    module name ``yt_channel_scrape``. Checks sys.modules
    first so that repeated test runs within the same
    process do not trigger prometheus duplicate-registry
    errors.'''
    if 'yt_channel_scrape' in sys.modules:
        return sys.modules['yt_channel_scrape']
    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_channel_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_channel_scrape', module_path,
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_channel_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_channel_scrape: ModuleType = _load_yt_channel_scrape()


class TestPriorityChannelWorkItems(unittest.TestCase):
    '''_priority_channel_work_items enumerates
    channel-rss-*.json.br files in mtime ascending order
    and skips *.json.br.failed.'''

    def setUp(self) -> None:
        self.tmp: str = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name: str, age_secs: float) -> str:
        path: str = os.path.join(self.tmp, name)
        Path(path).write_bytes(b'x')
        now: float = time.time()
        os.utime(path, (now - age_secs, now - age_secs))
        return path

    def test_returns_empty_for_empty_directory(
        self,
    ) -> None:
        result: list[Path] = (
            yt_channel_scrape._priority_channel_work_items(
                self.tmp,
            )
        )
        self.assertEqual(result, [])

    def test_filters_by_prefix_and_suffix(self) -> None:
        self._write('channel-rss-a.json.br', age_secs=1)
        self._write('channel-rss-b.json.br', age_secs=1)
        self._write(
            'channel-rss-c.json.br.failed', age_secs=1,
        )
        self._write('channel-d.json.br', age_secs=1)
        self._write('other.txt', age_secs=1)
        result: list[Path] = (
            yt_channel_scrape._priority_channel_work_items(
                self.tmp,
            )
        )
        names: set[str] = {p.name for p in result}
        self.assertEqual(
            names,
            {
                'channel-rss-a.json.br',
                'channel-rss-b.json.br',
            },
        )

    def test_orders_by_mtime_ascending(self) -> None:
        '''Oldest first so the queue drains FIFO under
        load.'''
        self._write(
            'channel-rss-young.json.br', age_secs=1,
        )
        self._write(
            'channel-rss-old.json.br', age_secs=60,
        )
        result: list[Path] = (
            yt_channel_scrape._priority_channel_work_items(
                self.tmp,
            )
        )
        self.assertEqual(
            [p.name for p in result],
            [
                'channel-rss-old.json.br',
                'channel-rss-young.json.br',
            ],
        )

    def test_missing_directory_returns_empty(
        self,
    ) -> None:
        result: list[Path] = (
            yt_channel_scrape._priority_channel_work_items(
                os.path.join(self.tmp, 'doesnotexist'),
            )
        )
        self.assertEqual(result, [])


class TestProcessPriorityFile(
    unittest.IsolatedAsyncioTestCase,
):
    '''_process_priority_file POSTs, then either moves
    on 201, leaves on retryable failure, or renames in
    place to .json.br.failed on permanent failure.'''

    async def asyncSetUp(self) -> None:
        import brotli
        import orjson
        self.tmp: str = tempfile.mkdtemp()
        self.priority: str = (
            os.path.join(self.tmp, 'priority')
        )
        self.uploaded: str = (
            os.path.join(self.tmp, 'uploaded')
        )
        os.makedirs(self.priority, exist_ok=True)
        os.makedirs(self.uploaded, exist_ok=True)
        self.module: ModuleType = _load_yt_channel_scrape()

        self.record: dict = {
            'channel_handle': 'foo',
            'channel_id': 'UC_foo',
            'url': 'https://www.youtube.com/channel/UC_foo',
            'title': 'Foo',
            'subscriber_count': 1,
            'video_count': 1,
            'view_count': 1,
            'description': '',
        }
        self.file: Path = (
            Path(self.priority)
            / 'channel-rss-foo.json.br'
        )
        self.file.write_bytes(brotli.compress(
            orjson.dumps(self.record),
            quality=11, mode=brotli.MODE_TEXT,
        ))

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _client_returning(
        self, status_code: int,
    ) -> mock.MagicMock:
        response: mock.MagicMock = mock.MagicMock()
        response.status_code = status_code
        client: mock.MagicMock = mock.MagicMock()
        client.post = mock.AsyncMock(return_value=response)
        client.exchange_url = 'https://exchange.example'
        return client

    def _passing_validator(self) -> mock.MagicMock:
        '''Return a validator mock whose validate() returns
        None (i.e. no validation error).'''
        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = None
        return validator

    async def test_success_moves_to_uploaded(self) -> None:
        client: mock.MagicMock = self._client_returning(201)
        retries: dict[str, int] = {}
        await self.module._process_priority_file(
            self.file, client,
            self._passing_validator(),
            self.uploaded, retries,
        )
        target: Path = (
            Path(self.uploaded)
            / 'channel-rss-foo.json.br'
        )
        self.assertTrue(target.exists())
        self.assertFalse(self.file.exists())

    async def test_retryable_5xx_leaves_file_increments(
        self,
    ) -> None:
        client: mock.MagicMock = self._client_returning(503)
        retries: dict[str, int] = {}
        await self.module._process_priority_file(
            self.file, client,
            self._passing_validator(),
            self.uploaded, retries,
        )
        self.assertTrue(self.file.exists())
        self.assertEqual(retries.get('foo'), 1)

    async def test_fifth_retry_renames_in_place(
        self,
    ) -> None:
        client: mock.MagicMock = self._client_returning(503)
        retries: dict[str, int] = {'foo': 4}
        await self.module._process_priority_file(
            self.file, client,
            self._passing_validator(),
            self.uploaded, retries,
        )
        failed: Path = (
            Path(self.priority)
            / 'channel-rss-foo.json.br.failed'
        )
        self.assertTrue(failed.exists())
        self.assertFalse(self.file.exists())
        self.assertEqual(retries.get('foo'), 0)

    async def test_non_retryable_4xx_renames_immediately(
        self,
    ) -> None:
        client: mock.MagicMock = self._client_returning(400)
        retries: dict[str, int] = {}
        await self.module._process_priority_file(
            self.file, client,
            self._passing_validator(),
            self.uploaded, retries,
        )
        failed: Path = (
            Path(self.priority)
            / 'channel-rss-foo.json.br.failed'
        )
        self.assertTrue(failed.exists())

    async def test_validation_failure_renames_immediately(
        self,
    ) -> None:
        client: mock.MagicMock = self._client_returning(201)
        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = (
            'missing field "title"'
        )
        retries: dict[str, int] = {}
        await self.module._process_priority_file(
            self.file, client,
            validator,
            self.uploaded, retries,
        )
        failed: Path = (
            Path(self.priority)
            / 'channel-rss-foo.json.br.failed'
        )
        self.assertTrue(failed.exists())
        # Validation fails before POST.
        client.post.assert_not_called()


class TestPrioritySweepLoop(
    unittest.IsolatedAsyncioTestCase,
):
    '''_priority_sweep_loop drains the directory in mtime
    order and sleeps between iterations. Samples
    METRIC_CHANNEL_PRIORITY_QUEUE_AGE per pass.'''

    async def asyncSetUp(self) -> None:
        import brotli, orjson
        self.tmp: str = tempfile.mkdtemp()
        self.priority: str = (
            os.path.join(self.tmp, 'priority')
        )
        self.uploaded: str = (
            os.path.join(self.tmp, 'uploaded')
        )
        os.makedirs(self.priority, exist_ok=True)
        os.makedirs(self.uploaded, exist_ok=True)
        self.module: ModuleType = _load_yt_channel_scrape()

        record: dict = {
            'channel_handle': 'foo',
            'channel_id': 'UC_foo', 'url': 'u',
            'title': 't', 'subscriber_count': 1,
            'video_count': 1, 'view_count': 1,
            'description': '',
        }
        Path(
            self.priority, 'channel-rss-foo.json.br',
        ).write_bytes(brotli.compress(
            orjson.dumps(record),
            quality=11, mode=brotli.MODE_TEXT,
        ))

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_one_pass_drains_file(self) -> None:
        client: mock.MagicMock = mock.MagicMock()
        response: mock.MagicMock = mock.MagicMock()
        response.status_code = 201
        client.post = mock.AsyncMock(return_value=response)
        client.exchange_url = 'https://exchange.example'

        validator: mock.MagicMock = mock.MagicMock()
        validator.validate.return_value = None

        task = asyncio.create_task(
            self.module._priority_sweep_loop(
                priority_dir=self.priority,
                uploaded_dir=self.uploaded,
                client=client,
                validator=validator,
                interval_seconds=0.01,
            ),
        )
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        target: Path = (
            Path(self.uploaded)
            / 'channel-rss-foo.json.br'
        )
        self.assertTrue(target.exists())


if __name__ == '__main__':
    unittest.main()
