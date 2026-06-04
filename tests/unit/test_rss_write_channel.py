'''Unit tests for RSS channel file naming.'''

import unittest
from pathlib import Path
from unittest import mock

from tools import yt_rss_scrape as mod


class TestRssWriteChannel(unittest.IsolatedAsyncioTestCase):

    async def test_writes_channel_rss_with_channel_id(self) -> None:
        settings = mock.Mock()
        settings.channel_data_directory = '/tmp/scrape-test'
        record = {
            'channel_id': 'UCabcdefghijklmnopqrstuv',
            'channel_handle': 'somehandle',
        }
        with mock.patch.object(
            mod, 'brotli_write_async', new=mock.AsyncMock(),
        ) as writer:
            await mod._write_channel(record, settings)
        target: Path = writer.await_args.args[0]
        self.assertEqual(
            target.name,
            'channel-rss-UCabcdefghijklmnopqrstuv.json.br',
        )

    async def test_skips_write_when_no_channel_id(self) -> None:
        settings = mock.Mock()
        settings.channel_data_directory = '/tmp/scrape-test'
        record = {'channel_id': '', 'channel_handle': 'somehandle'}
        with mock.patch.object(
            mod, 'brotli_write_async', new=mock.AsyncMock(),
        ) as writer, mock.patch.object(
            mod, 'METRIC_RSS_FAILURES',
        ) as metric:
            await mod._write_channel(record, settings)
        writer.assert_not_awaited()
        metric.labels.assert_called_once()
        self.assertEqual(
            metric.labels.call_args.kwargs['reason'],
            'no_channel_id',
        )


if __name__ == '__main__':
    unittest.main()
