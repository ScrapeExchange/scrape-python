'''post_/finalize_prepared_bulk_batch forward the BulkUploadConfig.'''

import inspect
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.bulk_upload import BulkBatchOutcome
from scrape_exchange.upload import (
    BulkUploadConfig,
    finalize_prepared_bulk_batch,
    post_prepared_bulk_batch,
)


def _config() -> BulkUploadConfig:
    return BulkUploadConfig(
        schema_owner='owner', schema_version='1.0.0',
        platform='youtube', entity='video',
        exchange_url='https://scrape.exchange',
        progress_timeout_seconds=12.0, filename_prefix='videos',
    )


class TestSplit(unittest.IsolatedAsyncioTestCase):

    async def test_post_forwards_config(self) -> None:
        post: AsyncMock = AsyncMock(return_value=('job', 'batch', None))
        fm: MagicMock = MagicMock()
        with patch('scrape_exchange.upload.post_bulk_batch', post):
            result = await post_prepared_bulk_batch(
                b'{}\n', [('v1', 'video-v1.json.br')], _config(),
                client='client', fm=fm,
            )

        self.assertEqual(result, ('job', 'batch', None))
        kwargs: dict = post.await_args.kwargs
        self.assertEqual(kwargs['platform'], 'youtube')
        self.assertEqual(kwargs['filename_prefix'], 'videos')
        self.assertEqual(kwargs['exchange_url'], 'https://scrape.exchange')
        self.assertIs(kwargs['fm'], fm)

    async def test_finalize_forwards_config_and_kwargs(self) -> None:
        outcome: BulkBatchOutcome = BulkBatchOutcome(
            status='completed', job_id='job', success=1, failed=0,
            missing=0,
        )
        finalize: AsyncMock = AsyncMock(return_value=outcome)
        id_fn = lambda name: name  # noqa: E731
        with patch(
            'scrape_exchange.upload.finalize_bulk_batch', finalize,
        ):
            result = await finalize_prepared_bulk_batch(
                'job', 'batch', None, [('v1', 'video-v1.json.br')],
                _config(), client='client', fm=MagicMock(),
                id_from_filename=id_fn,
            )

        self.assertIs(result, outcome)
        kwargs: dict = finalize.await_args.kwargs
        self.assertEqual(kwargs['progress_timeout_seconds'], 12.0)
        self.assertIs(kwargs['id_from_filename'], id_fn)
        self.assertIsNone(kwargs['exchange_set'])

    async def test_finalize_returns_err_unchanged(self) -> None:
        '''When err is not None, return it without calling
        finalize_bulk_batch.'''
        err_outcome: BulkBatchOutcome = BulkBatchOutcome(
            status='failed', job_id='job', success=0, failed=1,
            missing=0,
        )
        finalize: AsyncMock = AsyncMock()
        with patch(
            'scrape_exchange.upload.finalize_bulk_batch', finalize,
        ):
            result = await finalize_prepared_bulk_batch(
                'job', 'batch', err_outcome, [('v1', 'video-v1.json.br')],
                _config(), client='client', fm=MagicMock(),
            )

        self.assertIs(result, err_outcome)
        finalize.assert_not_called()

    def test_finalize_signature_for_yt_video_upload(self) -> None:
        '''Regression: yt_video_upload.py line 900 calls with positional
        args (job_id, batch_id, err, batch_records, config, ...).'''
        sig: inspect.Signature = inspect.signature(
            finalize_prepared_bulk_batch
        )
        params: list[str] = list(sig.parameters.keys())
        expected: list[str] = [
            'job_id', 'batch_id', 'err', 'batch_records', 'config',
            'client', 'fm',
        ]
        self.assertEqual(params[:7], expected)


if __name__ == '__main__':
    unittest.main()
