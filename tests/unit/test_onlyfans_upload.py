'''OnlyFans records participate in the generic uploader pipelines.'''

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    BulkResults,
    apply_bulk_results,
)
from scrape_exchange.onlyfans.onlyfans_creator import extract_profile
from tests.unit.test_onlyfans_creator import public_profile
from tools import scrape_upload as uploader


class TestOnlyFansUpload(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory[str] = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        with patch.dict('os.environ', {}, clear=True):
            self.settings: uploader.ScrapeUploadSettings = (
                uploader.ScrapeUploadSettings(
                    _env_file=None, onlyfans_creator_data_directory=(
                        self.directory.name
                    ),
                    redis_dsn=None, proxy_files=None, proxies_env=None,
                )
            )
        self.record: dict[str, Any] = extract_profile(
            public_profile(favoritedCount=0, subscribersCount=None), 'example',
        ).to_dict()
        self.schema: dict[str, Any] = json.loads(Path(
            'tests/collateral/drand-onlyfans-creator-schema.json',
        ).read_text())
        self.filename: str = 'onlyfans-creator-example.json.br'
        self.client: MagicMock = MagicMock()

    async def target(self) -> uploader.AssetUploadTarget:
        with patch.object(uploader, 'fetch_schema_dict', AsyncMock(
            return_value=self.schema,
        )) as fetch:
            targets: list[uploader.AssetUploadTarget] = (
                await uploader.build_upload_targets(self.settings, self.client)
            )
        self.assertEqual(len(targets), 1)
        self.assertEqual(fetch.await_args.args[2:], (
            'drand', 'onlyfans', 'creator', '0.0.1',
        ))
        return targets[0]

    async def test_directory_env_and_filename_detection(self) -> None:
        with patch.dict('os.environ', {
            'ONLYFANS_CREATOR_DATA_DIR': '/data/onlyfans/a,/data/onlyfans/b',
        }, clear=True):
            settings: uploader.ScrapeUploadSettings = (
                uploader.ScrapeUploadSettings(_env_file=None)
            )
        specs: list[uploader.AssetTargetSpec] = (
            uploader.configured_asset_target_specs(settings)
        )
        self.assertEqual([s.directory for s in specs], [
            '/data/onlyfans/a', '/data/onlyfans/b',
        ])
        descriptor: uploader.AssetDescriptor = specs[0].descriptor
        self.assertTrue(uploader.is_upload_file(self.filename, descriptor))
        self.assertFalse(uploader.is_upload_file(
            'instagram-creator-example.json.br', descriptor,
        ))
        self.assertFalse(uploader.is_upload_file(
            f'{self.filename}.invalid', descriptor,
        ))

    async def test_bulk_validates_and_routes_onlyfans_record(self) -> None:
        target: uploader.AssetUploadTarget = await self.target()
        await target.fm.write_file(self.filename, self.record)
        with patch.object(uploader, 'upload_prepared_bulk_batch', AsyncMock(
            return_value=BulkBatchOutcome(
                status='completed', success=1, failed=0, missing=0,
                success_ids={'example'}, job_id='test-job',
            ),
        )) as upload:
            count: int = await uploader.drain_bulk_target_once(
                settings=self.settings, target=target, client=self.client,
            )
        self.assertEqual(count, 1)
        payload: bytes
        records: list[tuple[str, str]]
        config: uploader.BulkUploadConfig
        payload, records, config = upload.await_args.args
        self.assertEqual(json.loads(payload), self.record)
        self.assertEqual(records, [('example', self.filename)])
        self.assertEqual((config.platform, config.entity),
                         ('onlyfans', 'creator'))
        self.assertEqual(config.filename_prefix, 'onlyfans-creators')

    async def test_background_routes_and_preserves_public_fields(self) -> None:
        target: uploader.AssetUploadTarget = await self.target()
        await target.fm.write_file(self.filename, self.record)
        self.client.enqueue_upload.return_value = True
        queued: set[tuple[str, str]] = set()
        count: int = await uploader.drain_background_target_once(
            settings=self.settings, target=target, client=self.client,
            queued_files=queued,
        )
        self.assertEqual(count, 1)
        payload: dict[str, Any] = (
            self.client.enqueue_upload.call_args.kwargs['json']
        )
        self.assertEqual(payload['platform'], 'onlyfans')
        self.assertEqual(payload['entity'], 'creator')
        self.assertEqual(payload['username'], 'drand')
        self.assertEqual(payload['version'], '0.0.1')
        self.assertEqual(payload['data'], self.record)
        self.assertEqual(payload['data']['like_count'], 0)
        self.assertIsNone(payload['data']['fan_count'])

    async def test_invalid_record_is_not_uploaded(self) -> None:
        target: uploader.AssetUploadTarget = await self.target()
        invalid: dict[str, Any] = {**self.record, 'like_count': -1}
        await target.fm.write_file(self.filename, invalid)
        count: int = await uploader.drain_background_target_once(
            settings=self.settings, target=target, client=self.client,
            queued_files=set(),
        )
        self.assertEqual(count, 0)
        self.client.enqueue_upload.assert_not_called()
        self.assertTrue(Path(
            self.directory.name, f'{self.filename}.invalid',
        ).exists())

    async def test_bulk_results_keep_failed_numeric_id_record_for_retry(
        self,
    ) -> None:
        target: uploader.AssetUploadTarget = await self.target()
        failed_filename: str = 'onlyfans-creator-failed.json.br'
        await target.fm.write_file(self.filename, self.record)
        await target.fm.write_file(failed_filename, self.record)
        outcome: tuple[int, int, int, set[str]] = await apply_bulk_results(
            [('example', self.filename), ('failed', failed_filename)],
            BulkResults(
                total=2, succeeded=1, failed=1, duplicate=0,
                failures=[{
                    'platform_content_id': self.record['user_id'],
                    'record_index': 1,
                }],
            ),
            target.fm, 'test-batch', 'test-job',
        )
        self.assertEqual(outcome, (1, 1, 0, {'example'}))
        self.assertTrue(Path(
            self.directory.name, 'uploaded', self.filename,
        ).exists())
        self.assertTrue(Path(self.directory.name, failed_filename).exists())
