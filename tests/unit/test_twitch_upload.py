'''Twitch creator discovery and shared upload pipeline contracts.'''

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.bulk_upload import BulkBatchOutcome
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.schema_validator import SchemaValidator
from scrape_exchange.twitch.twitch_profile_extractor import extract_profile
from tools import scrape_upload


class TestTwitchUpload(unittest.IsolatedAsyncioTestCase):
    def test_directory_environment_selects_twitch_schema(self) -> None:
        with patch.dict(os.environ, {
            'TWITCH_CREATOR_DATA_DIR': '/data/twitch/a,/data/twitch/b',
        }, clear=True):
            settings: scrape_upload.ScrapeUploadSettings = (
                scrape_upload.ScrapeUploadSettings(_env_file=None)
            )
        specs: list[scrape_upload.AssetTargetSpec] = (
            scrape_upload.configured_asset_target_specs(settings)
        )
        self.assertEqual([s.directory for s in specs], [
            '/data/twitch/a', '/data/twitch/b',
        ])
        descriptor: scrape_upload.AssetDescriptor = specs[0].descriptor
        self.assertEqual((descriptor.platform, descriptor.entity), (
            'twitch', 'creator',
        ))
        self.assertEqual((descriptor.schema_owner, descriptor.schema_version),
                         ('drand', '0.0.1'))
        self.assertTrue(scrape_upload.is_upload_file(
            'twitch-creator-example.json.br', descriptor,
        ))
        self.assertFalse(scrape_upload.is_upload_file(
            'instagram-creator-example.json.br', descriptor,
        ))

    async def test_saved_profiles_use_bulk_and_background_uploads(
        self,
    ) -> None:
        descriptor: scrape_upload.AssetDescriptor = (
            scrape_upload.descriptor_for('twitch', 'creator')
        )
        schema_path: Path = (
            Path(__file__).resolve().parents[1] / 'collateral'
            / 'drand-twitch-creator-schema.json'
        )
        validator: SchemaValidator = SchemaValidator(
            json.loads(schema_path.read_text()),
        )
        # HTML-only partial profiles must remain uploadable without an ID.
        record: dict = extract_profile([], '''
            <link rel="canonical" href="https://localhost/example">
            <h1 data-a-target="user-display-name">Example</h1>
        ''', 'example', 'https://localhost').to_dict()
        filename: str = 'twitch-creator-example.json.br'
        for mode in ('bulk', 'background'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                with patch.dict(os.environ, {}, clear=True):
                    settings: scrape_upload.ScrapeUploadSettings = (
                        scrape_upload.ScrapeUploadSettings(_env_file=None)
                    )
                fm: AssetFileManagement = AssetFileManagement(
                    tmp, prefix_rankings=descriptor.prefix_rankings,
                )
                await fm.write_file(filename, record)
                client: MagicMock = MagicMock()
                if mode == 'bulk':
                    async def uploaded(
                        *args: object, fm: AssetFileManagement,
                        **kwargs: object,
                    ) -> BulkBatchOutcome:
                        await fm.mark_uploaded(filename)
                        return BulkBatchOutcome(
                            status='completed', job_id='job', success=1,
                            failed=0, missing=0, success_ids={'example'},
                        )

                    upload: AsyncMock = AsyncMock(side_effect=uploaded)
                    with patch(
                        'tools.scrape_upload.upload_prepared_bulk_batch',
                        upload,
                    ), patch(
                        'tools.scrape_upload.resume_pending_bulk_uploads',
                        new_callable=AsyncMock,
                    ):
                        await scrape_upload.drain_bulk_directory(
                            settings=settings, descriptor=descriptor,
                            client=client, fm=fm, validator=validator,
                        )
                    upload.assert_awaited_once()
                    self.assertEqual(json.loads(upload.call_args.args[0]),
                                     record)
                    self.assertEqual(upload.call_args.args[1], [
                        ('example', filename),
                    ])
                    self.assertEqual(upload.call_args.args[2].platform,
                                     'twitch')
                    self.assertEqual(upload.call_args.args[2].schema_owner,
                                     'drand')
                    self.assertTrue(
                        (Path(tmp) / 'uploaded' / filename).exists(),
                    )
                else:
                    await scrape_upload.drain_background_directory(
                        settings=settings, descriptor=descriptor,
                        client=client, fm=fm, validator=validator,
                    )
                    client.enqueue_upload.assert_called_once()
                    kwargs: dict = client.enqueue_upload.call_args.kwargs
                    self.assertEqual(kwargs['platform'], 'twitch')
                    self.assertEqual(kwargs['entity'], 'creator')
                    self.assertEqual(kwargs['filename'], filename)
                    self.assertIs(kwargs['file_manager'], fm)
                    self.assertEqual(kwargs['json']['data'], record)
                    self.assertEqual(kwargs['json']['username'], 'drand')
                    self.assertEqual(kwargs['json']['version'], '0.0.1')
