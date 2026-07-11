'''Unit tests for the generic scrape upload tool.'''

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import os
import sys
import tempfile
import unittest

import brotli

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from scrape_exchange.bulk_upload import BulkBatchOutcome
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.schema_validator import SchemaValidator
from tools import scrape_upload


def _descriptor() -> scrape_upload.AssetDescriptor:
    return scrape_upload.AssetDescriptor(
        platform='example',
        entity='thing',
        prefixes=('asset-',),
        schema_owner='owner',
        schema_version='1.0.0',
        filename_prefix='assets',
        load_record=lambda data: dict(data),
    )


def _other_descriptor() -> scrape_upload.AssetDescriptor:
    return scrape_upload.AssetDescriptor(
        platform='other',
        entity='item',
        prefixes=('other-',),
        schema_owner='owner',
        schema_version='1.0.0',
        filename_prefix='items',
        load_record=lambda data: dict(data),
    )


def _validator() -> SchemaValidator:
    return SchemaValidator({
        'type': 'object',
        'required': ['id', 'url'],
        'properties': {
            'id': {'type': 'string'},
            'url': {'type': 'string'},
        },
        'additionalProperties': True,
    })


def _settings(**overrides: object) -> SimpleNamespace:
    data: dict[str, object] = {
        'schema_owner': None,
        'schema_version': None,
        'exchange_url': 'https://scrape.exchange',
        'bulk_progress_timeout_seconds': 1.0,
        'bulk_batch_size': 1000,
        'bulk_max_batch_bytes': 1024 * 1024,
        'max_active_bulk_jobs': 2,
        'scrape_upload_concurrency': 2,
        'upload_mode': 'bulk',
        'background_drain_timeout_seconds': 1.0,
        'instagram_creator_priority_queues': (
            '72:10000000,168:1000000,336:100000,720:10000,4320:0'
        ),
    }
    data.update(overrides)
    return SimpleNamespace(**data)


class TestScrapeUploadHelpers(unittest.TestCase):

    def test_content_id_from_filename(self) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()
        self.assertEqual(
            scrape_upload.content_id_from_filename(
                'asset-abc.json.br', descriptor,
            ),
            'abc',
        )

    def test_instagram_creator_uses_creator_entity(self) -> None:
        descriptor: scrape_upload.AssetDescriptor = (
            scrape_upload.descriptor_for('instagram', 'creator')
        )

        self.assertEqual(descriptor.entity, 'creator')
        self.assertEqual(
            descriptor.prefix_rankings,
            {'creator': [scrape_upload.INSTAGRAM_CREATOR_PREFIX]},
        )

    def test_upload_file_filter_rejects_markers(self) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()
        self.assertTrue(scrape_upload.is_upload_file(
            'asset-abc.json.br', descriptor,
        ))
        self.assertFalse(scrape_upload.is_upload_file(
            'asset-abc.json.br.invalid', descriptor,
        ))
        self.assertFalse(scrape_upload.is_upload_file(
            'asset-abc.json.br.tmp.123', descriptor,
        ))

    def test_configured_data_directories_define_targets(self) -> None:
        descriptor_a: scrape_upload.AssetDescriptor = _descriptor()
        descriptor_b: scrape_upload.AssetDescriptor = _other_descriptor()
        with patch.dict(
            scrape_upload.ASSET_DESCRIPTORS,
            {
                ('tiktok', 'creator'): descriptor_a,
                ('instagram', 'creator'): descriptor_b,
            },
        ):
            specs: list[scrape_upload.AssetTargetSpec] = (
                scrape_upload.configured_asset_target_specs(_settings(
                    youtube_video_data_directory=None,
                    youtube_channel_data_directory=None,
                    tiktok_video_data_directory=None,
                    tiktok_creator_data_directory='/data/a,/data/b',
                    tiktok_hashtag_data_directory=None,
                    ig_creator_data_directory='/data/c',
                ))
            )

        self.assertEqual(
            [
                (
                    spec.descriptor.platform,
                    spec.descriptor.entity,
                    spec.directory,
                )
                for spec in specs
            ],
            [
                ('example', 'thing', '/data/a'),
                ('example', 'thing', '/data/b'),
                ('other', 'item', '/data/c'),
            ],
        )

    def test_settings_load_standard_data_directory_env(self) -> None:
        with patch.dict(
            os.environ,
            {'YOUTUBE_VIDEO_DATA_DIR': '/data/videos'},
            clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        specs: list[scrape_upload.AssetTargetSpec] = (
            scrape_upload.configured_asset_target_specs(settings)
        )
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].descriptor.platform, 'youtube')
        self.assertEqual(specs[0].descriptor.entity, 'video')
        self.assertEqual(specs[0].directory, '/data/videos')

    def test_settings_ignore_command_line_data_directories(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(sys, 'argv', [
                'scrape_upload.py',
                '--youtube-video-data-directory',
                '/data/videos',
            ]),
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertIsNone(settings.youtube_video_data_directory)

    def test_settings_default_log_file_is_regular_file(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(
            settings.scrape_upload_log_file,
            scrape_upload.SCRAPE_UPLOAD_DEFAULT_LOG_FILE,
        )

    def test_settings_do_not_inherit_shared_log_file(self) -> None:
        with patch.dict(
            os.environ,
            {'LOG_FILE': '/dev/stdout'},
            clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(
            settings.scrape_upload_log_file,
            scrape_upload.SCRAPE_UPLOAD_DEFAULT_LOG_FILE,
        )

    def test_settings_default_metrics_port(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(settings.metrics_port, 9800)

    def test_settings_watch_enabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertTrue(settings.scrape_upload_watch)

    def test_settings_load_scrape_upload_watch_env(self) -> None:
        with patch.dict(
            os.environ,
            {'SCRAPE_UPLOAD_WATCH': 'false'},
            clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertFalse(settings.scrape_upload_watch)

    def test_settings_load_scrape_upload_metrics_port_env(self) -> None:
        with patch.dict(
            os.environ,
            {'SCRAPE_UPLOAD_METRICS_PORT': '9298'},
            clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(settings.metrics_port, 9298)

    def test_settings_load_asset_upload_metrics_port_env(self) -> None:
        with patch.dict(
            os.environ,
            {'ASSET_UPLOAD_METRICS_PORT': '9297'},
            clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(settings.metrics_port, 9297)

    def test_settings_reject_stdout_log_file(self) -> None:
        with (
            patch.dict(
                os.environ,
                {'SCRAPE_UPLOAD_LOG_FILE': '/dev/stdout'},
                clear=True,
            ),
            self.assertRaises(ValueError),
        ):
            scrape_upload.ScrapeUploadSettings(_env_file=None)

    def test_configure_logging_writes_to_configured_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_file: Path = Path(tmp) / 'logs' / 'scrape_upload.log'
            settings = SimpleNamespace(
                scrape_upload_log_file=str(log_file),
                scrape_upload_log_level='INFO',
                log_format='text',
            )

            try:
                scrape_upload.configure_logging(settings)
                logging.info('scrape-upload-file-log-test')

                for handler in logging.getLogger().handlers:
                    handler.flush()

                self.assertTrue(log_file.exists())
                self.assertIn(
                    'scrape-upload-file-log-test',
                    log_file.read_text(),
                )
            finally:
                for handler in logging.getLogger().handlers:
                    handler.close()
                    logging.getLogger().removeHandler(handler)

    def test_main_logs_fatal_error_to_configured_file(self) -> None:
        async def fail_run(settings: SimpleNamespace) -> None:
            scrape_upload.configure_logging(settings)
            raise RuntimeError('fatal upload startup')

        with tempfile.TemporaryDirectory() as tmp:
            log_file: Path = Path(tmp) / 'logs' / 'scrape_upload.log'
            settings = SimpleNamespace(
                scrape_upload_log_file=str(log_file),
                scrape_upload_log_level='INFO',
                log_format='text',
            )
            stderr = io.StringIO()

            try:
                with (
                    patch.object(
                        scrape_upload,
                        'ScrapeUploadSettings',
                        return_value=settings,
                    ),
                    patch.object(scrape_upload, 'run', fail_run),
                    contextlib.redirect_stderr(stderr),
                    self.assertRaises(SystemExit) as cm,
                ):
                    scrape_upload.main()

                self.assertEqual(cm.exception.code, 1)
                self.assertEqual('', stderr.getvalue())
                self.assertIn(
                    'scrape_upload failed',
                    log_file.read_text(),
                )
                self.assertIn(
                    'fatal upload startup',
                    log_file.read_text(),
                )
            finally:
                for handler in logging.getLogger().handlers:
                    handler.close()
                    logging.getLogger().removeHandler(handler)


class TestPrepareAssetLine(unittest.IsolatedAsyncioTestCase):

    async def test_youtube_channel_processor_persists_identity_maps(
        self,
    ) -> None:
        descriptor: scrape_upload.AssetDescriptor = (
            scrape_upload.descriptor_for('youtube', 'channel')
        )
        creator_puts: list[tuple[str, str]] = []
        name_puts: list[tuple[str, str]] = []

        class FakeCreatorMap:
            async def put(self, channel_id: str, handle: str) -> None:
                creator_puts.append((channel_id, handle))

        class FakeNameMap:
            async def put(
                self, *, asset_title: str, asset_id: str,
            ) -> None:
                name_puts.append((asset_title, asset_id))

        state = scrape_upload.YouTubeChannelProcessorState(
            creator_map=FakeCreatorMap(),
            name_map=FakeNameMap(),
            scrape_queue=None,
            exchange_set=None,
        )
        context = scrape_upload.AssetProcessingContext(
            settings=_settings(),
            client=object(),
            fm=object(),
            descriptor=descriptor,
            state=state,
        )

        record = await scrape_upload.YouTubeChannelProcessor().prepare_record(
            {
                'channel_id': 'UCabc',
                'channel_handle': 'ExampleHandle',
                'title': 'Example Channel',
                'url': 'https://scrape.exchange/channel',
            },
            filename='channel-UCabc.json.br',
            content_id='UCabc',
            context=context,
        )

        self.assertIsNotNone(record)
        self.assertEqual(record['channel_handle'], 'ExampleHandle')
        self.assertEqual(creator_puts, [('UCabc', 'ExampleHandle')])
        self.assertEqual(name_puts, [('Example Channel', 'UCabc')])

    async def test_youtube_video_processor_records_successful_ids(
        self,
    ) -> None:
        added: list[str] = []

        class FakeUploaded:
            async def add(self, video_id: str) -> None:
                added.append(video_id)

        state = scrape_upload.YouTubeVideoProcessorState(
            creator_map=object(),
            scrape_queue=None,
            uploaded=FakeUploaded(),
        )
        context = scrape_upload.AssetProcessingContext(
            settings=_settings(),
            client=object(),
            fm=object(),
            descriptor=scrape_upload.descriptor_for('youtube', 'video'),
            state=state,
        )

        await scrape_upload.YouTubeVideoProcessor().on_success_id(
            'video123',
            context,
        )

        self.assertEqual(added, ['video123'])

    async def test_tiktok_creator_corrupt_file_requeues_and_deletes(
        self,
    ) -> None:
        scheduled: list[tuple[str, str, int]] = []

        class FakeQueue:
            async def schedule_if_absent(
                self,
                creator_id: str,
                creator_name: str,
                delay_seconds: int,
            ) -> None:
                scheduled.append((
                    creator_id,
                    creator_name,
                    delay_seconds,
                ))

        with tempfile.TemporaryDirectory() as tmp:
            descriptor = scrape_upload.descriptor_for(
                'tiktok',
                'creator',
            )
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            await fm.write_file(
                'tiktok-creator-example.json.br',
                {'id': 'example'},
            )
            state = scrape_upload.TikTokCreatorProcessorState(
                queue=FakeQueue(),
            )
            context = scrape_upload.AssetProcessingContext(
                settings=_settings(),
                client=object(),
                fm=fm,
                descriptor=descriptor,
                state=state,
            )

            handled = await scrape_upload.TikTokCreatorProcessor(
            ).handle_brotli_error(
                filename='tiktok-creator-example.json.br',
                content_id='example',
                context=context,
                exc=brotli.error(),
            )

            self.assertTrue(handled)
            self.assertEqual(scheduled, [('example', 'example', 0)])
            self.assertFalse(
                (Path(tmp) / 'tiktok-creator-example.json.br').exists(),
            )

    async def test_schema_invalid_marks_file_invalid(self) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()
        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            await fm.write_file('asset-bad.json.br', {'id': 'bad'})

            result = await scrape_upload.prepare_asset_line(
                'asset-bad.json.br',
                fm=fm,
                descriptor=descriptor,
                validator=_validator(),
            )

            self.assertIsNone(result)
            self.assertFalse((Path(tmp) / 'asset-bad.json.br').exists())
            self.assertTrue(
                (Path(tmp) / 'asset-bad.json.br.invalid').exists(),
            )

    async def test_recoverable_corrupt_file_uploads_and_rewrites(
        self,
    ) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()
        record: dict[str, str] = {
            'id': 'good',
            'url': 'https://scrape.exchange/good',
        }

        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            path: Path = Path(tmp) / 'asset-good.json.br'
            path.write_bytes(
                brotli.compress(json.dumps(record).encode()) + b'garbage',
            )

            result = await scrape_upload.prepare_asset_line(
                'asset-good.json.br',
                fm=fm,
                descriptor=descriptor,
                validator=_validator(),
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result[0], 'good')
            self.assertEqual(result[3], record)
            rewritten: dict = json.loads(
                brotli.decompress(path.read_bytes()).decode(),
            )
            self.assertEqual(rewritten, record)

    async def test_tiktok_video_corrupt_file_requeues_and_deletes(
        self,
    ) -> None:
        forced: list[tuple[str, str]] = []

        class FakeQueue:
            async def force_enqueue(
                self,
                video_id: str,
                *,
                source: str,
                **kwargs,
            ) -> str:
                del kwargs
                forced.append((video_id, source))
                return 'revived'

        descriptor: scrape_upload.AssetDescriptor = (
            scrape_upload.descriptor_for('tiktok', 'video')
        )
        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            path: Path = Path(tmp) / 'tiktok-video-12345.json.br'
            path.write_bytes(b'not brotli')
            state = scrape_upload.TikTokVideoProcessorState(
                scrape_queue=FakeQueue(),
            )

            result = await scrape_upload.prepare_asset_line(
                'tiktok-video-12345.json.br',
                fm=fm,
                descriptor=descriptor,
                validator=_validator(),
                settings=_settings(),
                client=object(),
                processor=scrape_upload.TikTokVideoProcessor(),
                state=state,
            )

            self.assertIsNone(result)
            self.assertEqual(
                forced,
                [('12345', 'scrape_upload_corrupt_video_file')],
            )
            self.assertFalse(path.exists())

    async def test_instagram_creator_corrupt_file_requeues_and_deletes(
        self,
    ) -> None:
        scheduled: list[tuple[str, str, int]] = []

        class FakeQueue:
            async def schedule_if_absent(
                self,
                creator_id: str,
                creator_name: str,
                delay_seconds: int,
            ) -> None:
                scheduled.append((
                    creator_id,
                    creator_name,
                    delay_seconds,
                ))

        descriptor: scrape_upload.AssetDescriptor = (
            scrape_upload.descriptor_for('instagram', 'creator')
        )
        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            path: Path = Path(tmp) / 'instagram-creator-example.json.br'
            path.write_bytes(b'not brotli')
            state = scrape_upload.InstagramCreatorProcessorState(
                queue=FakeQueue(),
            )

            result = await scrape_upload.prepare_asset_line(
                'instagram-creator-example.json.br',
                fm=fm,
                descriptor=descriptor,
                validator=_validator(),
                settings=_settings(),
                client=object(),
                processor=scrape_upload.InstagramCreatorProcessor(),
                state=state,
            )

            self.assertIsNone(result)
            self.assertEqual(
                scheduled,
                [('example', 'example', 0)],
            )
            self.assertFalse(path.exists())

    async def test_build_targets_uses_descriptor_schema_identity(
        self,
    ) -> None:
        descriptor_a: scrape_upload.AssetDescriptor = _descriptor()
        descriptor_b: scrape_upload.AssetDescriptor = (
            scrape_upload.AssetDescriptor(
                platform='other',
                entity='item',
                prefixes=('other-',),
                schema_owner='drand',
                schema_version='2.0.0',
                filename_prefix='items',
                load_record=lambda data: dict(data),
            )
        )
        schema: dict = {
            'type': 'object',
            'additionalProperties': True,
        }

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            patch(
                'tools.scrape_upload.configured_asset_target_specs',
                return_value=[
                    scrape_upload.AssetTargetSpec(
                        descriptor=descriptor_a,
                        directory=dir_a,
                    ),
                    scrape_upload.AssetTargetSpec(
                        descriptor=descriptor_b,
                        directory=dir_b,
                    ),
                ],
            ),
            patch(
                'tools.scrape_upload.fetch_schema_dict',
                new=AsyncMock(return_value=schema),
            ) as fetch_schema,
        ):
            await scrape_upload.build_upload_targets(
                _settings(
                    schema_owner='boinko',
                    schema_version='0.0.2',
                ),
                client=object(),
            )

        calls: list[tuple] = [
            call.args for call in fetch_schema.call_args_list
        ]
        self.assertEqual(
            [
                (args[2], args[3], args[4], args[5])
                for args in calls
            ],
            [
                ('owner', 'example', 'thing', '1.0.0'),
                ('drand', 'other', 'item', '2.0.0'),
            ],
        )


class TestDrainDirectories(unittest.IsolatedAsyncioTestCase):

    async def test_bulk_upload_rotates_between_targets(self) -> None:
        descriptor_a: scrape_upload.AssetDescriptor = _descriptor()
        descriptor_b: scrape_upload.AssetDescriptor = _other_descriptor()
        calls: list[tuple[str, str, list[str]]] = []

        async def fake_upload(
            batch_buf: bytes,
            batch_records: list[tuple[str, str]],
            config,
            client,
            fm: AssetFileManagement,
            *,
            id_from_filename=None,
        ) -> BulkBatchOutcome:
            del batch_buf
            del client
            del id_from_filename
            calls.append((
                config.platform,
                config.entity,
                [content_id for content_id, _ in batch_records],
            ))
            for _content_id, filename in batch_records:
                await fm.mark_uploaded(filename)
            return BulkBatchOutcome(
                status='completed',
                job_id='job',
                success=len(batch_records),
                failed=0,
                missing=0,
                success_ids={r[0] for r in batch_records},
            )

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            patch(
                'tools.scrape_upload.upload_prepared_bulk_batch',
                side_effect=fake_upload,
            ),
            patch(
                'tools.scrape_upload.resume_pending_bulk_uploads',
                new=AsyncMock(),
            ),
        ):
            fm_a: AssetFileManagement = AssetFileManagement(
                dir_a,
                prefix_rankings=descriptor_a.prefix_rankings,
            )
            fm_b: AssetFileManagement = AssetFileManagement(
                dir_b,
                prefix_rankings=descriptor_b.prefix_rankings,
            )
            await fm_a.write_file(
                'asset-a1.json.br',
                {'id': 'a1', 'url': 'https://scrape.exchange/a1'},
            )
            await fm_a.write_file(
                'asset-a2.json.br',
                {'id': 'a2', 'url': 'https://scrape.exchange/a2'},
            )
            await fm_b.write_file(
                'other-b1.json.br',
                {'id': 'b1', 'url': 'https://scrape.exchange/b1'},
            )

            await scrape_upload.drain_targets_round_robin(
                settings=_settings(bulk_batch_size=1),
                targets=[
                    scrape_upload.AssetUploadTarget(
                        descriptor=descriptor_a,
                        fm=fm_a,
                        validator=_validator(),
                    ),
                    scrape_upload.AssetUploadTarget(
                        descriptor=descriptor_b,
                        fm=fm_b,
                        validator=_validator(),
                    ),
                ],
                client=object(),
            )

        self.assertEqual(
            [call[:2] for call in calls],
            [
                ('example', 'thing'),
                ('other', 'item'),
                ('example', 'thing'),
            ],
        )

    async def test_bulk_upload_moves_each_directory_file(
        self,
    ) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()

        async def fake_upload(
            batch_buf: bytes,
            batch_records: list[tuple[str, str]],
            config,
            client,
            fm: AssetFileManagement,
            *,
            id_from_filename=None,
        ) -> BulkBatchOutcome:
            del batch_buf
            del config
            del client
            del id_from_filename
            for _content_id, filename in batch_records:
                await fm.mark_uploaded(filename)
            return BulkBatchOutcome(
                status='completed',
                job_id='job',
                success=len(batch_records),
                failed=0,
                missing=0,
                success_ids={r[0] for r in batch_records},
            )

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            patch(
                'tools.scrape_upload.upload_prepared_bulk_batch',
                side_effect=fake_upload,
            ),
            patch(
                'tools.scrape_upload.resume_pending_bulk_uploads',
                new=AsyncMock(),
            ),
        ):
            fm_a: AssetFileManagement = AssetFileManagement(
                dir_a,
                prefix_rankings=descriptor.prefix_rankings,
            )
            fm_b: AssetFileManagement = AssetFileManagement(
                dir_b,
                prefix_rankings=descriptor.prefix_rankings,
            )
            await fm_a.write_file(
                'asset-a.json.br',
                {'id': 'a', 'url': 'https://scrape.exchange/a'},
            )
            await fm_b.write_file(
                'asset-b.json.br',
                {'id': 'b', 'url': 'https://scrape.exchange/b'},
            )

            await asyncio.gather(
                scrape_upload.drain_bulk_directory(
                    settings=_settings(),
                    descriptor=descriptor,
                    client=object(),
                    fm=fm_a,
                    validator=_validator(),
                ),
                scrape_upload.drain_bulk_directory(
                    settings=_settings(),
                    descriptor=descriptor,
                    client=object(),
                    fm=fm_b,
                    validator=_validator(),
                ),
            )

            self.assertTrue(
                (Path(dir_a) / 'uploaded' / 'asset-a.json.br').exists(),
            )
            self.assertTrue(
                (Path(dir_b) / 'uploaded' / 'asset-b.json.br').exists(),
            )

    async def test_background_enqueue_uses_source_file_manager(
        self,
    ) -> None:
        descriptor: scrape_upload.AssetDescriptor = _descriptor()
        calls: list[dict] = []

        class FakeClient:
            def enqueue_upload(self, *args, **kwargs) -> bool:
                del args
                calls.append(kwargs)
                return True

        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings=descriptor.prefix_rankings,
            )
            await fm.write_file(
                'asset-a.json.br',
                {'id': 'a', 'url': 'https://scrape.exchange/a'},
            )

            await scrape_upload.drain_background_directory(
                settings=_settings(upload_mode='background'),
                descriptor=descriptor,
                client=FakeClient(),
                fm=fm,
                validator=_validator(),
            )

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0]['file_manager'], fm)
        self.assertEqual(calls[0]['filename'], 'asset-a.json.br')
        self.assertEqual(calls[0]['platform'], 'example')
        self.assertEqual(calls[0]['entity'], 'thing')
        self.assertEqual(calls[0]['json']['platform'], 'example')
        self.assertEqual(calls[0]['json']['entity'], 'thing')


if __name__ == '__main__':
    unittest.main()
