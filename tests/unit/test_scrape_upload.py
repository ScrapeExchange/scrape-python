'''Unit tests for the generic scrape upload tool.'''

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import os
import signal
import sys
import tempfile
import unittest

import brotli
from prometheus_client import REGISTRY

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    BulkUploadState,
    delete_bulk_state,
    list_bulk_states,
    write_bulk_state,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.schema_validator import SchemaValidator
from tests.unit._bulk_upload_fakes import FakeBulkExchange
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
        'twitch_creator_data_directory': None,
        'onlyfans_creator_data_directory': None,
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

    def test_settings_max_active_bulk_jobs_defaults_to_three(
        self,
    ) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(settings.max_active_bulk_jobs, 3)

    def test_settings_read_bulk_max_active_jobs_env(self) -> None:
        with patch.dict(
            os.environ, {'BULK_MAX_ACTIVE_JOBS': '2'}, clear=True,
        ):
            settings = scrape_upload.ScrapeUploadSettings(
                _env_file=None,
            )

        self.assertEqual(settings.max_active_bulk_jobs, 2)

    def test_settings_pending_count_interval(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            default = scrape_upload.ScrapeUploadSettings(_env_file=None)
        with patch.dict(
            os.environ,
            {'SCRAPE_UPLOAD_PENDING_COUNT_INTERVAL': '60'},
            clear=True,
        ):
            custom = scrape_upload.ScrapeUploadSettings(_env_file=None)

        self.assertEqual(default.pending_count_interval_seconds, 600.0)
        self.assertEqual(custom.pending_count_interval_seconds, 60.0)


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


def _batch_count(outcome: str) -> float:
    return REGISTRY.get_sample_value('upload_batches_total', {
        'platform': 'example', 'scraper': scrape_upload.SCRAPER_LABEL,
        'entity': 'thing', 'mode': 'bulk',
        'worker_id': scrape_upload.get_worker_id(), 'outcome': outcome,
    }) or 0.0


class TestDrainDirectories(unittest.IsolatedAsyncioTestCase):

    async def test_bulk_upload_rotates_between_targets(self) -> None:
        descriptor_a: scrape_upload.AssetDescriptor = _descriptor()
        descriptor_b: scrape_upload.AssetDescriptor = _other_descriptor()

        fake: FakeBulkExchange = FakeBulkExchange()

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            fake.patched(),
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
            [
                (config.platform, config.entity)
                for config, _b, _r in fake.posts
            ],
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

        fake: FakeBulkExchange = FakeBulkExchange()

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            fake.patched(),
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


class TestOverlappingBulkUpload(unittest.IsolatedAsyncioTestCase):

    async def _write(
        self, fm: AssetFileManagement, *ids: str,
    ) -> None:
        for asset_id in ids:
            await fm.write_file(
                f'asset-{asset_id}.json.br',
                {'id': asset_id, 'url': f'https://scrape.exchange/{asset_id}'},
            )

    def _target(
        self, fm: AssetFileManagement,
        descriptor: scrape_upload.AssetDescriptor | None = None,
    ) -> scrape_upload.AssetUploadTarget:
        return scrape_upload.AssetUploadTarget(
            descriptor=descriptor or _descriptor(), fm=fm,
            validator=_validator(),
        )

    async def test_posts_next_batch_before_first_finalizes(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b', 'c')
            drain = asyncio.create_task(
                scrape_upload.drain_targets_round_robin(
                    settings=_settings(
                        bulk_batch_size=1, max_active_bulk_jobs=2,
                    ),
                    targets=[self._target(fm)], client=object(),
                ),
            )
            for _ in range(50):
                await asyncio.sleep(0.01)
                if len(fake.posts) >= 2:
                    break
            self.assertEqual(len(fake.posts), 2)
            self.assertEqual(fake.finalized, [])
            fake.release.set()
            await asyncio.wait_for(drain, 5)

        self.assertEqual(len(fake.posts), 3)
        self.assertLessEqual(fake.max_in_finalize, 2)

    async def test_cap_does_not_block_other_target(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
            fake.patched(),
        ):
            fm_a = AssetFileManagement(
                dir_a, prefix_rankings=_descriptor().prefix_rankings,
            )
            fm_b = AssetFileManagement(
                dir_b, prefix_rankings=_other_descriptor().prefix_rankings,
            )
            await self._write(fm_a, 'a1', 'a2', 'a3')
            await fm_b.write_file(
                'other-b1.json.br',
                {'id': 'b1', 'url': 'https://scrape.exchange/b1'},
            )
            drain = asyncio.create_task(
                scrape_upload.drain_targets_round_robin(
                    settings=_settings(
                        bulk_batch_size=1, max_active_bulk_jobs=1,
                    ),
                    targets=[
                        self._target(fm_a),
                        self._target(fm_b, _other_descriptor()),
                    ],
                    client=object(),
                ),
            )
            for _ in range(50):
                await asyncio.sleep(0.01)
                if len(fake.posts) >= 2:
                    break
            entities: list[str] = [c.entity for c, _b, _r in fake.posts]
            self.assertEqual(sorted(entities), ['item', 'thing'])
            fake.release.set()
            await asyncio.wait_for(drain, 5)

    async def test_in_flight_files_not_resent(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            settings = _settings(max_active_bulk_jobs=3)
            first: int = await scrape_upload.drain_bulk_target_once(
                settings=settings, target=target, client=object(),
            )
            # Same file rewritten while its batch is in flight.
            await self._write(fm, 'a')
            for _ in range(3):
                await scrape_upload.drain_bulk_target_once(
                    settings=settings, target=target, client=object(),
                )
            fake.release.set()
            await asyncio.gather(*target.upload.in_flight_jobs)

        self.assertEqual(first, 1)
        self.assertEqual(len(fake.posts), 1)

    async def test_finalize_error_counts_and_releases(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        fake.finalize_error = RuntimeError('boom')
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            before: float = _batch_count('finalize_error')
            with self.assertLogs(level='ERROR'):
                await scrape_upload.drain_bulk_target_once(
                    settings=_settings(), target=target, client=object(),
                )
                await asyncio.gather(
                    *target.upload.in_flight_jobs, return_exceptions=True,
                )
                await asyncio.sleep(0)

            self.assertEqual(_batch_count('finalize_error') - before, 1)
            self.assertEqual(target.upload.in_flight_jobs, set())
            self.assertEqual(target.upload.in_flight_files, set())
            fake.finalize_error = None
            await scrape_upload.drain_targets_round_robin(
                settings=_settings(), targets=[target], client=object(),
                resume_bulk=False,
            )

        self.assertEqual(len(fake.posts), 2)

    async def test_waits_for_finalize_instead_of_spinning(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b')
            target = self._target(fm)
            calls: int = 0
            real = scrape_upload.drain_bulk_target_once

            async def counting(**kwargs: object) -> int:
                nonlocal calls
                calls += 1
                return await real(**kwargs)

            with patch.object(
                scrape_upload, 'drain_bulk_target_once', counting,
            ):
                drain = asyncio.create_task(
                    scrape_upload.drain_targets_round_robin(
                        settings=_settings(
                            bulk_batch_size=1, max_active_bulk_jobs=1,
                        ),
                        targets=[target], client=object(),
                    ),
                )
                await asyncio.sleep(0.2)
                calls_while_blocked: int = calls
                fake.release.set()
                await asyncio.wait_for(drain, 5)

        self.assertLessEqual(calls_while_blocked, 3)

    async def test_batch_overflow_keeps_remaining_names(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b', 'c', 'd', 'e')
            await scrape_upload.drain_targets_round_robin(
                settings=_settings(
                    bulk_batch_size=2, scrape_upload_concurrency=4,
                ),
                targets=[self._target(fm)], client=object(),
            )

        sent: list[str] = sorted(
            cid for _c, _b, records in fake.posts for cid, _f in records
        )
        self.assertEqual(sent, ['a', 'b', 'c', 'd', 'e'])

    async def test_vanished_file_is_skipped_quietly(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b')
            target = self._target(fm)
            # Prime the listing, then remove one listed file.
            names: list[str] = await scrape_upload._next_upload_names(
                target, 10,
            )
            target.upload.pending.extendleft(reversed(names))
            os.remove(os.path.join(tmp, 'asset-a.json.br'))
            with self.assertNoLogs(level='WARNING'):
                await scrape_upload.drain_targets_round_robin(
                    settings=_settings(), targets=[target],
                    client=object(), resume_bulk=False,
                )

        sent: list[str] = [
            cid for _c, _b, records in fake.posts for cid, _f in records
        ]
        self.assertEqual(sent, ['b'])

    async def test_post_exception_leaves_nothing_in_flight(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        fake.post_error = RuntimeError('network')
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            with self.assertRaises(RuntimeError):
                await scrape_upload.drain_bulk_target_once(
                    settings=_settings(), target=target, client=object(),
                )

        self.assertEqual(target.upload.in_flight_jobs, set())
        self.assertEqual(target.upload.in_flight_files, set())

    async def test_cap_reached_without_blocking_or_resume(self) -> None:
        self.assertFalse(hasattr(scrape_upload, 'reserve_bulk_upload_slot'))
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b', 'c', 'd')
            real_post = fake.post

            async def post_with_state(
                batch_buf: bytes, batch_records: list[tuple[str, str]],
                config: object, client: object, fm: AssetFileManagement,
            ) -> tuple[str, str, BulkBatchOutcome | None]:
                result = await real_post(
                    batch_buf, batch_records, config, client, fm,
                )
                await write_bulk_state(fm, BulkUploadState(
                    job_id=result[0], batch_id=result[1],
                    schema_owner='owner', schema_version='1.0.0',
                    platform='example', entity='thing',
                    upload_filename='x', batch_records=batch_records,
                ))
                return result

            resume: AsyncMock = AsyncMock()
            with (
                patch(
                    'tools.scrape_upload.post_prepared_bulk_batch',
                    side_effect=post_with_state,
                ),
                patch(
                    'tools.scrape_upload.resume_pending_bulk_uploads',
                    resume,
                ),
            ):
                drain = asyncio.create_task(
                    scrape_upload.drain_targets_round_robin(
                        settings=_settings(
                            bulk_batch_size=1, max_active_bulk_jobs=3,
                        ),
                        targets=[self._target(fm)], client=object(),
                        resume_bulk=False,
                    ),
                )
                for _ in range(50):
                    await asyncio.sleep(0.01)
                    if len(fake.posts) >= 3:
                        break
                self.assertEqual(len(fake.posts), 3)
                await asyncio.sleep(0.05)
                self.assertEqual(len(fake.posts), 3)
                resume.assert_not_called()
                fake.release.set()
                await asyncio.wait_for(drain, 5)

        self.assertEqual(len(fake.posts), 4)
        resume.assert_not_called()

    async def test_every_record_posted_exactly_once(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b', 'c', 'd', 'e')
            await asyncio.wait_for(
                scrape_upload.drain_targets_round_robin(
                    settings=_settings(
                        bulk_batch_size=50, scrape_upload_concurrency=2,
                    ),
                    targets=[self._target(fm)], client=object(),
                ),
                5,
            )

        sent: list[str] = sorted(
            cid for _c, _b, records in fake.posts for cid, _f in records
        )
        self.assertEqual(sent, ['a', 'b', 'c', 'd', 'e'])


    async def test_publish_pending_counts_sets_gauge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b')
            Path(tmp, 'asset-c.json.br.invalid').write_text('x')
            with self.assertLogs(level='INFO') as logs:
                await scrape_upload.publish_pending_counts(
                    [self._target(fm)],
                )

        records: list[logging.LogRecord] = [
            record for record in logs.records
            if record.getMessage() == 'Files pending upload'
        ]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].levelno, logging.INFO)
        self.assertEqual(records[0].platform, 'example')
        self.assertEqual(records[0].entity, 'thing')
        self.assertEqual(records[0].count, 2)

        value: float | None = REGISTRY.get_sample_value(
            'files_pending_upload', {
                'platform': 'example', 'scraper': scrape_upload.SCRAPER_LABEL,
                'entity': 'thing', 'worker_id': scrape_upload.get_worker_id(),
            },
        )
        self.assertEqual(value, 2.0)

    async def test_drain_in_flight_cancels_after_timeout(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            await scrape_upload.drain_bulk_target_once(
                settings=_settings(), target=target, client=object(),
            )
            tasks: set[asyncio.Task] = set(target.upload.in_flight_jobs)
            await scrape_upload.drain_in_flight([target], 0.05)

            self.assertTrue(all(task.done() for task in tasks))
            self.assertTrue(all(task.cancelled() for task in tasks))
            self.assertIsNone(target.upload.listing)
            # The file is still on disk for the resume / next run.
            self.assertTrue(Path(tmp, 'asset-a.json.br').exists())

    async def test_drain_in_flight_waits_for_quick_jobs(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            await scrape_upload.drain_bulk_target_once(
                settings=_settings(), target=target, client=object(),
            )
            await scrape_upload.drain_in_flight([target], 5.0)

        self.assertEqual(fake.finalized, ['job1'])

    async def test_orphaned_jobs_bounded_then_resumed(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        fake.write_state = True
        fake.finalize_status = 'progress_failed'
        posts_at_resume: list[int] = []

        async def resume(
            fm: AssetFileManagement, *args: object, **kwargs: object,
        ) -> None:
            posts_at_resume.append(len(fake.posts))
            for state in list_bulk_states(fm):
                await delete_bulk_state(fm, state.job_id)

        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a', 'b', 'c', 'd', 'e', 'f')
            target = self._target(fm)
            with (
                patch(
                    'tools.scrape_upload.resume_pending_bulk_uploads',
                    side_effect=resume,
                ),
                self.assertLogs(level='WARNING') as logs,
            ):
                await asyncio.wait_for(
                    scrape_upload.drain_targets_round_robin(
                        settings=_settings(
                            bulk_batch_size=1, max_active_bulk_jobs=2,
                        ),
                        targets=[target], client=object(),
                        resume_bulk=False,
                    ),
                    5,
                )
            orphans: int = len(list_bulk_states(fm))

        # Two POSTs fill the cap; their state files stay behind, so
        # one resume runs; after it two more POSTs fill the cap again
        # and the drain ends instead of re-posting forever.
        self.assertEqual(posts_at_resume, [2])
        self.assertIn(
            'Orphaned bulk jobs reached the cap; resuming them',
            [record.getMessage() for record in logs.records],
        )
        self.assertEqual(len(fake.posts), 4)
        self.assertEqual(orphans, 2)
        self.assertEqual(target.upload.in_flight_jobs, set())
        self.assertEqual(target.upload.in_flight_job_ids, {})

    async def test_idle_target_scans_once_per_call(self) -> None:
        calls: int = 0
        real = scrape_upload.StreamingListing.next_chunk

        async def counting(
            listing: scrape_upload.StreamingListing,
        ) -> tuple[list[str], bool]:
            nonlocal calls
            calls += 1
            return await real(listing)

        with tempfile.TemporaryDirectory() as tmp:
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            target = self._target(fm)
            with patch.object(
                scrape_upload.StreamingListing, 'next_chunk', counting,
            ):
                posted: int = await scrape_upload.drain_bulk_target_once(
                    settings=_settings(), target=target, client=object(),
                )
            target.upload.listing.close()

        self.assertEqual(posted, 0)
        self.assertEqual(calls, 1)

    async def test_round_robin_repolls_while_job_hangs(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange(hold_finalize=True)
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            calls: int = 0
            real = scrape_upload.drain_bulk_target_once

            async def counting(**kwargs: object) -> int:
                nonlocal calls
                calls += 1
                return await real(**kwargs)

            with (
                patch.object(
                    scrape_upload, 'drain_bulk_target_once', counting,
                ),
                patch.object(
                    scrape_upload, 'ROUND_ROBIN_POLL_SECONDS', 0.02,
                    create=True,
                ),
            ):
                drain = asyncio.create_task(
                    scrape_upload.drain_targets_round_robin(
                        settings=_settings(max_active_bulk_jobs=1),
                        targets=[self._target(fm)], client=object(),
                        resume_bulk=False,
                    ),
                )
                await asyncio.sleep(0.3)
                calls_while_hung: int = calls
                fake.release.set()
                await asyncio.wait_for(drain, 5)

        self.assertGreaterEqual(calls_while_hung, 4)

    async def test_post_error_outcome_is_not_progress(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        fake.post_outcome = BulkBatchOutcome(
            status='post_rejected', job_id=None,
            success=0, failed=0, missing=0,
        )
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            await self._write(fm, 'a')
            target = self._target(fm)
            before: float = _batch_count('post_rejected')
            posted: int = await scrape_upload.drain_bulk_target_once(
                settings=_settings(), target=target, client=object(),
            )
            await asyncio.wait_for(
                scrape_upload.drain_targets_round_robin(
                    settings=_settings(), targets=[target],
                    client=object(), resume_bulk=False,
                ),
                2,
            )

        self.assertEqual(posted, 0)
        self.assertEqual(_batch_count('post_rejected') - before, 2)
        self.assertEqual(len(fake.posts), 2)

    async def test_drain_terminates_when_no_file_prepares(self) -> None:
        fake: FakeBulkExchange = FakeBulkExchange()
        with tempfile.TemporaryDirectory() as tmp, fake.patched():
            fm = AssetFileManagement(
                tmp, prefix_rankings=_descriptor().prefix_rankings,
            )
            for asset_id in ('x', 'y', 'z'):
                # Schema-invalid: no 'url'.
                await fm.write_file(
                    f'asset-{asset_id}.json.br', {'id': asset_id},
                )
            with self.assertLogs(level='WARNING'):
                await asyncio.wait_for(
                    scrape_upload.drain_targets_round_robin(
                        settings=_settings(bulk_batch_size=1),
                        targets=[self._target(fm)], client=object(),
                    ),
                    5,
                )

        self.assertEqual(fake.posts, [])

    async def test_drain_in_flight_closes_all_listings_on_error(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
        ):
            target_a = self._target(AssetFileManagement(dir_a))
            target_b = self._target(AssetFileManagement(dir_b))
            broken: MagicMock = MagicMock()
            broken.close.side_effect = OSError('close failed')
            healthy: MagicMock = MagicMock()
            target_a.upload.listing = broken
            target_b.upload.listing = healthy
            with self.assertLogs(level='WARNING'):
                await scrape_upload.drain_in_flight(
                    [target_a, target_b], 0.1,
                )

        healthy.close.assert_called_once()
        self.assertIsNone(target_a.upload.listing)
        self.assertIsNone(target_b.upload.listing)


class TestShutdown(unittest.IsolatedAsyncioTestCase):

    async def test_sigterm_cancels_run_and_drains_clamped(self) -> None:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        handlers: dict[int, object] = {}
        entered: asyncio.Event = asyncio.Event()

        async def block(**kwargs: object) -> None:
            entered.set()
            await asyncio.Event().wait()

        client: SimpleNamespace = SimpleNamespace(aclose=AsyncMock())
        drain: AsyncMock = AsyncMock()
        settings: SimpleNamespace = _settings(
            api_key_id='id', api_key_secret='secret', metrics_port=0,
            pending_count_interval_seconds=600.0,
            background_drain_timeout_seconds=300.0,
            scrape_upload_watch=False,
        )
        with (
            patch.object(
                loop, 'add_signal_handler',
                side_effect=lambda sig, cb: handlers.update({sig: cb}),
            ),
            patch.object(loop, 'remove_signal_handler'),
            patch.object(scrape_upload, 'configure_logging'),
            patch.object(scrape_upload, 'start_metrics_server'),
            patch.object(
                scrape_upload.ExchangeClient, 'setup',
                AsyncMock(return_value=client),
            ),
            patch.object(
                scrape_upload, 'build_upload_targets',
                AsyncMock(return_value=['t']),
            ),
            patch.object(scrape_upload, 'pending_count_loop', AsyncMock()),
            patch.object(scrape_upload, 'drain_targets_round_robin', block),
            patch.object(scrape_upload, 'drain_in_flight', drain),
        ):
            task: asyncio.Task = asyncio.create_task(
                scrape_upload.run(settings),
            )
            await asyncio.wait_for(entered.wait(), 1)
            self.assertIn(signal.SIGINT, handlers)
            handlers[signal.SIGTERM]()
            with self.assertRaises(asyncio.CancelledError):
                await task

        drain.assert_awaited_once_with(
            ['t'], scrape_upload.BULK_SHUTDOWN_DRAIN_SECONDS,
        )
        self.assertEqual(scrape_upload.BULK_SHUTDOWN_DRAIN_SECONDS, 45.0)
        client.aclose.assert_awaited_once()

    def test_main_exits_cleanly_when_cancelled(self) -> None:
        async def cancelled(settings: object) -> None:
            raise asyncio.CancelledError()

        with (
            patch.object(scrape_upload, 'ScrapeUploadSettings'),
            patch.object(scrape_upload, 'run', cancelled),
            self.assertLogs(level='INFO') as logs,
        ):
            self.assertIsNone(scrape_upload.main())

        self.assertIn(
            'scrape_upload stopped by signal',
            [record.getMessage() for record in logs.records],
        )


if __name__ == '__main__':
    unittest.main()
