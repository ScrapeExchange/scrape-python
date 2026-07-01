import unittest

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scrape_exchange.brotli import brotli_write_async
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from tools.yt_backfill_channel_category_country import (
    BackfillSettings,
    CATEGORY_THRESHOLD,
    ChannelEvidence,
    VideoEvidence,
    build_channel_plan,
    configure_backfill_logging,
    count_video_categories,
    dedupe_video_evidence,
    log_channel_category_decision,
    merge_category_counts,
    needs_api_video_evidence,
    normalize_category,
    plan_channel_category_update,
    plan_video_country_update,
    run,
    select_channel_category,
)


class TestCategoryPlanning(unittest.TestCase):
    def test_normalize_category_collapses_whitespace(self) -> None:
        self.assertEqual(
            normalize_category('  People   &   Blogs  '),
            'People & Blogs',
        )
        self.assertIsNone(normalize_category('   '))
        self.assertIsNone(normalize_category(None))

    def test_select_channel_category_requires_more_than_threshold(
        self,
    ) -> None:
        counts: dict[str, int] = {'Music': CATEGORY_THRESHOLD}

        result = select_channel_category(counts)

        self.assertIsNone(result.category)
        self.assertEqual(result.reason, 'below_threshold')

    def test_select_channel_category_returns_single_winner(self) -> None:
        counts: dict[str, int] = {'Music': 21, 'Education': 5}

        result = select_channel_category(counts)

        self.assertEqual(result.category, 'Music')
        self.assertIsNone(result.reason)

    def test_select_channel_category_rejects_tie(self) -> None:
        counts: dict[str, int] = {'Music': 25, 'Education': 25}

        result = select_channel_category(counts)

        self.assertIsNone(result.category)
        self.assertEqual(result.reason, 'tie')

    def test_count_video_categories_uses_normalized_values(self) -> None:
        first: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        first.category = ' Music '
        second: YouTubeVideo = YouTubeVideo(video_id='bbbbbbbbbbb')
        second.category = 'Music'
        third: YouTubeVideo = YouTubeVideo(video_id='ccccccccccc')

        counts: dict[str, int] = count_video_categories([
            first, second, third,
        ])

        self.assertEqual(counts, {'Music': 2})

    def test_merge_counts_replaces_without_reuse(self) -> None:
        merged: dict[str, int] = merge_category_counts(
            current={'Music': 10},
            observed={'Education': 2},
            reuse_existing_data=False,
        )

        self.assertEqual(merged, {'Education': 2})

    def test_merge_counts_adds_with_reuse(self) -> None:
        merged: dict[str, int] = merge_category_counts(
            current={'Music': 10},
            observed={'Music': 2, 'Education': 1},
            reuse_existing_data=True,
        )

        self.assertEqual(merged, {'Music': 12, 'Education': 1})

    def test_plan_channel_category_fills_missing_only(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')

        plan = plan_channel_category_update(channel, 'Music')

        self.assertTrue(plan.should_update)
        self.assertEqual(plan.value, 'Music')

    def test_plan_channel_category_reports_existing_value(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        channel.category = 'Education'

        plan = plan_channel_category_update(channel, 'Music')

        self.assertFalse(plan.should_update)
        self.assertEqual(plan.reason, 'existing_category')

    def test_plan_video_country_reports_conflict(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        video.channel_country = 'GB'

        plan = plan_video_country_update(video, 'US')

        self.assertFalse(plan.should_update)
        self.assertEqual(plan.reason, 'conflicting_channel_country')

    def test_dedupe_video_evidence_prefers_local(self) -> None:
        local: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        local.category = 'Music'
        remote: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        remote.category = 'Education'

        deduped: list[VideoEvidence] = dedupe_video_evidence([
            VideoEvidence(video=remote, source='api'),
            VideoEvidence(video=local, source='local'),
        ])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].video.category, 'Music')
        self.assertEqual(deduped[0].source, 'local')

    def test_build_plan_adopts_api_country_for_local_channel(self) -> None:
        local_channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        api_channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        api_channel.country = 'US'
        video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        video.channel_id = 'UC123'

        plan = build_channel_plan(
            local=ChannelEvidence(channel=local_channel, source='local'),
            api=ChannelEvidence(channel=api_channel, source='api'),
            videos=[VideoEvidence(video=video, source='local')],
            existing_counts={},
            reuse_existing_data=False,
            overwrite_category=False,
            overwrite_country=False,
        )

        self.assertIsNotNone(plan)
        self.assertIsNone(local_channel.country)
        self.assertEqual(plan.country, 'US')
        self.assertEqual(plan.country_source, 'api')
        self.assertTrue(plan.channel_country_update.should_update)
        self.assertEqual(plan.channel_country_update.value, 'US')

    def test_build_plan_reports_country_conflict(self) -> None:
        local_channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        local_channel.country = 'GB'
        api_channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        api_channel.country = 'US'

        plan = build_channel_plan(
            local=ChannelEvidence(channel=local_channel, source='local'),
            api=ChannelEvidence(channel=api_channel, source='api'),
            videos=[],
            existing_counts={},
            reuse_existing_data=False,
            overwrite_category=False,
            overwrite_country=False,
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan.country, 'GB')
        self.assertEqual(plan.country_conflict, 'local=GB api=US')

    def test_api_video_evidence_not_needed_when_local_wins(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        videos: list[VideoEvidence] = []
        for index in range(21):
            video: YouTubeVideo = YouTubeVideo(video_id=f'{index:011d}')
            video.category = 'Music'
            videos.append(VideoEvidence(video=video, source='local'))

        self.assertFalse(
            needs_api_video_evidence(
                channel=channel,
                local_videos=videos,
                country=None,
                apply_remote=False,
            )
        )

    def test_api_video_evidence_needed_when_local_below_threshold(
        self,
    ) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        video: YouTubeVideo = YouTubeVideo(video_id='aaaaaaaaaaa')
        video.category = 'Music'

        self.assertTrue(
            needs_api_video_evidence(
                channel=channel,
                local_videos=[VideoEvidence(video=video, source='local')],
                country=None,
                apply_remote=False,
            )
        )


class TestBackfillLogging(unittest.TestCase):
    def test_configure_logging_uses_settings_values(self) -> None:
        settings: BackfillSettings = BackfillSettings(_cli_parse_args=[
            '--log-level', 'DEBUG',
            '--log-file', '/dev/stdout',
            '--log-format', 'text',
        ])

        with patch(
            'tools.yt_backfill_channel_category_country.configure_logging'
        ) as configure:
            configure_backfill_logging(settings)

        configure.assert_called_once_with(
            level='DEBUG',
            filename='/dev/stdout',
            log_format='text',
        )

    def test_logs_channel_category_decision_details(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_id='UC123')
        videos: list[VideoEvidence] = []
        categories: list[str] = ['Music', 'Music', 'Education', 'Gaming']
        for index, category in enumerate(categories):
            video: YouTubeVideo = YouTubeVideo(video_id=f'{index:011d}')
            video.category = category
            videos.append(VideoEvidence(video=video, source='local'))
        plan = build_channel_plan(
            local=ChannelEvidence(channel=channel, source='local'),
            api=None,
            videos=videos,
            existing_counts={},
            reuse_existing_data=False,
            overwrite_category=False,
            overwrite_country=False,
        )
        self.assertIsNotNone(plan)

        with patch(
            'tools.yt_backfill_channel_category_country.LOGGER.info'
        ) as info:
            log_channel_category_decision(
                plan,
                local_video_files_read=4,
                api_calls=3,
            )

        info.assert_called_once()
        self.assertEqual(
            info.call_args.args[0],
            'Decided YouTube channel category',
        )
        extra: dict = info.call_args.kwargs['extra']
        self.assertEqual(extra['channel_id'], 'UC123')
        self.assertEqual(extra['local_video_files_read'], 4)
        self.assertEqual(extra['api_calls'], 3)
        self.assertEqual(extra['videos_evaluated'], 4)
        self.assertEqual(
            extra['top_video_categories'],
            [
                {'category': 'Music', 'video_count': 2},
                {'category': 'Education', 'video_count': 1},
                {'category': 'Gaming', 'video_count': 1},
            ],
        )


class TestStreamingRun(unittest.IsolatedAsyncioTestCase):
    async def test_channel_id_run_does_not_list_whole_directories(
        self,
    ) -> None:
        with TemporaryDirectory() as channel_dir:
            with TemporaryDirectory() as video_dir:
                channel: YouTubeChannel = YouTubeChannel(
                    channel_id='UC123',
                )
                channel.video_ids = {'aaaaaaaaaaa'}
                video: YouTubeVideo = YouTubeVideo(
                    video_id='aaaaaaaaaaa',
                )
                video.channel_id = 'UC123'
                video.category = 'Music'
                channel_path: Path = Path(channel_dir)
                video_path: Path = Path(video_dir)
                await brotli_write_async(
                    channel_path / 'channel-UC123.json.br',
                    channel.to_dict(with_video_ids=True),
                )
                await brotli_write_async(
                    video_path / 'video-dlp-aaaaaaaaaaa.json.br',
                    video.to_dict(),
                )
                settings: BackfillSettings = BackfillSettings(
                    _cli_parse_args=[
                        '--include-api-channels', 'false',
                        '--redis-dsn', '',
                        '--channel-id', 'UC123',
                        '--channel-data-directory', channel_dir,
                        '--video-data-directory', video_dir,
                    ],
                )

                with patch.object(
                    AssetFileManagement,
                    'list_base',
                    side_effect=AssertionError('list_base called'),
                ):
                    with patch.object(
                        AssetFileManagement,
                        'list_uploaded',
                        side_effect=AssertionError(
                            'list_uploaded called',
                        ),
                    ):
                        with patch('builtins.print'):
                            summary = await run(settings)

                self.assertEqual(summary.channels_seen, 1)
                self.assertEqual(summary.channels_planned, 1)


if __name__ == '__main__':
    unittest.main()
