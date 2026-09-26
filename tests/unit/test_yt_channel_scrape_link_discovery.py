'''Unit tests for channel-to-channel discovery in yt_channel_scrape.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube.youtube_types import (
    YouTubeChannelLink,
)


def _discovery_settings(**overrides) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.channel_discover_linked_channels = True
    settings.channel_discovery_min_subscribers = 0
    settings.exchange_url = 'https://scrape.exchange'
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings


def _mock_channel(
    links: list[YouTubeChannelLink],
    *,
    channel_id: str = 'UCe1e1e1e1e1e1e1e1e1e1e1',
) -> MagicMock:
    channel: MagicMock = MagicMock()
    channel.channel_id = channel_id
    channel.channel_handle = 'parent'
    channel.channel_links = links
    return channel


class TestResolveDiscoveredLinkId(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_bare_channel_id_passes_through(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        self.assertEqual(
            await _resolve_discovered_link_id(
                'UCa1a1a1a1a1a1a1a1a1a1a1', None,
            ),
            'UCa1a1a1a1a1a1a1a1a1a1a1',
        )

    async def test_channel_path_is_unwrapped(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        self.assertEqual(
            await _resolve_discovered_link_id(
                'channel/UCa1a1a1a1a1a1a1a1a1a1a1', None,
            ),
            'UCa1a1a1a1a1a1a1a1a1a1a1',
        )

    async def test_handle_resolves_via_innertube(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_handle',
            new_callable=AsyncMock,
            return_value='UCb1b1b1b1b1b1b1b1b1b1b1',
        ) as mock_resolve:
            self.assertEqual(
                await _resolve_discovered_link_id(
                    'somehandle', None,
                ),
                'UCb1b1b1b1b1b1b1b1b1b1b1',
            )
        mock_resolve.assert_awaited_once_with('somehandle')

    async def test_identity_map_hit_avoids_innertube(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        identity: AsyncMock = AsyncMock()
        identity.handle_map.get.return_value = (
            'UCc1c1c1c1c1c1c1c1c1c1c1'
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_handle',
            new_callable=AsyncMock,
        ) as mock_resolve:
            self.assertEqual(
                await _resolve_discovered_link_id(
                    'somehandle', identity,
                ),
                'UCc1c1c1c1c1c1c1c1c1c1c1',
            )
        mock_resolve.assert_not_awaited()

    async def test_invalid_handle_returns_none(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_handle',
            new_callable=AsyncMock,
        ) as mock_resolve:
            self.assertIsNone(
                await _resolve_discovered_link_id(
                    'not a handle!', None,
                ),
            )
        mock_resolve.assert_not_awaited()

    async def test_resolution_error_returns_none(self) -> None:
        from tools.yt_channel_scrape import (
            _resolve_discovered_link_id,
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_handle',
            new_callable=AsyncMock,
            side_effect=RuntimeError('innertube down'),
        ):
            self.assertIsNone(
                await _resolve_discovered_link_id(
                    'somehandle', None,
                ),
            )


class TestEnqueueDiscoveredChannelLinks(
    unittest.IsolatedAsyncioTestCase,
):

    async def _run(
        self,
        links: list[YouTubeChannelLink],
        *,
        settings: MagicMock | None = None,
        creator_map_value: str | None = None,
        exists: bool | None = False,
    ) -> AsyncMock:
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = creator_map_value
        http_client: MagicMock = MagicMock()
        with (
            patch(
                'tools.yt_channel_scrape.resolve_channel_handle',
                new_callable=AsyncMock,
                return_value='UCb1b1b1b1b1b1b1b1b1b1b1',
            ),
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock,
                return_value=exists,
            ),
        ):
            from tools.yt_channel_scrape import (
                _enqueue_discovered_channel_links,
            )
            await _enqueue_discovered_channel_links(
                _mock_channel(links),
                queue=queue,
                settings=settings or _discovery_settings(),
                creator_map_backend=creator_map,
                http_client=http_client,
                identity=None,
            )
        return queue

    async def test_new_link_is_enqueued(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 500)],
        )
        queue.enqueue_scheduled.assert_awaited_once_with(
            'UCb1b1b1b1b1b1b1b1b1b1b1',
            source='discovered_link',
        )

    async def test_no_links_no_enqueue(self) -> None:
        queue: AsyncMock = await self._run([])
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_disabled_setting_no_enqueue(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 500)],
            settings=_discovery_settings(
                channel_discover_linked_channels=False,
            ),
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_link_below_min_subscribers_skipped(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 5)],
            settings=_discovery_settings(
                channel_discovery_min_subscribers=100,
            ),
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_self_link_skipped(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink(
                'UCe1e1e1e1e1e1e1e1e1e1e1', 500,
            )],
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_already_scraped_link_skipped(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 500)],
            creator_map_value='somenewchannel',
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_link_on_exchange_skipped(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 500)],
            exists=True,
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_exchange_set_hit_skips_api_check(self) -> None:
        '''A ``youtube:exchange_channels`` set hit answers the
        existence check from Redis; the exchange API must not be
        contacted and the link must not be enqueued.'''
        queue: AsyncMock = AsyncMock()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = None
        link_id: str = 'UCb1b1b1b1b1b1b1b1b1'
        http_client: MagicMock = MagicMock()
        exchange_set: AsyncMock = AsyncMock()
        exchange_set.contains_many.return_value = {link_id: True}
        with (
            patch(
                'tools.yt_channel_scrape.resolve_channel_handle',
                new_callable=AsyncMock,
                return_value=link_id,
            ),
            patch(
                'tools.yt_channel_scrape'
                '.RedisExchangeChannelsSet',
                return_value=exchange_set,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock,
            ) as exists_mock,
        ):
            from tools.yt_channel_scrape import (
                _enqueue_discovered_channel_links,
            )
            await _enqueue_discovered_channel_links(
                _mock_channel([
                    YouTubeChannelLink('somenewchannel', 500),
                ]),
                queue=queue,
                settings=_discovery_settings(),
                creator_map_backend=creator_map,
                http_client=http_client,
                identity=None,
            )
        exchange_set.contains_many.assert_awaited_once_with(
            [link_id],
        )
        exists_mock.assert_not_awaited()
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_exchange_check_failure_skips_enqueue(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('somenewchannel', 500)],
            exists=None,
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_unresolvable_link_skipped(self) -> None:
        queue: AsyncMock = await self._run(
            [YouTubeChannelLink('not a handle!', 500)],
        )
        queue.enqueue_scheduled.assert_not_awaited()

    async def test_mixed_links_enqueue_only_new(self) -> None:
        with patch(
            'tools.yt_channel_scrape.resolve_channel_handle',
            new_callable=AsyncMock,
            side_effect=lambda handle: {
                'newchannel': 'UCb1b1b1b1b1b1b1b1b1b1b1',
                'existing': 'UCc1c1c1c1c1c1c1c1c1c1c1',
                'tiny': 'UCd1d1d1d1d1d1d1d1d1d1d1',
                'failed': None,
            }.get(handle),
        ), patch(
            'tools.yt_channel_scrape'
            '._channel_exists_on_exchange',
            new_callable=AsyncMock,
            side_effect=(
                lambda _client, _url, channel_id: channel_id != (
                    'UCb1b1b1b1b1b1b1b1b1b1b1'
                )
            ),
        ):
            from tools.yt_channel_scrape import (
                _enqueue_discovered_channel_links,
            )
            queue: AsyncMock = AsyncMock()
            creator_map: AsyncMock = AsyncMock()
            creator_map.get.return_value = None
            links: list[YouTubeChannelLink] = [
                # new and missing on the exchange: enqueued
                YouTubeChannelLink('newchannel', 1000),
                # on the exchange: skipped
                YouTubeChannelLink('existing', 1000),
                # below min subs: skipped
                YouTubeChannelLink('tiny', 5),
                # unresolvable: skipped
                YouTubeChannelLink('failed', 1000),
            ]
            await _enqueue_discovered_channel_links(
                _mock_channel(links),
                queue=queue,
                settings=_discovery_settings(
                    channel_discovery_min_subscribers=100,
                ),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
                identity=None,
            )
        queue.enqueue_scheduled.assert_awaited_once_with(
            'UCb1b1b1b1b1b1b1b1b1b1b1',
            source='discovered_link',
        )

    async def test_summary_log_reports_found_new_known(self) -> None:
        with (
            patch(
                'tools.yt_channel_scrape.resolve_channel_handle',
                new_callable=AsyncMock,
                side_effect=lambda handle: {
                    'newchannel': 'UCb1b1b1b1b1b1b1b1b1b1',
                    'existing': 'UCc1c1c1c1c1c1c1c1c1c1',
                }.get(handle),
            ),
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock,
                side_effect=(
                    lambda _client, _url, channel_id: channel_id == (
                        'UCc1c1c1c1c1c1c1c1c1c1'
                    )
                ),
            ),
            self.assertLogs(level='INFO') as captured,
        ):
            from tools.yt_channel_scrape import (
                _enqueue_discovered_channel_links,
            )
            queue: AsyncMock = AsyncMock()
            creator_map: AsyncMock = AsyncMock()
            creator_map.get.return_value = None
            await _enqueue_discovered_channel_links(
                _mock_channel([
                    YouTubeChannelLink('newchannel', 1000),
                    YouTubeChannelLink('existing', 1000),
                ]),
                queue=queue,
                settings=_discovery_settings(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
                identity=None,
            )
        summaries: list = [
            record for record in captured.records
            if record.getMessage() == (
                'discovery: channel link summary'
            )
        ]
        self.assertEqual(len(summaries), 1)
        record = summaries[0]
        self.assertEqual(record.links_found, 2)
        self.assertEqual(record.links_enqueued, 1)
        self.assertEqual(record.links_known, 1)
        self.assertEqual(record.outcome_enqueued, 1)
        self.assertEqual(record.outcome_on_exchange, 1)


class TestDiscoveryInScrapeOne(
    unittest.IsolatedAsyncioTestCase,
):

    def setUp(self) -> None:
        # Reuse the queue_channel_videos isolation.
        self.patcher = patch(
            'tools.yt_channel_scrape.queue_channel_videos',
            new_callable=AsyncMock,
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    async def test_scrape_one_enqueues_discovered_links(self) -> None:
        from tests.unit.test_yt_channel_scrape_scrape_phase import (
            _mock_settings,
        )
        from tools.yt_channel_scrape import _scrape_one_queued

        channel: MagicMock = MagicMock()
        channel.subscriber_count = 1_000_000
        channel.video_count = 1
        channel.video_ids = {'video-id'}
        channel.channel_id = 'UCa1a1a1a1a1a1a1a1a1a1a1'
        channel.channel_handle = 'channel'
        channel.title = 'Channel'
        channel.channel_links = [
            YouTubeChannelLink('somenewchannel', 500),
        ]
        settings: MagicMock = _mock_settings()
        settings.channel_discover_linked_channels = True
        settings.channel_discovery_min_subscribers = 0
        queue: AsyncMock = AsyncMock()
        queue.get_scrape_progress.return_value = MagicMock(
            full_scrape_due=False,
        )
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = None
        with (
            patch(
                'tools.yt_channel_scrape'
                '._do_scrape_channel_to_disk_typed',
                new_callable=AsyncMock,
                return_value=channel,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._enqueue_discovered_channel_links',
                new_callable=AsyncMock,
            ) as mock_discovery,
        ):
            await _scrape_one_queued(
                'UCa1a1a1a1a1a1a1a1a1a1a1',
                queue=queue,
                settings=settings,
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
            )

        mock_discovery.assert_awaited_once()
        call = mock_discovery.await_args
        assert call is not None
        self.assertEqual(call.args[0], channel)
        self.assertEqual(call.kwargs['queue'], queue)

    async def test_discovery_failure_does_not_fail_scrape(self) -> None:
        from tests.unit.test_yt_channel_scrape_scrape_phase import (
            _mock_settings,
        )
        from tools.yt_channel_scrape import _scrape_one_queued

        channel: MagicMock = MagicMock()
        channel.subscriber_count = 1_000_000
        channel.video_count = 1
        channel.video_ids = {'video-id'}
        channel.channel_id = 'UCa1a1a1a1a1a1a1a1a1a1a1'
        channel.channel_handle = 'channel'
        channel.title = 'Channel'
        channel.channel_links = []
        settings: MagicMock = _mock_settings()
        settings.channel_discover_linked_channels = True
        settings.channel_discovery_min_subscribers = 0
        queue: AsyncMock = AsyncMock()
        queue.get_scrape_progress.return_value = MagicMock(
            full_scrape_due=False,
        )
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = None
        with (
            patch(
                'tools.yt_channel_scrape'
                '._do_scrape_channel_to_disk_typed',
                new_callable=AsyncMock,
                return_value=channel,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._channel_exists_on_exchange',
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                'tools.yt_channel_scrape'
                '._enqueue_discovered_channel_links',
                new_callable=AsyncMock,
                side_effect=RuntimeError('discovery exploded'),
            ),
        ):
            await _scrape_one_queued(
                'UCa1a1a1a1a1a1a1a1a1a1a1',
                queue=queue,
                settings=settings,
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
            )

        queue.update_tier.assert_awaited_once()
        queue.mark_soft_unavailable.assert_not_awaited()