'''Unit tests for channel_id-based filename helpers.'''

import unittest
from pathlib import Path
from unittest import mock

from tools.yt_channel_scrape import (
    get_channel_filename,
    _channel_id_from_filename,
    _persisted_channel_id_or_fail,
)


class TestChannelIdFilenames(unittest.TestCase):

    def test_get_channel_filename_uses_channel_id(self) -> None:
        cid: str = 'UCabcdefghijklmnopqrstuv'
        self.assertEqual(
            get_channel_filename(cid),
            f'channel-{cid}.json.br',
        )

    def test_channel_id_from_filename_round_trips(self) -> None:
        cid: str = 'UCabcdefghijklmnopqrstuv'
        self.assertEqual(
            _channel_id_from_filename(get_channel_filename(cid)),
            cid,
        )

    def test_channel_id_from_rss_filename(self) -> None:
        cid: str = 'UCabcdefghijklmnopqrstuv'
        self.assertEqual(
            _channel_id_from_filename(
                f'channel-rss-{cid}.json.br',
            ),
            f'rss-{cid}',
        )


class TestNoIdGuard(unittest.TestCase):

    def test_returns_channel_id_when_present(self) -> None:
        self.assertEqual(
            _persisted_channel_id_or_fail(
                'UCabcdefghijklmnopqrstuv', extra={},
            ),
            'UCabcdefghijklmnopqrstuv',
        )

    def test_returns_none_and_bumps_metric_when_empty(self) -> None:
        with mock.patch(
            'tools.yt_channel_scrape.METRIC_SCRAPE_FAILURES',
        ) as metric:
            result = _persisted_channel_id_or_fail('', extra={})
        self.assertIsNone(result)
        metric.labels.assert_called_once()
        self.assertEqual(
            metric.labels.call_args.kwargs['reason'],
            'no_channel_id',
        )
        metric.labels.return_value.inc.assert_called_once()


class TestLegacyMarkerKey(unittest.IsolatedAsyncioTestCase):

    async def test_not_found_marker_uses_channel_id(self) -> None:
        from tools import yt_channel_scrape as mod
        fm = mock.AsyncMock()
        channel = mock.Mock()
        channel.channel_id = 'UCabcdefghijklmnopqrstuv'
        with mock.patch.object(
            mod, '_try_scrape_channel_typed',
            side_effect=mod.ChannelNotFoundError('nope'),
        ):
            ok, _ = await mod._try_scrape_channel(
                channel, mock.Mock(), fm,
                'UCabcdefghijklmnopqrstuv',
                {'channel_id': 'UCabcdefghijklmnopqrstuv'},
            )
        self.assertFalse(ok)
        fm.mark_not_found.assert_awaited_once()
        marker_arg = fm.mark_not_found.await_args.args[0]
        self.assertEqual(
            marker_arg, 'channel-UCabcdefghijklmnopqrstuv',
        )


class TestBoundaryResolver(unittest.IsolatedAsyncioTestCase):

    def _identity_store(self, mapped: str | None) -> mock.Mock:
        store = mock.Mock()
        store.handle_map = mock.AsyncMock()
        store.handle_map.get.return_value = mapped
        return store

    def _fm(self, marker_exists: bool) -> mock.Mock:
        fm = mock.Mock()
        marker = mock.Mock()
        marker.exists.return_value = marker_exists
        fm.marker_path.return_value = marker
        fm.mark_not_found = mock.AsyncMock()
        return fm

    async def test_bare_id_passes_through_without_io(self) -> None:
        from tools import yt_channel_scrape as mod
        cid: str = 'UCabcdefghijklmnopqrstuv'
        store = self._identity_store(None)
        fm = self._fm(False)
        with mock.patch.object(
            mod, 'resolve_channel_handle',
            new=mock.AsyncMock(),
        ) as inner:
            result = await mod._resolve_handle_to_channel_id(
                cid, store, fm,
            )
        self.assertEqual(result, cid)
        store.handle_map.get.assert_not_awaited()
        inner.assert_not_awaited()

    async def test_handle_map_hit_returns_id(self) -> None:
        from tools import yt_channel_scrape as mod
        cid: str = 'UChandlemap0000000000000'
        store = self._identity_store(cid)
        fm = self._fm(False)
        with mock.patch.object(
            mod, 'resolve_channel_handle',
            new=mock.AsyncMock(),
        ) as inner:
            result = await mod._resolve_handle_to_channel_id(
                '@somehandle', store, fm,
            )
        self.assertEqual(result, cid)
        inner.assert_not_awaited()

    async def test_negative_cache_short_circuits(self) -> None:
        from tools import yt_channel_scrape as mod
        store = self._identity_store(None)
        fm = self._fm(True)  # .not_found marker present
        with mock.patch.object(
            mod, 'resolve_channel_handle',
            new=mock.AsyncMock(),
        ) as inner:
            result = await mod._resolve_handle_to_channel_id(
                '@somehandle', store, fm,
            )
        self.assertIsNone(result)
        inner.assert_not_awaited()
        fm.mark_not_found.assert_not_awaited()

    async def test_innertube_miss_writes_negative_marker(self) -> None:
        from tools import yt_channel_scrape as mod
        store = self._identity_store(None)
        fm = self._fm(False)
        with mock.patch.object(
            mod, 'resolve_channel_handle',
            new=mock.AsyncMock(return_value=None),
        ):
            result = await mod._resolve_handle_to_channel_id(
                '@somehandle', store, fm,
            )
        self.assertIsNone(result)
        fm.mark_not_found.assert_awaited_once()
        self.assertEqual(
            fm.mark_not_found.await_args.args[0],
            'channel-somehandle',
        )

    async def test_innertube_hit_returns_id(self) -> None:
        from tools import yt_channel_scrape as mod
        cid: str = 'UCinnertube000000000000a'
        store = self._identity_store(None)
        fm = self._fm(False)
        with mock.patch.object(
            mod, 'resolve_channel_handle',
            new=mock.AsyncMock(return_value=cid),
        ):
            result = await mod._resolve_handle_to_channel_id(
                '@somehandle', store, fm,
            )
        self.assertEqual(result, cid)
        fm.mark_not_found.assert_not_awaited()


class TestCandidateSelectionById(unittest.IsolatedAsyncioTestCase):

    async def test_select_via_set_checks_channel_ids(self) -> None:
        import fakeredis.aioredis
        from tools import yt_channel_scrape as mod
        from scrape_exchange.youtube.exchange_channels_set import (
            RedisExchangeChannelsSet,
        )
        member: str = 'UCmember000000000000000a'
        fresh: str = 'UCfresh0000000000000000a'
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        ex = RedisExchangeChannelsSet(redis)
        await ex.add_many([member])
        selected = await mod._select_new_channels_via_set(
            [member, fresh], ex, max_new_channels=10,
            already_resolved_count=0,
        )
        self.assertEqual(selected, {fresh})
        await redis.aclose()

    async def test_filter_resolves_to_ids_and_bounds_budget(
        self,
    ) -> None:
        from tools import yt_channel_scrape as mod
        ids: list[str] = [
            'UCaaaaaaaaaaaaaaaaaaaaaa',
            'UCbbbbbbbbbbbbbbbbbbbbbb',
            'UCcccccccccccccccccccccc',
        ]
        resolver_calls: list[str] = []

        async def fake_resolver(identifier, identity_store, fm):
            resolver_calls.append(identifier)
            return {'h1': ids[0], 'h2': ids[1], 'h3': ids[2]}[
                identifier
            ]

        fm = mock.Mock()
        fm.base_dir = Path('/tmp/scrape-test-x')
        fm.uploaded_dir = Path('/tmp/scrape-test-x/uploaded')
        marker = mock.Mock()
        marker.exists.return_value = False
        fm.marker_path.return_value = marker
        creator_map = mock.AsyncMock()
        creator_map.get.return_value = None
        with mock.patch.object(
            mod, '_resolve_handle_to_channel_id', new=fake_resolver,
        ), mock.patch.object(Path, 'exists', return_value=False):
            candidates = await mod._filter_unscraped_candidates(
                ['h1', 'h2', 'h3'], fm, set(),
                mock.Mock(), creator_map, max_candidates=2,
            )
        self.assertEqual(candidates, ids[:2])
        # Budget-bounded: only resolved until 2 candidates gathered.
        self.assertEqual(len(resolver_calls), 2)

    async def test_filter_uploaded_check_maps_id_to_handle(
        self,
    ) -> None:
        from tools import yt_channel_scrape as mod
        cid: str = 'UCuploaded00000000000000'
        fm = mock.Mock()
        fm.base_dir = Path('/tmp/scrape-test-y')
        fm.uploaded_dir = Path('/tmp/scrape-test-y/uploaded')
        marker = mock.Mock()
        marker.exists.return_value = False
        fm.marker_path.return_value = marker
        fm.mark_uploaded = mock.AsyncMock()
        creator_map = mock.AsyncMock()
        creator_map.get.return_value = '@uploadedhandle'

        async def fake_resolver(identifier, identity_store, fm_):
            return cid

        with mock.patch.object(
            mod, '_resolve_handle_to_channel_id', new=fake_resolver,
        ), mock.patch.object(Path, 'exists', return_value=False):
            candidates = await mod._filter_unscraped_candidates(
                ['@uploadedhandle'], fm,
                {'@uploadedhandle'},
                mock.Mock(), creator_map, max_candidates=5,
            )
        self.assertEqual(candidates, [])


class TestScrapeChannelById(unittest.IsolatedAsyncioTestCase):

    async def test_scrape_channel_takes_id_and_builds_id_filename(
        self,
    ) -> None:
        from tools import yt_channel_scrape as mod
        cid: str = 'UCabcdefghijklmnopqrstuv'
        settings = mock.Mock()
        fm = mock.Mock()
        fm.base_dir = Path('/tmp/scrape-test')
        creator_map = mock.AsyncMock()
        creator_map.get.return_value = None  # no handle known
        captured: dict[str, object] = {}

        async def fake_to_disk(
            settings_, fm_, channel_id_, filename_,
            creator_map_, extra_,
        ):
            captured['channel_id'] = channel_id_
            captured['filename'] = filename_
            return False, False, None

        with mock.patch.object(
            mod, '_scrape_channel_to_disk', new=fake_to_disk,
        ), mock.patch.object(
            mod, '_skip_due_to_existing_state',
            new=mock.AsyncMock(return_value=False),
        ):
            await mod.scrape_channel(settings, fm, cid, creator_map)
        self.assertEqual(captured['channel_id'], cid)
        self.assertEqual(
            captured['filename'], f'channel-{cid}.json.br',
        )


if __name__ == '__main__':
    unittest.main()
