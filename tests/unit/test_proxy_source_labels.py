'''
Split-pool children label each proxy with the file it came from, not
with 'env': the supervisor passes the labels in PROXY_SOURCES next to
the PROXIES slice.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scrape_exchange import scraper_supervisor
from scrape_exchange.proxy_file_watcher import ProxyFileWatcher
from scrape_exchange.proxy_loader import (
    PROXY_SOURCES_ENV_VAR,
    ProxyCatalog,
    load_proxy_catalog,
    load_proxy_entries,
    proxy_file_label,
    proxy_sources_env,
    set_active_catalog,
)
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    _ChildSlot,
    spawn_children,
)

VPN: list[str] = ['http://vpn1.test:3128', 'http://vpn2.test:3128']
SELLER: list[str] = ['http://seller1.test:3128', 'http://seller2.test:3128']


def _write(path: Path, proxies: list[str]) -> None:
    tmp: Path = path.with_suffix('.tmp')
    tmp.write_text('\n'.join(proxies) + '\n', encoding='utf-8')
    os.replace(tmp, path)


class _CatalogCase(unittest.TestCase):
    '''Loads vpn.proxies.lst and proxy-seller.proxies.lst as the
    active catalog, as the supervisor does at startup.'''

    def setUp(self) -> None:
        tmp: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir: Path = Path(tmp.name)
        self.vpn: Path = self.dir / 'vpn.proxies.lst'
        self.seller: Path = self.dir / 'proxy-seller.proxies.lst'
        _write(self.vpn, VPN)
        _write(self.seller, SELLER)
        self.addCleanup(set_active_catalog, ProxyCatalog())
        set_active_catalog(load_proxy_catalog([self.vpn, self.seller]))


class TestLoadProxyEntriesLabels(unittest.TestCase):

    def tearDown(self) -> None:
        set_active_catalog(ProxyCatalog())

    def test_labels_are_used(self) -> None:
        catalog: ProxyCatalog = load_proxy_entries(
            [VPN[0], SELLER[0]],
            labels=['vpn.proxies', 'proxy-seller.proxies'],
        )
        self.assertEqual(catalog.source[VPN[0]], 'vpn.proxies')
        self.assertEqual(catalog.source[SELLER[0]], 'proxy-seller.proxies')

    def test_without_labels_falls_back_to_env(self) -> None:
        catalog: ProxyCatalog = load_proxy_entries([VPN[0]])
        self.assertEqual(catalog.source[VPN[0]], 'env')

    def test_mismatched_labels_fall_back_to_env(self) -> None:
        with self.assertLogs('scrape_exchange.proxy_loader', 'WARNING'):
            catalog: ProxyCatalog = load_proxy_entries(
                [VPN[0], SELLER[0]], labels=['vpn.proxies'],
            )
        self.assertEqual(catalog.source[VPN[0]], 'env')
        self.assertEqual(catalog.source[SELLER[0]], 'env')

    def test_empty_label_falls_back_to_env(self) -> None:
        catalog: ProxyCatalog = load_proxy_entries(
            [VPN[0], SELLER[0]], labels=['vpn.proxies', ''],
        )
        self.assertEqual(catalog.source[VPN[0]], 'vpn.proxies')
        self.assertEqual(catalog.source[SELLER[0]], 'env')


class TestProxySourcesEnv(_CatalogCase):

    def test_labels_follow_the_active_catalog(self) -> None:
        self.assertEqual(
            proxy_sources_env([SELLER[1], VPN[0]]),
            'proxy-seller.proxies,vpn.proxies',
        )

    def test_unknown_proxy_is_none(self) -> None:
        self.assertEqual(
            proxy_sources_env(['http://other.test:3128']), 'none',
        )


class TestSplitChildrenKeepSourceLabels(_CatalogCase):

    def _config(self) -> SupervisorConfig:
        return SupervisorConfig(
            scraper_label='test',
            num_processes_env_var='TEST_NUM_PROCESSES',
            num_processes=2,
            concurrency=1,
            proxies=VPN + SELLER,
            metrics_port=0,
            log_file=None,
            split_proxy_pool=True,
        )

    def _spawn(self, chunks: list[list[str]]) -> list[_ChildSlot]:
        with patch.object(
            scraper_supervisor, '_spawn_one_slot', lambda slot: None,
        ), patch.object(scraper_supervisor.time, 'sleep'):
            return spawn_children(self._config(), chunks)

    def test_spawn_passes_sources(self) -> None:
        slots: list[_ChildSlot] = self._spawn(
            [[VPN[0], SELLER[0]], [SELLER[1], VPN[1]]],
        )
        self.assertEqual(
            slots[0].spawn_env[PROXY_SOURCES_ENV_VAR],
            'vpn.proxies,proxy-seller.proxies',
        )
        self.assertEqual(
            slots[1].spawn_env[PROXY_SOURCES_ENV_VAR],
            'proxy-seller.proxies,vpn.proxies',
        )

    def test_child_settings_label_by_file(self) -> None:
        from scrape_exchange.settings import ScraperSettings
        slots: list[_ChildSlot] = self._spawn(
            [[VPN[0], SELLER[0]], [SELLER[1], VPN[1]]],
        )
        env: dict[str, str] = slots[1].spawn_env
        # The child starts with an empty catalog of its own.
        set_active_catalog(ProxyCatalog())
        with patch.dict(os.environ, {
            'PROXY_FILES': env['PROXY_FILES'],
            'PROXIES': env['PROXIES'],
            PROXY_SOURCES_ENV_VAR: env[PROXY_SOURCES_ENV_VAR],
        }):
            settings: ScraperSettings = ScraperSettings(
                _env_file=None, _cli_parse_args=False,
            )
        self.assertEqual(list(settings.proxies), [SELLER[1], VPN[1]])
        self.assertEqual(
            proxy_file_label(SELLER[1]), 'proxy-seller.proxies',
        )
        self.assertEqual(proxy_file_label(VPN[1]), 'vpn.proxies')

    def test_reload_updates_sources(self) -> None:
        slots: list[_ChildSlot] = self._spawn([[VPN[0]], [SELLER[0]]])
        added: str = 'http://seller3.test:3128'
        _write(self.seller, SELLER + [added])
        watcher: ProxyFileWatcher = ProxyFileWatcher(
            [self.vpn, self.seller], VPN + SELLER,
        )
        new_proxies: list[str] | None = watcher.check()
        self.assertIsNotNone(new_proxies)
        # The watcher refreshes the catalog, so the added proxy is
        # labelled by its file.
        self.assertEqual(proxy_file_label(added), 'proxy-seller.proxies')
        scraper_supervisor._schedule_proxy_reload(
            'test', slots, new_proxies, split_proxy_pool=True, now=0.0,
        )
        for slot in slots:
            proxies: list[str] = slot.spawn_env['PROXIES'].split(',')
            sources: list[str] = (
                slot.spawn_env[PROXY_SOURCES_ENV_VAR].split(',')
            )
            self.assertEqual(
                sources, [proxy_file_label(p) for p in proxies],
            )
            self.assertNotIn('none', sources)


if __name__ == '__main__':
    unittest.main()
