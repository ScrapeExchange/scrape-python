"""Tests that ScraperSettings reads PROXY_FILES, populates the
proxies attribute, and registers the active catalog."""

import os
import tempfile
import unittest

from pathlib import Path

from scrape_exchange.proxy_loader import (
    ProxyCatalog,
    proxy_file_label,
    set_active_catalog,
)
from scrape_exchange.settings import ScraperSettings


class TestProxyFilesSetting(unittest.TestCase):

    def setUp(self) -> None:
        # Reset registry between tests.
        set_active_catalog(ProxyCatalog())

    def tearDown(self) -> None:
        set_active_catalog(ProxyCatalog())
        for k in (
            'PROXY_FILES', 'PROXIES',
            'API_KEY_ID', 'API_KEY_SECRET',
        ):
            os.environ.pop(k, None)

    def test_proxy_files_loads_into_proxies_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            a: Path = tmpdir / 'hype.txt'
            a.write_text(
                'http://1.1.1.1:80\n', encoding='utf-8',
            )
            b: Path = tmpdir / 'local.txt'
            b.write_text(
                'local://192.0.2.5\n', encoding='utf-8',
            )
            os.environ['PROXY_FILES'] = f'{a},{b}'

            s: ScraperSettings = ScraperSettings(
                _cli_parse_args=False,
            )

        self.assertEqual(s.proxies, [
            'http://1.1.1.1:80',
            'local://192.0.2.5',
        ])
        self.assertEqual(
            proxy_file_label('http://1.1.1.1:80'), 'hype',
        )
        self.assertEqual(
            proxy_file_label('local://192.0.2.5'), 'local',
        )

    def test_unset_proxy_files_yields_empty_proxies(self) -> None:
        os.environ.pop('PROXY_FILES', None)
        s: ScraperSettings = ScraperSettings(
            _cli_parse_args=False,
        )
        self.assertEqual(s.proxies, [])

    def test_class_default_is_immutable(self) -> None:
        # Important: protect against the model_construct() hazard
        # where mutating settings.proxies on a freshly constructed
        # instance corrupts the class default.
        s: ScraperSettings = ScraperSettings.model_construct()
        self.assertEqual(s.proxies, ())
        with self.assertRaises(AttributeError):
            s.proxies.append('XYZ')

    def test_proxy_files_accepts_path_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p: Path = Path(tmp) / 'a.txt'
            p.write_text('http://1.1.1.1:80\n', encoding='utf-8')
            s: ScraperSettings = ScraperSettings(
                proxy_files=p, _cli_parse_args=False,
            )
        self.assertEqual(s.proxies, ['http://1.1.1.1:80'])

    def test_proxy_files_accepts_list_of_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            a: Path = tmpdir / 'a.txt'
            a.write_text('http://1.1.1.1:80\n', encoding='utf-8')
            b: Path = tmpdir / 'b.txt'
            b.write_text('http://2.2.2.2:80\n', encoding='utf-8')
            s: ScraperSettings = ScraperSettings(
                proxy_files=[a, b], _cli_parse_args=False,
            )
        self.assertEqual(s.proxies, [
            'http://1.1.1.1:80', 'http://2.2.2.2:80',
        ])


if __name__ == '__main__':
    unittest.main()
