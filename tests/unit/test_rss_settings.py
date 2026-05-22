'''
Unit tests for the RssSettings class in
tools/yt_rss_scrape.py.
'''

import importlib.util
import os
import unittest

from pathlib import Path
from types import ModuleType


def _load_yt_rss_scrape() -> ModuleType:
    '''Load tools/yt_rss_scrape.py as a module.

    ``tools/`` is not a Python package; load it
    directly from its file path. Cached in
    ``sys.modules`` so re-imports don't re-register
    Prometheus metrics.
    '''

    import sys
    for _key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        if _key in sys.modules:
            return sys.modules[_key]

    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_rss_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert (
        spec is not None
        and spec.loader is not None
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()
RssSettings = yt_rss_scrape.RssSettings


class TestEligibilityFractionSetting(unittest.TestCase):

    def setUp(self) -> None:
        # Strip env vars that might bleed in from the
        # developer's shell or .env file.
        for key in (
            'RSS_ELIGIBILITY_FRACTION',
            'eligibility_fraction',
        ):
            os.environ.pop(key, None)

    def test_default_is_half(self) -> None:
        s: RssSettings = RssSettings(
            _env_file=None,
            _cli_parse_args=[],
        )
        self.assertEqual(s.eligibility_fraction, 0.5)

    def test_env_var_override(self) -> None:
        os.environ['RSS_ELIGIBILITY_FRACTION'] = '0.7'
        try:
            s: RssSettings = RssSettings(
                _env_file=None,
                _cli_parse_args=[],
            )
            self.assertEqual(
                s.eligibility_fraction, 0.7,
            )
        finally:
            del os.environ[
                'RSS_ELIGIBILITY_FRACTION'
            ]

    def test_invalid_value_raises(self) -> None:
        '''Values outside (0, 1] must be rejected.'''
        import pydantic
        for bad in ('0.0', '-0.1', '1.5', '2.0'):
            os.environ['RSS_ELIGIBILITY_FRACTION'] = (
                bad
            )
            try:
                with self.assertRaises(
                    pydantic.ValidationError,
                ):
                    RssSettings(
                        _env_file=None,
                        _cli_parse_args=[],
                    )
            finally:
                del os.environ[
                    'RSS_ELIGIBILITY_FRACTION'
                ]


class TestRssProxyFilesOverride(unittest.TestCase):
    '''``RSS_PROXY_FILES`` must override ``PROXY_FILES`` for the
    RSS scraper's proxy catalog without affecting the base
    ``proxy_files`` / inherited fields. The video and channel
    scrapers, which read ``settings.proxies`` via
    ``ScraperSettings._load_proxy_catalog``, are unaffected
    because they don't read ``rss_proxy_files``.'''

    def setUp(self) -> None:
        for key in (
            'RSS_PROXY_FILES', 'rss_proxy_files',
            'PROXY_FILES', 'proxy_files',
        ):
            os.environ.pop(key, None)
        import tempfile
        self._tmp: Path = Path(
            tempfile.mkdtemp(prefix='rss_proxy_files_'),
        )
        self._base_file: Path = (
            self._tmp / 'base.proxies.lst'
        )
        self._base_file.write_text(
            'http://baseuser:basepass@10.0.0.1:8080\n'
            'http://baseuser:basepass@10.0.0.2:8080\n'
        )
        self._rss_file: Path = (
            self._tmp / 'rss.proxies.lst'
        )
        self._rss_file.write_text(
            'http://rssuser:rsspass@10.1.0.1:8080\n'
        )

    def tearDown(self) -> None:
        for key in (
            'RSS_PROXY_FILES', 'rss_proxy_files',
            'PROXY_FILES', 'proxy_files',
        ):
            os.environ.pop(key, None)
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_default_is_none(self) -> None:
        s: RssSettings = RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertIsNone(s.rss_proxy_files)

    def test_inherits_proxy_files_when_unset(self) -> None:
        os.environ['PROXY_FILES'] = str(self._base_file)
        s: RssSettings = RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(len(s.proxies), 2)
        self.assertIn(
            'http://baseuser:basepass@10.0.0.1:8080',
            s.proxies,
        )

    def test_override_replaces_proxy_catalog(self) -> None:
        os.environ['PROXY_FILES'] = str(self._base_file)
        os.environ['RSS_PROXY_FILES'] = str(self._rss_file)
        s: RssSettings = RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(len(s.proxies), 1)
        self.assertIn(
            'http://rssuser:rsspass@10.1.0.1:8080',
            s.proxies,
        )
        self.assertNotIn(
            'http://baseuser:basepass@10.0.0.1:8080',
            s.proxies,
        )

    def test_override_with_no_base_proxy_files(self) -> None:
        os.environ['RSS_PROXY_FILES'] = str(self._rss_file)
        s: RssSettings = RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(len(s.proxies), 1)
        self.assertIn(
            'http://rssuser:rsspass@10.1.0.1:8080',
            s.proxies,
        )

    def test_override_supports_comma_separated(self) -> None:
        second: Path = self._tmp / 'rss2.proxies.lst'
        second.write_text(
            'http://rssuser:rsspass@10.1.0.2:8080\n',
        )
        os.environ['RSS_PROXY_FILES'] = (
            f'{self._rss_file},{second}'
        )
        s: RssSettings = RssSettings(
            _env_file=None, _cli_parse_args=[],
        )
        self.assertEqual(len(s.proxies), 2)
