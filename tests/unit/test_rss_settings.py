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
    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']

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
