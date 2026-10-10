'''Tests for scrape_exchange/browser_tmpdir.py.'''

import os
import tempfile
import unittest

from unittest.mock import patch

from scrape_exchange.browser_tmpdir import (
    browser_tmpdir_path,
    isolate_browser_tmpdir,
)


class TestBrowserTmpdir(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.base: str = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_path_is_scoped_to_scraper_and_worker(self) -> None:
        self.assertEqual(
            browser_tmpdir_path('instagram_creator', '2', self.base),
            os.path.join(self.base, 'scrape-browser-instagram_creator-2'),
        )

    def test_creates_dir_and_exports_tmpdir(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            path: str = isolate_browser_tmpdir('ig', '0', self.base)
            self.assertEqual(os.environ['TMPDIR'], path)
        self.assertTrue(os.path.isdir(path))
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o700)

    def test_wipes_profiles_left_by_previous_run(self) -> None:
        path: str = browser_tmpdir_path('ig', '1', self.base)
        leaked: str = os.path.join(
            path, 'playwright_firefoxdev_profile-abc123',
        )
        os.makedirs(leaked)
        with open(os.path.join(leaked, 'prefs.js'), 'w') as fd:
            fd.write('x')
        with patch.dict(os.environ, {}, clear=False):
            isolate_browser_tmpdir('ig', '1', self.base)
        self.assertTrue(os.path.isdir(path))
        self.assertEqual(os.listdir(path), [])

    def test_leaves_other_worker_slots_alone(self) -> None:
        sibling: str = browser_tmpdir_path('ig', '2', self.base)
        live: str = os.path.join(sibling, 'playwright_firefoxdev_profile-x')
        os.makedirs(live)
        with patch.dict(os.environ, {}, clear=False):
            isolate_browser_tmpdir('ig', '1', self.base)
        self.assertTrue(os.path.isdir(live))


if __name__ == '__main__':
    unittest.main()
