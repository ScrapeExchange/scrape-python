'''
Unit tests for TikTokSessionJar.
'''

import json
import time
import tempfile
import unittest
from pathlib import Path

from scrape_exchange.tiktok.tiktok_session_jar import (
    SessionRecord,
    TikTokSessionJar,
)


class TestTikTokSessionJar(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: Path = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_set_and_get_token(self) -> None:
        jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir),
            ttl_seconds=3600,
        )
        jar.set_token('1.2.3.4', 'tok_abc')
        rec: SessionRecord | None = jar.get('1.2.3.4')
        self.assertIsNotNone(rec)
        self.assertEqual(rec.ms_token, 'tok_abc')
        self.assertFalse(rec.is_expired())

    def test_expiry_via_ttl(self) -> None:
        jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir),
            ttl_seconds=1,
        )
        jar.set_token('1.2.3.4', 'tok_abc')
        rec: SessionRecord = jar.get('1.2.3.4')
        # Pretend the token was captured 5 s ago
        rec.captured_at = time.time() - 5
        self.assertTrue(rec.is_expired())

    def test_persists_across_instances(self) -> None:
        jar_a: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir), ttl_seconds=3600,
        )
        jar_a.set_token('1.2.3.4', 'tok_persist')
        jar_a.flush()

        jar_b: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir), ttl_seconds=3600,
        )
        rec: SessionRecord | None = jar_b.get('1.2.3.4')
        self.assertIsNotNone(rec)
        self.assertEqual(rec.ms_token, 'tok_persist')

    def test_per_proxy_profile_dir(self) -> None:
        jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir), ttl_seconds=3600,
        )
        d: Path = jar.profile_dir('1.2.3.4')
        self.assertTrue(d.is_dir())
        self.assertEqual(d.parent, self.state_dir / 'profiles')

    def test_disk_layout(self) -> None:
        jar: TikTokSessionJar = TikTokSessionJar(
            state_dir=str(self.state_dir), ttl_seconds=3600,
        )
        jar.set_token('1.2.3.4', 'tok_disk')
        jar.flush()
        store: Path = self.state_dir / 'tokens.json'
        self.assertTrue(store.is_file())
        contents: dict = json.loads(store.read_text())
        self.assertEqual(
            contents['1.2.3.4']['ms_token'], 'tok_disk',
        )


if __name__ == '__main__':
    unittest.main()
