"""Unit tests for scrape_exchange.proxy_loader entry parser."""

import io
import logging as stdlib_logging
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from scrape_exchange.proxy_loader import (
    ProxyCatalog,
    _parse_entry,
    load_proxy_catalog,
    proxy_file_label,
    set_active_catalog,
)


class TestParseEntry(unittest.TestCase):

    def test_url_with_auth_unchanged(self) -> None:
        result: str = _parse_entry('http://user:pass@10.0.0.1:7777')
        self.assertEqual(result, 'http://user:pass@10.0.0.1:7777')

    def test_url_no_auth_unchanged(self) -> None:
        result: str = _parse_entry('http://10.0.0.1:7777')
        self.assertEqual(result, 'http://10.0.0.1:7777')

    def test_https_url_unchanged(self) -> None:
        result: str = _parse_entry('https://10.0.0.1:7777')
        self.assertEqual(result, 'https://10.0.0.1:7777')

    def test_four_colon_form_canonicalized(self) -> None:
        result: str = _parse_entry('http://10.0.0.1:7777:user:pass')
        self.assertEqual(result, 'http://user:pass@10.0.0.1:7777')

    def test_four_colon_https_canonicalized(self) -> None:
        result: str = _parse_entry('https://10.0.0.1:7777:user:pass')
        self.assertEqual(result, 'https://user:pass@10.0.0.1:7777')

    def test_four_colon_password_with_colon_preserved(self) -> None:
        result: str = _parse_entry(
            'http://10.0.0.1:7777:user:pa:ss',
        )
        self.assertEqual(result, 'http://user:pa:ss@10.0.0.1:7777')

    def test_four_colon_empty_part_rejected(self) -> None:
        for entry in (
            'http://10.0.0.1:7777::pass',
            'http://10.0.0.1:7777:user:',
            'http://:7777:user:pass',
            'http://10.0.0.1::user:pass',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    _parse_entry(entry)

    def test_local_ipv4_unchanged(self) -> None:
        result: str = _parse_entry('local://192.0.2.5')
        self.assertEqual(result, 'local://192.0.2.5')

    def test_local_invalid_ip_rejected(self) -> None:
        for entry in (
            'local://0.0.0.0',
            'local://255.255.255.255',
            'local://224.0.0.1',
            'local://not-an-ip',
            'local://192.0.2',
            'local://',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    _parse_entry(entry)

    def test_local_ipv6_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _parse_entry('local://::1')

    def test_unknown_scheme_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _parse_entry('socks5://1.2.3.4:1080')

    def test_bare_host_port_gets_http_prefix(self) -> None:
        result: str = _parse_entry('1.2.3.4:8080')
        self.assertEqual(result, 'http://1.2.3.4:8080')

    def test_bare_4_colon_form_gets_http_prefix(self) -> None:
        result: str = _parse_entry('1.2.3.4:8080:user:pass')
        self.assertEqual(result, 'http://user:pass@1.2.3.4:8080')

    def test_bare_4_colon_password_with_colon_preserved(
        self,
    ) -> None:
        result: str = _parse_entry('1.2.3.4:8080:user:pa:ss')
        self.assertEqual(result, 'http://user:pa:ss@1.2.3.4:8080')

    def test_https_prefix_preserved_not_rewritten_to_http(
        self,
    ) -> None:
        result: str = _parse_entry('https://1.2.3.4:8080')
        self.assertEqual(result, 'https://1.2.3.4:8080')

    def test_non_numeric_port_rejected(self) -> None:
        for entry in (
            'http://10.0.0.1:abc',
            'http://10.0.0.1:abc:user:pass',
            'http://user:pass@10.0.0.1:abc',
            'https://10.0.0.1:port',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    _parse_entry(entry)

    def test_out_of_range_port_rejected(self) -> None:
        for entry in (
            'http://10.0.0.1:0',
            'http://10.0.0.1:65536',
            'http://user:pass@10.0.0.1:99999',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    _parse_entry(entry)

    def test_url_with_path_query_or_fragment_rejected(self) -> None:
        for entry in (
            'http://10.0.0.1:80/',
            'http://10.0.0.1:80/path',
            'http://10.0.0.1:80?x=1',
            'http://10.0.0.1:80#frag',
            'http://user:pass@10.0.0.1:80/path',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    _parse_entry(entry)


class TestLoadProxyCatalogSingleFile(unittest.TestCase):

    def _write(self, tmpdir: Path, name: str, body: str) -> Path:
        path: Path = tmpdir / name
        path.write_text(body)
        return path

    def test_reads_entries_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            f: Path = self._write(
                tmpdir, 'a.txt',
                'http://1.1.1.1:80\n'
                'http://2.2.2.2:80\n'
                'local://192.0.2.5\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([f])
        self.assertEqual(catalog.entries, [
            'http://1.1.1.1:80',
            'http://2.2.2.2:80',
            'local://192.0.2.5',
        ])
        self.assertEqual(
            catalog.source['local://192.0.2.5'], 'a',
        )

    def test_strips_comments_and_blank_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            f: Path = self._write(
                tmpdir, 'a.txt',
                '# header comment\n'
                '\n'
                '   # indented comment\n'
                'http://1.1.1.1:80   \n'
                '\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([f])
        self.assertEqual(catalog.entries, ['http://1.1.1.1:80'])

    def test_canonicalizes_4_colon_form(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            f: Path = self._write(
                tmpdir, 'a.txt',
                'http://1.1.1.1:80:user:pass\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([f])
        self.assertEqual(
            catalog.entries, ['http://user:pass@1.1.1.1:80'],
        )

    def test_dedupes_within_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            f: Path = self._write(
                tmpdir, 'a.txt',
                'http://1.1.1.1:80\n'
                'http://1.1.1.1:80\n'
                'http://1.1.1.1:80:user:pass\n'
                'http://user:pass@1.1.1.1:80\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([f])
        self.assertEqual(catalog.entries, [
            'http://1.1.1.1:80',
            'http://user:pass@1.1.1.1:80',
        ])

    def test_missing_file_hard_fails(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_proxy_catalog([Path('/nonexistent/x.txt')])

    def test_empty_file_hard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            f: Path = Path(tmp) / 'empty.txt'
            f.write_text('')
            with self.assertRaises(ValueError) as ctx:
                load_proxy_catalog([f])
        self.assertIn('empty', str(ctx.exception).lower())

    def test_only_comments_file_hard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            f: Path = Path(tmp) / 'comments.txt'
            f.write_text('# nothing else\n\n# more\n')
            with self.assertRaises(ValueError):
                load_proxy_catalog([f])

    def test_malformed_line_message_includes_location(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            f: Path = Path(tmp) / 'bad.txt'
            f.write_text(
                'http://1.1.1.1:80\n'
                'garbage\n',
            )
            with self.assertRaises(ValueError) as ctx:
                load_proxy_catalog([f])
        msg: str = str(ctx.exception)
        self.assertIn('bad.txt', msg)
        self.assertIn(':2', msg)
        self.assertIn('garbage', msg)


class TestLoadProxyCatalogTildeExpansion(unittest.TestCase):
    '''Tilde-prefixed paths should be expanded.

    Comma-separated CLI flags (``--proxy-files A,B,C``) only get
    shell-expanded for the first entry; everything after the first
    comma keeps its literal ``~`` because the rest is part of the
    same shell word. Pydantic-settings forwards those raw strings
    to ``load_proxy_catalog`` and the loader must do its own
    expansion or hard-fail with a confusing
    ``proxy file not found: ~/...`` error.
    '''

    def test_tilde_expanded_in_path(self) -> None:
        import os
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            real: Path = tmpdir / 'a.txt'
            real.write_text('http://1.1.1.1:80\n')
            with unittest.mock.patch.dict(
                os.environ, {'HOME': str(tmpdir)},
            ):
                catalog: ProxyCatalog = load_proxy_catalog(
                    [Path('~/a.txt')],
                )
        self.assertEqual(catalog.entries, ['http://1.1.1.1:80'])

    def test_tilde_expanded_in_string_path(self) -> None:
        '''pydantic-settings sometimes forwards str rather than
        Path; ``load_proxy_catalog`` must handle that too.'''
        import os
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            (tmpdir / 'b.txt').write_text('http://2.2.2.2:80\n')
            with unittest.mock.patch.dict(
                os.environ, {'HOME': str(tmpdir)},
            ):
                catalog: ProxyCatalog = load_proxy_catalog(
                    ['~/b.txt'],  # type: ignore[list-item]
                )
        self.assertEqual(catalog.entries, ['http://2.2.2.2:80'])


class TestLoadProxyCatalogMultiFile(unittest.TestCase):

    def _write(self, tmpdir: Path, name: str, body: str) -> Path:
        path: Path = tmpdir / name
        path.write_text(body, encoding='utf-8')
        return path

    def test_concatenates_files_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            a: Path = self._write(
                tmpdir, 'a.txt', 'http://1.1.1.1:80\n',
            )
            b: Path = self._write(
                tmpdir, 'b.txt', 'http://2.2.2.2:80\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([a, b])
        self.assertEqual(catalog.entries, [
            'http://1.1.1.1:80',
            'http://2.2.2.2:80',
        ])
        self.assertEqual(
            catalog.source['http://1.1.1.1:80'], 'a',
        )
        self.assertEqual(
            catalog.source['http://2.2.2.2:80'], 'b',
        )

    def test_first_file_wins_for_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir: Path = Path(tmp)
            a: Path = self._write(
                tmpdir, 'a.txt', 'http://1.1.1.1:80\n',
            )
            b: Path = self._write(
                tmpdir, 'b.txt', 'http://1.1.1.1:80\n',
            )
            catalog: ProxyCatalog = load_proxy_catalog([a, b])
        self.assertEqual(catalog.entries, ['http://1.1.1.1:80'])
        self.assertEqual(
            catalog.source['http://1.1.1.1:80'], 'a',
        )

    def test_duplicate_entry_logs_per_entry_warning(self) -> None:
        from scrape_exchange import proxy_loader
        buf: io.StringIO = io.StringIO()
        handler: stdlib_logging.Handler = (
            stdlib_logging.StreamHandler(buf)
        )
        proxy_loader._LOGGER.addHandler(handler)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                tmpdir: Path = Path(tmp)
                a: Path = self._write(
                    tmpdir, 'a.txt', 'http://1.1.1.1:80\n',
                )
                b: Path = self._write(
                    tmpdir, 'b.txt', 'http://1.1.1.1:80\n',
                )
                load_proxy_catalog([a, b])
        finally:
            proxy_loader._LOGGER.removeHandler(handler)
        out: str = buf.getvalue()
        # Per-entry warning naming both source files (or stems)
        self.assertIn('http://1.1.1.1:80', out)
        self.assertIn('a', out)
        self.assertIn('b', out)

    def test_duplicate_stem_hard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp1:
            with tempfile.TemporaryDirectory() as tmp2:
                a: Path = Path(tmp1) / 'shared.txt'
                a.write_text(
                    'http://1.1.1.1:80\n', encoding='utf-8',
                )
                b: Path = Path(tmp2) / 'shared.txt'
                b.write_text(
                    'http://2.2.2.2:80\n', encoding='utf-8',
                )
                with self.assertRaises(ValueError) as ctx:
                    load_proxy_catalog([a, b])
        self.assertIn('stem', str(ctx.exception).lower())

    def test_empty_paths_returns_empty_catalog_with_warning(
        self,
    ) -> None:
        from scrape_exchange import proxy_loader
        buf: io.StringIO = io.StringIO()
        handler: stdlib_logging.Handler = (
            stdlib_logging.StreamHandler(buf)
        )
        proxy_loader._LOGGER.addHandler(handler)
        try:
            catalog: ProxyCatalog = load_proxy_catalog([])
        finally:
            proxy_loader._LOGGER.removeHandler(handler)
        self.assertEqual(catalog.entries, [])
        self.assertIn('unset', buf.getvalue().lower())


class TestActiveCatalogLookup(unittest.TestCase):

    def setUp(self) -> None:
        # Reset the registry so tests don't leak.
        set_active_catalog(ProxyCatalog())

    def tearDown(self) -> None:
        set_active_catalog(ProxyCatalog())

    def test_label_lookup_after_set(self) -> None:
        catalog: ProxyCatalog = ProxyCatalog(
            entries=['http://1.1.1.1:80'],
            source={'http://1.1.1.1:80': 'hype'},
        )
        set_active_catalog(catalog)
        self.assertEqual(
            proxy_file_label('http://1.1.1.1:80'), 'hype',
        )

    def test_label_lookup_returns_none_for_unknown_entry(
        self,
    ) -> None:
        set_active_catalog(ProxyCatalog())
        self.assertEqual(
            proxy_file_label('http://1.1.1.1:80'), 'none',
        )


class TestJitterPoolWarmup(unittest.IsolatedAsyncioTestCase):
    '''The pool-warm-up jitter helper sleeps once per entry per
    process and is a no-op on subsequent calls. Used to spread
    cold-start CONNECT tunnels across a 0-3s window.'''

    def setUp(self) -> None:
        from scrape_exchange import proxy_loader
        proxy_loader._reset_warmup_for_tests()

    async def test_first_call_sleeps_within_window(self) -> None:
        from scrape_exchange.proxy_loader import (
            jitter_pool_warmup,
            POOL_WARMUP_MAX_SECONDS,
        )
        with unittest.mock.patch(
            'scrape_exchange.proxy_loader.random.uniform',
            return_value=0.42,
        ):
            slept: list[float] = []

            async def _fake_sleep(d: float) -> None:
                slept.append(d)

            with unittest.mock.patch(
                'scrape_exchange.proxy_loader.asyncio.sleep',
                side_effect=_fake_sleep,
            ):
                await jitter_pool_warmup('http://a:3128')
        self.assertEqual(slept, [0.42])
        self.assertGreaterEqual(POOL_WARMUP_MAX_SECONDS, 0.42)

    async def test_second_call_for_same_entry_does_not_sleep(
        self,
    ) -> None:
        from scrape_exchange.proxy_loader import (
            jitter_pool_warmup,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.proxy_loader.random.uniform',
            return_value=1.0,
        ), unittest.mock.patch(
            'scrape_exchange.proxy_loader.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_pool_warmup('http://a:3128')
            await jitter_pool_warmup('http://a:3128')
            await jitter_pool_warmup('http://a:3128')
        self.assertEqual(slept, [1.0])

    async def test_distinct_entries_each_sleep_once(
        self,
    ) -> None:
        from scrape_exchange.proxy_loader import (
            jitter_pool_warmup,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.proxy_loader.random.uniform',
            side_effect=[0.1, 0.2, 0.3],
        ), unittest.mock.patch(
            'scrape_exchange.proxy_loader.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            await jitter_pool_warmup('http://a:3128')
            await jitter_pool_warmup('http://b:3128')
            await jitter_pool_warmup('http://c:3128')
            # Repeats — must not consume more random.uniform
            # values nor add more sleep entries.
            await jitter_pool_warmup('http://a:3128')
            await jitter_pool_warmup('http://b:3128')
        self.assertEqual(slept, [0.1, 0.2, 0.3])

    async def test_concurrent_first_use_serializes(self) -> None:
        '''Two coroutines awaiting the same fresh entry at the
        same time must not both sleep — the lock serialises
        them and only the first does the work.'''
        from scrape_exchange.proxy_loader import (
            jitter_pool_warmup,
        )
        slept: list[float] = []

        async def _fake_sleep(d: float) -> None:
            slept.append(d)

        with unittest.mock.patch(
            'scrape_exchange.proxy_loader.random.uniform',
            return_value=0.5,
        ), unittest.mock.patch(
            'scrape_exchange.proxy_loader.asyncio.sleep',
            side_effect=_fake_sleep,
        ):
            import asyncio
            await asyncio.gather(
                jitter_pool_warmup('http://a:3128'),
                jitter_pool_warmup('http://a:3128'),
                jitter_pool_warmup('http://a:3128'),
            )
        self.assertEqual(slept, [0.5])


class TestPooledHttpxKeepaliveExpiry(unittest.TestCase):
    '''The pooled httpx client uses a long keep-alive expiry
    (httpx default is 5s) so idle gaps from rate-limit waiting
    and breaker cooldowns do not force every request to open a
    new CONNECT tunnel.'''

    def test_keepalive_expiry_is_300_seconds(self) -> None:
        '''Empirical probe on 2026-05-12 confirmed YouTube keeps
        a single connection alive across 1000 sequential requests
        with 300s gaps. The pool window is sized to that tested
        ceiling (raised from 120s, then trimmed back from 600s
        to stay safely under what the proxy provider is willing
        to hold open).'''
        from scrape_exchange.proxy_loader import (
            _POOLED_HTTPX_LIMITS,
        )
        self.assertEqual(
            _POOLED_HTTPX_LIMITS.keepalive_expiry, 300.0,
        )


if __name__ == '__main__':
    unittest.main()
