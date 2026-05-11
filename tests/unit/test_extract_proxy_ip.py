"""Unit tests for scrape_exchange.util.extract_proxy_ip,
including the local:// branch added for the PROXY_FILES feature."""

import unittest

from scrape_exchange.util import extract_proxy_ip


class TestExtractProxyIp(unittest.TestCase):
    """Existing http(s) URL parsing remains correct."""

    def test_http_url_returns_host(self) -> None:
        self.assertEqual(
            extract_proxy_ip('http://127.0.0.1:8080'), '127.0.0.1',
        )

    def test_http_url_with_auth_returns_host(self) -> None:
        self.assertEqual(
            extract_proxy_ip('http://user:pass@127.0.0.1:8080'),
            '127.0.0.1',
        )

    def test_http_url_no_port_returns_host(self) -> None:
        self.assertEqual(
            extract_proxy_ip('http://user:pass@127.0.0.1'),
            '127.0.0.1',
        )

    def test_https_url_returns_host(self) -> None:
        self.assertEqual(
            extract_proxy_ip('https://10.0.0.1:7777'), '10.0.0.1',
        )


class TestExtractProxyIpLocalScheme(unittest.TestCase):
    """The local:// branch added for native source-IP egress."""

    def test_local_returns_ipv4(self) -> None:
        self.assertEqual(
            extract_proxy_ip('local://192.0.2.5'), '192.0.2.5',
        )

    def test_local_strips_prefix_only(self) -> None:
        self.assertEqual(
            extract_proxy_ip('local://10.0.0.1'), '10.0.0.1',
        )

    def test_local_rejects_empty_payload(self) -> None:
        with self.assertRaises(ValueError):
            extract_proxy_ip('local://')

    def test_local_rejects_non_ipv4_payload(self) -> None:
        # local:// payload is trusted to have been validated by the
        # loader, but extract_proxy_ip's docstring promises to reject
        # invalid hosts. Defense in depth.
        for entry in (
            'local://::1',
            'local://not-an-ip',
            'local://192.0.2.5/path',
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    extract_proxy_ip(entry)


if __name__ == '__main__':
    unittest.main()
