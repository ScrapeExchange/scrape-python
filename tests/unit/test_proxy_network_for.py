'''
Unit tests for scrape_exchange.util.proxy_network_for —
the CIDR-range categoriser used to add a ``proxy_network``
label alongside the existing ``proxy_ip`` label on
Prometheus metrics.
'''

import os
import unittest

from unittest.mock import patch

from scrape_exchange.util import proxy_network_for


class TestProxyNetworkFor(unittest.TestCase):

    def _env(self, value: str) -> patch.dict:
        return patch.dict(
            os.environ,
            {'PROXY_NETWORKS': value},
            clear=False,
        )

    def test_none_proxy_returns_none(self) -> None:
        with self._env(''):
            self.assertEqual(
                proxy_network_for(None), 'none',
            )

    def test_sentinel_none_string_returns_none(self) -> None:
        with self._env('65.181.160.0/21'):
            self.assertEqual(
                proxy_network_for('none'), 'none',
            )

    def test_unconfigured_env_returns_other(self) -> None:
        '''When PROXY_NETWORKS is empty, any real IP
        ends up in "other".'''
        with self._env(''):
            self.assertEqual(
                proxy_network_for('65.181.160.5'), 'other',
            )

    def test_ip_inside_first_range(self) -> None:
        with self._env(
            '65.181.160.0/21,158.94.48.0/20',
        ):
            self.assertEqual(
                proxy_network_for('65.181.160.5'),
                '65.181.160.0/21',
            )

    def test_ip_inside_second_range(self) -> None:
        with self._env(
            '65.181.160.0/21,158.94.48.0/20',
        ):
            self.assertEqual(
                proxy_network_for('158.94.59.134'),
                '158.94.48.0/20',
            )

    def test_ip_outside_all_ranges(self) -> None:
        with self._env(
            '65.181.160.0/21,158.94.48.0/20',
        ):
            self.assertEqual(
                proxy_network_for('8.8.8.8'), 'other',
            )

    def test_single_host_cidr(self) -> None:
        '''A /32 range matches exactly one IP.'''
        with self._env('95.99.200.234/32'):
            self.assertEqual(
                proxy_network_for('95.99.200.234'),
                '95.99.200.234/32',
            )
            self.assertEqual(
                proxy_network_for('95.99.200.235'),
                'other',
            )

    def test_hostname_returns_other(self) -> None:
        '''Non-IP hostnames (e.g. socks5 proxy hosts) fall
        through to "other" rather than raising.'''
        with self._env('65.181.160.0/21'):
            self.assertEqual(
                proxy_network_for('proxy.example'),
                'other',
            )

    def test_invalid_cidr_skipped(self) -> None:
        '''A junk entry in PROXY_NETWORKS is logged and
        skipped; the valid entries still work.'''
        with self._env('not-a-cidr,65.181.160.0/21'):
            with self.assertLogs(
                'scrape_exchange.util', level='WARNING',
            ):
                self.assertEqual(
                    proxy_network_for('65.181.160.5'),
                    '65.181.160.0/21',
                )

    def test_whitespace_entries_tolerated(self) -> None:
        with self._env(
            ' 65.181.160.0/21 , 158.94.48.0/20 ',
        ):
            self.assertEqual(
                proxy_network_for('65.181.163.10'),
                '65.181.160.0/21',
            )

    def test_env_change_refreshes_cache(self) -> None:
        '''Changing PROXY_NETWORKS between calls picks up
        the new config without restart.'''
        with self._env('65.181.160.0/21'):
            self.assertEqual(
                proxy_network_for('158.94.59.134'),
                'other',
            )
        with self._env('158.94.48.0/20'):
            self.assertEqual(
                proxy_network_for('158.94.59.134'),
                '158.94.48.0/20',
            )

    def test_local_rfc1918_range(self) -> None:
        with self._env('192.168.1.0/24'):
            self.assertEqual(
                proxy_network_for('192.168.1.16'),
                '192.168.1.0/24',
            )
            self.assertEqual(
                proxy_network_for('192.168.2.1'),
                'other',
            )

    def test_order_sensitive_first_match_wins(self) -> None:
        '''Overlapping ranges resolve to the first one
        listed — users can put more-specific first if
        they want them to win.'''
        with self._env(
            '65.181.160.0/21,65.181.160.0/20',
        ):
            # Both contain the IP; the /21 is listed
            # first so it wins.
            self.assertEqual(
                proxy_network_for('65.181.160.5'),
                '65.181.160.0/21',
            )


if __name__ == '__main__':
    unittest.main()
