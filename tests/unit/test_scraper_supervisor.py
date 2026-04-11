'''
Unit tests for scrape_exchange/scraper_supervisor.py.

Exercises the pure helpers (``split_proxies``,
``chunks_are_disjoint_cover``, ``publish_config_metrics``) and the
pure bits of ``spawn_children``. The subprocess spawn itself is
patched so we can assert on the child env without actually
forking.
'''

import os
import unittest

from unittest.mock import patch, MagicMock

from scrape_exchange.scraper_supervisor import (
    METRIC_CONCURRENCY,
    METRIC_NUM_PROCESSES,
    SupervisorConfig,
    chunks_are_disjoint_cover,
    publish_config_metrics,
    spawn_children,
    split_proxies,
)


class TestSplitProxies(unittest.TestCase):

    def test_zero_or_negative_raises(self) -> None:
        with self.assertRaises(ValueError):
            split_proxies(['a', 'b'], 0)
        with self.assertRaises(ValueError):
            split_proxies(['a', 'b'], -1)

    def test_round_robin_even(self) -> None:
        chunks = split_proxies(['a', 'b', 'c', 'd'], 2)
        self.assertEqual(chunks, [['a', 'c'], ['b', 'd']])

    def test_round_robin_uneven(self) -> None:
        chunks = split_proxies(['a', 'b', 'c', 'd', 'e'], 2)
        self.assertEqual(chunks, [['a', 'c', 'e'], ['b', 'd']])

    def test_more_chunks_than_proxies_has_empty(self) -> None:
        chunks = split_proxies(['a', 'b'], 4)
        self.assertEqual(chunks, [['a'], ['b'], [], []])

    def test_preserves_input_order_within_chunks(self) -> None:
        chunks = split_proxies(['p1', 'p2', 'p3'], 3)
        self.assertEqual(chunks, [['p1'], ['p2'], ['p3']])


class TestChunksAreDisjointCover(unittest.TestCase):

    def test_disjoint_round_robin_split_is_ok(self) -> None:
        proxies: list[str] = ['a', 'b', 'c', 'd']
        chunks = split_proxies(proxies, 2)
        self.assertTrue(chunks_are_disjoint_cover(chunks, proxies))

    def test_overlap_rejected(self) -> None:
        proxies: list[str] = ['a', 'b', 'c']
        chunks = [['a', 'b'], ['b', 'c']]  # 'b' in both
        self.assertFalse(chunks_are_disjoint_cover(chunks, proxies))

    def test_drop_rejected(self) -> None:
        proxies: list[str] = ['a', 'b', 'c']
        chunks = [['a'], ['b']]  # 'c' missing
        self.assertFalse(chunks_are_disjoint_cover(chunks, proxies))

    def test_extra_rejected(self) -> None:
        proxies: list[str] = ['a', 'b']
        chunks = [['a'], ['b'], ['c']]
        self.assertFalse(chunks_are_disjoint_cover(chunks, proxies))

    def test_different_set_rejected(self) -> None:
        proxies: list[str] = ['a', 'b']
        chunks = [['x'], ['y']]
        self.assertFalse(chunks_are_disjoint_cover(chunks, proxies))


class TestPublishConfigMetrics(unittest.TestCase):

    def setUp(self) -> None:
        # Ensure the labels exist on the gauge so .labels(...)._value
        # has a defined value to inspect. The metric is process-wide,
        # so use unique scraper labels to avoid interference.
        self.scraper_label: str = 'pytest-scraper'

    def test_supervisor_gauges(self) -> None:
        publish_config_metrics(
            role='supervisor',
            scraper_label=self.scraper_label,
            num_processes=4, concurrency=3,
        )
        np_val: float = METRIC_NUM_PROCESSES.labels(
            role='supervisor', scraper=self.scraper_label,
        )._value.get()
        conc_val: float = METRIC_CONCURRENCY.labels(
            role='supervisor', scraper=self.scraper_label,
        )._value.get()
        self.assertEqual(np_val, 4.0)
        self.assertEqual(conc_val, 3.0)

    def test_worker_gauges_separate_from_supervisor(self) -> None:
        publish_config_metrics(
            role='supervisor',
            scraper_label=self.scraper_label,
            num_processes=4, concurrency=3,
        )
        publish_config_metrics(
            role='worker',
            scraper_label=self.scraper_label,
            num_processes=1, concurrency=3,
        )
        sup_np: float = METRIC_NUM_PROCESSES.labels(
            role='supervisor', scraper=self.scraper_label,
        )._value.get()
        w_np: float = METRIC_NUM_PROCESSES.labels(
            role='worker', scraper=self.scraper_label,
        )._value.get()
        self.assertEqual(sup_np, 4.0)
        self.assertEqual(w_np, 1.0)


class TestSpawnChildrenEnv(unittest.TestCase):
    '''
    Verify that spawn_children constructs the right per-child env
    without actually forking. We patch ``subprocess.Popen`` and
    capture the ``env`` kwarg for each invocation.
    '''

    def _make_config(
        self, log_file: str | None = None,
        log_file_env_var: str | None = 'CHANNEL_LOG_FILE',
    ) -> SupervisorConfig:
        return SupervisorConfig(
            scraper_label='channel',
            num_processes_env_var='CHANNEL_NUM_PROCESSES',
            num_processes=3,
            concurrency=5,
            proxies='http://a,http://b,http://c',
            metrics_port=9600,
            log_file=log_file,
            log_file_env_var=log_file_env_var,
        )

    def test_env_overrides_per_child(self) -> None:
        config: SupervisorConfig = self._make_config(
            log_file='/var/log/channel.log',
        )
        chunks: list[list[str]] = [
            ['http://a'], ['http://b'], ['http://c'],
        ]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 12345
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(len(captured_envs), 3)
        # Worker 1 → base + 1, log -1
        self.assertEqual(
            captured_envs[0]['CHANNEL_NUM_PROCESSES'], '1',
        )
        self.assertEqual(captured_envs[0]['PROXIES'], 'http://a')
        self.assertEqual(captured_envs[0]['METRICS_PORT'], '9601')
        self.assertEqual(
            captured_envs[0]['LOG_FILE'],
            '/var/log/channel-1.log',
        )
        # The scraper-specific log-file env var must also be
        # written so the child's pydantic settings don't resolve
        # the higher-priority alias back to the parent's base
        # path.
        self.assertEqual(
            captured_envs[0]['CHANNEL_LOG_FILE'],
            '/var/log/channel-1.log',
        )
        # Worker 2 → base + 2, log -2
        self.assertEqual(
            captured_envs[1]['METRICS_PORT'], '9602',
        )
        self.assertEqual(
            captured_envs[1]['LOG_FILE'],
            '/var/log/channel-2.log',
        )
        self.assertEqual(
            captured_envs[1]['CHANNEL_LOG_FILE'],
            '/var/log/channel-2.log',
        )
        # Worker 3 → base + 3, log -3
        self.assertEqual(
            captured_envs[2]['METRICS_PORT'], '9603',
        )
        self.assertEqual(
            captured_envs[2]['LOG_FILE'],
            '/var/log/channel-3.log',
        )
        self.assertEqual(
            captured_envs[2]['CHANNEL_LOG_FILE'],
            '/var/log/channel-3.log',
        )

    def test_dev_stdout_log_file_is_not_suffixed(self) -> None:
        '''
        ``/dev/stdout`` is a stream target; suffixing it would
        produce ``/dev/stdout-1`` which is nonsense. Each child
        must inherit the parent's stdout unchanged.
        '''
        config: SupervisorConfig = self._make_config(
            log_file='/dev/stdout',
        )
        chunks: list[list[str]] = [['http://a'], ['http://b']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_FILE', None)
            os.environ.pop('CHANNEL_LOG_FILE', None)
            with patch(
                'scrape_exchange.scraper_supervisor.subprocess.Popen',
                side_effect=fake_popen,
            ):
                spawn_children(config, chunks)

        for env in captured_envs:
            self.assertNotIn('LOG_FILE', env)
            self.assertNotIn('CHANNEL_LOG_FILE', env)

    def test_no_log_file_env_var_falls_back_to_base_log_file(
        self,
    ) -> None:
        '''
        A scraper without a scraper-specific log file env var
        (older integrations or tests) should still write the
        suffixed path to plain ``LOG_FILE``.
        '''
        config: SupervisorConfig = self._make_config(
            log_file='/var/log/channel.log',
            log_file_env_var=None,
        )
        chunks: list[list[str]] = [['http://a']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(
            captured_envs[0]['LOG_FILE'],
            '/var/log/channel-1.log',
        )
        self.assertNotIn('CHANNEL_LOG_FILE', captured_envs[0])

    def test_no_log_file_means_no_log_file_env_override(
        self,
    ) -> None:
        config: SupervisorConfig = self._make_config(log_file=None)
        chunks: list[list[str]] = [['http://a'], ['http://b']]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        # Ensure the parent env has no pre-existing LOG_FILE so we
        # can assert the child env doesn't pick it up from anywhere.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('LOG_FILE', None)
            with patch(
                'scrape_exchange.scraper_supervisor.subprocess.Popen',
                side_effect=fake_popen,
            ):
                spawn_children(config, chunks)

        self.assertNotIn('LOG_FILE', captured_envs[0])
        self.assertNotIn('LOG_FILE', captured_envs[1])

    def test_multi_proxy_chunk_serialised_csv(self) -> None:
        config: SupervisorConfig = self._make_config()
        chunks: list[list[str]] = [
            ['http://a', 'http://c', 'http://e'],
            ['http://b', 'http://d'],
        ]
        captured_envs: list[dict[str, str]] = []

        def fake_popen(argv, env, **kwargs):
            captured_envs.append(env)
            mock = MagicMock()
            mock.pid = 1
            return mock

        with patch(
            'scrape_exchange.scraper_supervisor.subprocess.Popen',
            side_effect=fake_popen,
        ):
            spawn_children(config, chunks)

        self.assertEqual(
            captured_envs[0]['PROXIES'],
            'http://a,http://c,http://e',
        )
        self.assertEqual(
            captured_envs[1]['PROXIES'],
            'http://b,http://d',
        )


if __name__ == '__main__':
    unittest.main()
