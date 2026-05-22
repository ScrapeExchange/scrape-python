'''
Verify every Gauge in the scraper fleet has the expected
multiprocess_mode declared. The mode is what makes the gauge usable
when the supervisor sets PROMETHEUS_MULTIPROC_DIR; without it,
prometheus_client raises ValueError on Gauge construction in
multiproc mode.
'''
import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType


def _load_tool(name: str) -> ModuleType:
    '''
    Load a module from tools/<name>.py, reusing any already-loaded
    instance from sys.modules to avoid duplicate-registry errors when
    some test files load via spec_from_file_location (bare name) while
    others use the dotted tools.<name> form.
    '''
    for key in (name, f'tools.{name}'):
        if key in sys.modules:
            return sys.modules[key]
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / f'{name}.py'
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    sys.modules[f'tools.{name}'] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class TestSharedModulesGaugeModes(unittest.TestCase):

    def test_rate_limiter_bucket_tokens_livemostrecent(self) -> None:
        from scrape_exchange.rate_limiter import (
            METRIC_BUCKET_TOKENS,
        )
        self.assertEqual(
            METRIC_BUCKET_TOKENS._multiprocess_mode,
            'livemostrecent',
        )

    def test_rate_limiter_global_bucket_tokens_livemostrecent(
        self,
    ) -> None:
        from scrape_exchange.rate_limiter import (
            METRIC_GLOBAL_BUCKET_TOKENS,
        )
        self.assertEqual(
            METRIC_GLOBAL_BUCKET_TOKENS._multiprocess_mode,
            'livemostrecent',
        )

    def test_exchange_client_upload_queue_depth_livemostrecent(
        self,
    ) -> None:
        from scrape_exchange.exchange_client import (
            METRIC_UPLOAD_QUEUE_DEPTH,
        )
        self.assertEqual(
            METRIC_UPLOAD_QUEUE_DEPTH._multiprocess_mode,
            'livemostrecent',
        )

    def test_scraper_metrics_scrape_queue_size_livemostrecent(
        self,
    ) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_SCRAPE_QUEUE_SIZE,
        )
        self.assertEqual(
            METRIC_SCRAPE_QUEUE_SIZE._multiprocess_mode,
            'livemostrecent',
        )

    def test_scraper_metrics_worker_sleep_seconds_livemostrecent(
        self,
    ) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_WORKER_SLEEP_SECONDS,
        )
        self.assertEqual(
            METRIC_WORKER_SLEEP_SECONDS._multiprocess_mode,
            'livemostrecent',
        )

    def test_scraper_metrics_channel_priority_queue_age_max(
        self,
    ) -> None:
        from scrape_exchange.scraper_metrics import (
            METRIC_CHANNEL_PRIORITY_QUEUE_AGE,
        )
        self.assertEqual(
            METRIC_CHANNEL_PRIORITY_QUEUE_AGE._multiprocess_mode,
            'max',
        )

    def test_supervisor_num_processes_max(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            METRIC_NUM_PROCESSES,
        )
        self.assertEqual(
            METRIC_NUM_PROCESSES._multiprocess_mode, 'max',
        )

    def test_supervisor_concurrency_max(self) -> None:
        from scrape_exchange.scraper_supervisor import (
            METRIC_CONCURRENCY,
        )
        self.assertEqual(
            METRIC_CONCURRENCY._multiprocess_mode, 'max',
        )

    def test_youtube_rate_limiter_circuit_state_max(self) -> None:
        from scrape_exchange.youtube.youtube_rate_limiter import (
            METRIC_RSS_CIRCUIT_STATE,
        )
        self.assertEqual(
            METRIC_RSS_CIRCUIT_STATE._multiprocess_mode, 'max',
        )

    def test_youtube_video_extract_info_active_livemostrecent(
        self,
    ) -> None:
        from scrape_exchange.youtube.youtube_video import (
            METRIC_EXTRACT_INFO_ACTIVE,
        )
        self.assertEqual(
            METRIC_EXTRACT_INFO_ACTIVE._multiprocess_mode,
            'livemostrecent',
        )


class TestToolLocalGaugeModes(unittest.TestCase):

    def test_yt_channel_scrape_unique_channels_read(self) -> None:
        from tools.yt_channel_scrape import (
            METRIC_UNIQUE_CHANNELS_READ,
        )
        self.assertEqual(
            METRIC_UNIQUE_CHANNELS_READ._multiprocess_mode,
            'livemostrecent',
        )

    def test_yt_channel_scrape_channel_ids_to_resolve(self) -> None:
        from tools.yt_channel_scrape import (
            METRIC_CHANNEL_IDS_TO_RESOLVE,
        )
        self.assertEqual(
            METRIC_CHANNEL_IDS_TO_RESOLVE._multiprocess_mode,
            'livemostrecent',
        )

    def test_yt_rss_scrape_channel_map_size(self) -> None:
        mod = _load_tool('yt_rss_scrape')
        self.assertEqual(
            mod.METRIC_CHANNEL_MAP_SIZE._multiprocess_mode,
            'livemostrecent',
        )

    def test_yt_rss_scrape_concurrency(self) -> None:
        mod = _load_tool('yt_rss_scrape')
        self.assertEqual(
            mod.METRIC_CONCURRENCY._multiprocess_mode,
            'livemostrecent',
        )

    def test_yt_rss_scrape_seconds_since_last_processed(
        self,
    ) -> None:
        mod = _load_tool('yt_rss_scrape')
        self.assertEqual(
            mod.METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED
            ._multiprocess_mode,
            'livemostrecent',
        )

    def test_yt_rss_scrape_tier_population_max(self) -> None:
        mod = _load_tool('yt_rss_scrape')
        self.assertEqual(
            mod.METRIC_TIER_POPULATION._multiprocess_mode, 'max',
        )

    def test_yt_channel_upload_files_pending_upload(self) -> None:
        from tools.yt_channel_upload import (
            METRIC_FILES_PENDING_UPLOAD,
        )
        self.assertEqual(
            METRIC_FILES_PENDING_UPLOAD._multiprocess_mode,
            'livemostrecent',
        )


if __name__ == '__main__':
    unittest.main()
