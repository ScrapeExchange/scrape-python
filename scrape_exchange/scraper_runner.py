'''
Shared scaffolding for scraper tools.

:class:`ScraperRunner` consolidates the startup and
shutdown boilerplate that every scraper tool repeats:
supervisor check, logging, metrics, rate-limiter init,
ExchangeClient setup, signal handlers, and graceful
drain.

The tool's own worker logic is passed as an async
callable that receives a :class:`ScraperRunContext`.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import signal
import sys

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from scrape_exchange.metrics_server import start_metrics_server

from scrape_exchange.exchange_client import (
    ExchangeClient,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.rate_limiter import RateLimiter
from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.proxy_loader import (
    aclose_pooled_httpx_clients,
)
from scrape_exchange.youtube.youtube_client import (
    aclose_pooled_youtube_clients,
)
from scrape_exchange.youtube.youtube_channel_tabs import (
    aclose_pooled_innertube,
    configure_innertube_executor,
    shutdown_innertube_executor,
)
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.settings import ScraperSettings


# Cadence of the async heartbeat that proves the event loop is alive.
# Must be comfortably below the watchdog loop timeout (default 60s).
_HEARTBEAT_INTERVAL_SECONDS: float = 5.0


async def _watchdog_heartbeat() -> None:
    '''Touch the loop-liveness signal forever. If the event loop ever
    freezes, this stops running and the watchdog's loop signal goes
    stale.'''
    while True:
        Watchdog.get().touch_loop()
        await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)


def _start_metrics_server_or_skip(metrics_port: int) -> None:
    '''Bind the per-worker Prometheus metrics HTTP server on
    ``metrics_port``. Skip the bind silently when the supervisor
    has set ``PROMETHEUS_MULTIPROC_DIR``, because in that mode
    worker metrics are aggregated through the multiproc directory
    and exposed via the supervisor's single HTTP endpoint.
    '''

    if os.environ.get('PROMETHEUS_MULTIPROC_DIR'):
        logging.info(
            'Worker running under supervisor multiproc mode; '
            'skipping per-worker metrics HTTP server',
            extra={'metrics_port': metrics_port},
        )
        return
    try:
        start_metrics_server(metrics_port)
        logging.info(
            'Prometheus metrics available',
            extra={'metrics_port': metrics_port},
        )
    except OSError as exc:
        logging.warning(
            'Failed to bind Prometheus metrics port; worker will '
            'run without metrics',
            exc=exc,
            extra={'metrics_port': metrics_port},
        )


@dataclass
class ScraperRunContext:
    '''Passed to the worker function by
    ScraperRunner.'''
    settings: ScraperSettings
    client: ExchangeClient | None
    rate_limiter: RateLimiter
    proxies: list[str]


class ScraperRunner:
    '''
    Shared startup/shutdown scaffolding for scraper
    tools.

    Owns: supervisor dispatch, logging, metrics,
    rate-limiter init, ExchangeClient setup, signal
    handlers, graceful drain.

    Does NOT own: the worker loop, domain models,
    upload payloads, platform-specific setup.
    '''

    def __init__(
        self,
        settings: ScraperSettings,
        scraper_label: str,
        platform: str,
        num_processes: int,
        concurrency: int,
        metrics_port: int,
        log_file: str,
        log_level: str,
        rate_limiter_factory: Callable[
            [ScraperSettings], RateLimiter
        ],
        client_required: bool = True,
    ) -> None:
        self._settings: ScraperSettings = settings
        self._scraper_label: str = scraper_label
        self._platform: str = platform
        self._num_processes: int = num_processes
        self._concurrency: int = concurrency
        self._metrics_port: int = metrics_port
        self._log_file: str = log_file
        self._log_level: str = log_level
        self._rl_factory: Callable[
            [ScraperSettings], RateLimiter
        ] = rate_limiter_factory
        self._client_required: bool = client_required

    def run_sync(
        self,
        worker_func: Callable[
            ['ScraperRunContext'],
            Awaitable[None],
        ],
    ) -> int:
        '''
        Synchronous entry point for ``main()``.

        Runs the supervisor check before entering
        asyncio.  If ``num_processes > 1``, dispatches
        to the supervisor and returns its exit code.
        Otherwise enters ``asyncio.run(self.run(...))``.
        '''
        configure_logging(
            level=self._log_level,
            filename=self._log_file,
            log_format=self._settings.log_format,
        )

        if self._num_processes > 1:
            return run_supervisor(SupervisorConfig(
                scraper_label=self._scraper_label,
                num_processes_env_var=(
                    f'{self._scraper_label.upper()}'
                    '_NUM_PROCESSES'
                ),
                log_file_env_var=(
                    f'{self._scraper_label.upper()}'
                    '_LOG_FILE'
                ),
                metrics_port_env_var=(
                    f'{self._scraper_label.upper()}'
                    '_METRICS_PORT'
                ),
                num_processes=self._num_processes,
                concurrency=self._concurrency,
                proxies=self._settings.proxies,
                metrics_port=self._metrics_port,
                log_file=self._log_file or None,
                api_key_id=(
                    self._settings.api_key_id
                ),
                api_key_secret=(
                    self._settings.api_key_secret
                ),
                exchange_url=(
                    self._settings.exchange_url
                ),
            ))

        try:
            asyncio.run(self.run(worker_func))
        except asyncio.CancelledError:
            logging.info(
                'Scraper shutdown complete',
                extra={'scraper': self._scraper_label},
            )
        # Terminate via os._exit(0) rather than returning: the dedicated
        # InnerTube executor's worker threads are non-daemon, and a still-
        # wedged InnerTube call would otherwise hang the interpreter's
        # atexit join indefinitely. The drain already ran in run()'s
        # finally; flush logging and exit immediately. rc 0 makes the
        # supervisor retire the slot (no respawn) for an intentional stop.
        logging.shutdown()
        os._exit(0)

    async def run(
        self,
        worker_func: Callable[
            ['ScraperRunContext'],
            Awaitable[None],
        ],
    ) -> None:
        '''
        Async entry point.  Sets up metrics, rate
        limiter, client, signal handlers, then calls
        ``worker_func(context)``.  Drains uploads on
        exit.
        '''
        proxies: list[str] = self._settings.proxies

        logging.info(
            'Scraper worker started',
            extra={
                'scraper': self._scraper_label,
                'platform': self._platform,
                'metrics_port': self._metrics_port,
                'proxies_count': len(proxies),
                'first_proxy': (
                    proxies[0] if proxies else None
                ),
                'last_proxy': (
                    proxies[-1] if proxies else None
                ),
            },
        )

        _start_metrics_server_or_skip(self._metrics_port)
        publish_config_metrics(
            role='worker', scraper_label=self._scraper_label, num_processes=1,
            concurrency=self._concurrency,
        )

        configure_innertube_executor(
            self._settings.innertube_executor_threads,
        )

        heartbeat_task: asyncio.Task[Any] | None = None
        watchdog: Watchdog | None = None
        if self._settings.watchdog_enabled:
            watchdog = Watchdog(
                loop_timeout=(
                    self._settings.watchdog_loop_timeout_seconds
                ),
                work_timeout=(
                    self._settings.watchdog_work_timeout_seconds
                ),
            )
            Watchdog.set_instance(watchdog)
            watchdog.start()
            heartbeat_task = asyncio.create_task(_watchdog_heartbeat())

        rate_limiter: RateLimiter = (self._rl_factory(self._settings))
        rate_limiter.set_proxies(self._settings.proxies)

        post_rate: float = float(max(
            1, self._num_processes * self._concurrency,
        ))
        ScrapeExchangeRateLimiter.get(
            state_dir=(self._settings.rate_limiter_state_dir),
            post_rate=post_rate, redis_dsn=self._settings.redis_dsn,
        )

        client: ExchangeClient | None = None
        try:
            client = await ExchangeClient.setup(
                api_key_id=(self._settings.api_key_id),
                api_key_secret=(self._settings.api_key_secret),
                exchange_url=(self._settings.exchange_url),
            )
        except Exception as exc:
            if self._client_required:
                logging.critical(
                    'Failed to connect to Scrape Exchange API',
                    exc=exc, extra={
                        'exchange_url': (self._settings.exchange_url),
                    }
                )
                logging.shutdown()
                sys.exit(1)
            logging.warning(
                'ExchangeClient setup failed; continuing without upload '
                'capability', exc=exc,
            )

        loop: asyncio.AbstractEventLoop = (asyncio.get_running_loop())
        main_task: asyncio.Task[Any] = (asyncio.current_task())
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, main_task.cancel)
            except NotImplementedError:
                pass

        try:
            ctx: ScraperRunContext = (
                ScraperRunContext(
                    settings=self._settings, client=client,
                    rate_limiter=rate_limiter, proxies=proxies,
                )
            )
            await worker_func(ctx)
        except asyncio.CancelledError:
            logging.info(
                'Shutdown signal received; draining background uploads'
            )
            raise
        finally:
            if watchdog is not None:
                watchdog.stop()
            if heartbeat_task is not None:
                heartbeat_task.cancel()
            if client is not None:
                await client.drain_uploads(timeout=10.0)
            await aclose_pooled_httpx_clients()
            await aclose_pooled_youtube_clients()
            await aclose_pooled_innertube()
            shutdown_innertube_executor()
