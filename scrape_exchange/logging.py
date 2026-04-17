'''
Shared logging configuration for scrape_exchange tools.

Provides a JSON formatter and a ``configure_logging`` helper that the
YouTube scraping tools use to set up structured or plain-text logging
based on their settings.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import logging.handlers
from datetime import datetime, timezone
from typing import Any, Callable

import orjson


_RESERVED_ATTRS: frozenset[str] = frozenset({
    'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
    'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
    'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
    'thread', 'threadName', 'processName', 'process', 'message',
    'asctime', 'taskName',
})


_PATCHED_METHODS: tuple[str, ...] = (
    'debug', 'info', 'warning', 'error', 'exception', 'critical',
)
# Originals for the ``Logger`` class methods. Wrapping only the
# ``Logger`` class — not the module-level ``logging.debug`` etc. —
# covers both call styles: stdlib ``logging.debug`` just delegates to
# ``root.debug`` which hits the wrapped class method. Patching a
# single layer keeps the ``stacklevel`` bump predictable.
_ORIGINAL_LOGGER_METHODS: dict[str, Callable] = {
    name: getattr(logging.Logger, name) for name in _PATCHED_METHODS
}

# Current output format; the wrappers consult this at call time so the
# wrappers can be installed once at import time (before any
# ``configure_logging`` call) and later reflect the real format once
# the tool has resolved its settings.
_log_format: str = 'json'


def _sanitize_extra(extra: dict) -> dict:
    '''
    Rename keys in *extra* that collide with reserved ``LogRecord``
    attribute names. Without this, stdlib ``Logger.makeRecord`` raises
    ``KeyError("Attempt to overwrite 'name' in LogRecord")`` when a
    caller logs structured data whose key happens to match a standard
    record attribute (``name``, ``msg``, ``args``, ``filename``,
    ``lineno`` …). Colliding keys are suffixed with an underscore,
    which keeps them visible in the JSON output without losing data.
    '''

    out: dict = {}
    for key, value in extra.items():
        if key in _RESERVED_ATTRS:
            safe_key: str = f'{key}_'
            # Extremely defensive: if the suffixed name ALSO happens to
            # collide (e.g. two attrs, one already suffixed), keep
            # adding underscores until we find a free slot.
            while safe_key in _RESERVED_ATTRS or safe_key in out:
                safe_key += '_'
            out[safe_key] = value
        else:
            out[key] = value
    return out


def _make_logger_method_exc_wrapper(original: Callable) -> Callable:
    '''
    Wrap a ``logging.Logger`` instance method (``debug``, ``info``,
    etc.) so that ``logger.info('msg', exc=exc, extra={...})`` call
    sites behave consistently:

    * ``exc=`` is folded into ``extra['exc']`` (emitted as a top-level
      JSON field by :class:`JsonFormatter`).
    * In text mode the exception is appended to the message so the
      traditional colon-separated output retains the exception detail.
    * ``extra=`` is sanitised so keys colliding with ``LogRecord``
      attributes are suffixed with an underscore instead of raising.
    * ``stacklevel`` is bumped by one so ``findCaller`` reports the
      real source site rather than this wrapper.
    '''

    def wrapper(self, msg, *args, exc=None, **kwargs):
        extra: dict | None = kwargs.pop('extra', None)
        if exc is not None:
            extra = dict(extra or {})
            extra['exc'] = exc
            if _log_format == 'text':
                msg = f'{msg}: {exc}'
        if extra:
            extra = _sanitize_extra(extra)
            kwargs['extra'] = extra
        kwargs['stacklevel'] = kwargs.get('stacklevel', 1) + 1
        return original(self, msg, *args, **kwargs)

    wrapper.__wrapped__ = original  # type: ignore[attr-defined]
    return wrapper


def _install_exc_wrappers() -> None:
    for name, original in _ORIGINAL_LOGGER_METHODS.items():
        setattr(
            logging.Logger,
            name,
            _make_logger_method_exc_wrapper(original),
        )


# Install wrappers eagerly so every import path through
# ``scrape_exchange.logging`` (including unit tests that import library
# modules using ``_LOGGER.X(..., exc=exc)``) picks up the ``exc``
# kwarg support regardless of whether ``configure_logging`` was called.
_install_exc_wrappers()


class JsonFormatter(logging.Formatter):
    '''
    Format log records as single-line JSON documents.

    Emits the standard fields (timestamp, level, logger, file, func,
    line, message) plus a traceback string under ``exception`` when
    ``exc_info`` is present. Any keys passed via the ``extra`` argument
    of a logging call are merged into the top level of the record.
    '''

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            'timestamp': datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'file': record.filename,
            'func': record.funcName,
            'line': record.lineno,
            'message': record.getMessage(),
        }

        if record.exc_info:
            payload['exception'] = self.formatException(record.exc_info)
        if record.stack_info:
            payload['stack'] = self.formatStack(record.stack_info)

        reserved: frozenset[str] = _RESERVED_ATTRS | payload.keys()
        for key, value in record.__dict__.items():
            if key in reserved or key.startswith('_'):
                continue
            if isinstance(value, BaseException):
                payload[key] = {
                    'type': type(value).__name__,
                    'message': str(value),
                }
                continue
            try:
                orjson.dumps(value)
                payload[key] = value
            except TypeError:
                payload[key] = repr(value)

        return orjson.dumps(payload).decode('utf-8')


_TEXT_FORMAT: str = (
    '%(levelname)s:'
    '%(asctime)s:'
    '%(filename)s:'
    '%(funcName)s():'
    '%(lineno)d:'
    '%(message)s'
)


def configure_logging(
    level: str,
    filename: str,
    log_format: str,
) -> None:
    '''
    Configure the root logger for a scraping tool.

    :param level: Logging level name (e.g. ``'INFO'``).
    :param filename: Destination file path; ``/dev/stdout`` sends to
        standard output.
    :param log_format: Either ``'json'`` for structured records or
        ``'text'`` for the legacy colon-separated format.
    '''

    handler: logging.Handler
    if filename == '/dev/stdout':
        handler = logging.FileHandler(filename)
    else:
        handler = logging.handlers.WatchedFileHandler(filename)
    formatter: logging.Formatter
    if log_format == 'json':
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)
    handler.setFormatter(formatter)

    root: logging.Logger = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)

    global _log_format
    _log_format = log_format
