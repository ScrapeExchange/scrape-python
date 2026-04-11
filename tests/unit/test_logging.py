'''
Unit tests for ``scrape_exchange.logging``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import logging
import unittest
from io import StringIO

from scrape_exchange import logging as se_logging
from scrape_exchange.logging import (
    _ORIGINAL_LOGGER_METHODS,
    JsonFormatter,
    configure_logging,
)


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter().format(record))


def _make_record(
    msg: str = 'hello',
    level: int = logging.INFO,
    args: tuple = (),
    extra: dict | None = None,
    exc_info=None,
) -> logging.LogRecord:
    logger: logging.Logger = logging.getLogger('test.logger')
    record: logging.LogRecord = logger.makeRecord(
        name='test.logger',
        level=level,
        fn='fake.py',
        lno=42,
        msg=msg,
        args=args,
        exc_info=exc_info,
        func='fake_func',
        extra=extra,
    )
    return record


class JsonFormatterTests(unittest.TestCase):

    def test_standard_fields_present(self) -> None:
        payload = _format(_make_record('hello world'))
        self.assertEqual(payload['level'], 'INFO')
        self.assertEqual(payload['logger'], 'test.logger')
        self.assertEqual(payload['file'], 'fake.py')
        self.assertEqual(payload['func'], 'fake_func')
        self.assertEqual(payload['line'], 42)
        self.assertEqual(payload['message'], 'hello world')
        self.assertIn('timestamp', payload)
        self.assertTrue(payload['timestamp'].endswith('+00:00'))

    def test_message_args_are_interpolated(self) -> None:
        record = _make_record(msg='user %s id %d', args=('ada', 7))
        payload = _format(record)
        self.assertEqual(payload['message'], 'user ada id 7')

    def test_extra_fields_merged(self) -> None:
        record = _make_record(
            extra={'video_id': 'abc123', 'retry': 3}
        )
        payload = _format(record)
        self.assertEqual(payload['video_id'], 'abc123')
        self.assertEqual(payload['retry'], 3)

    def test_exception_included(self) -> None:
        try:
            raise RuntimeError('boom')
        except RuntimeError:
            import sys
            record = _make_record(
                msg='failed',
                level=logging.ERROR,
                exc_info=sys.exc_info(),
            )
        payload = _format(record)
        self.assertIn('exception', payload)
        self.assertIn('RuntimeError: boom', payload['exception'])

    def test_non_serializable_extra_falls_back_to_repr(self) -> None:
        class Opaque:
            def __repr__(self) -> str:
                return '<Opaque>'

        record = _make_record(extra={'obj': Opaque()})
        payload = _format(record)
        self.assertEqual(payload['obj'], '<Opaque>')

    def test_exception_value_in_extra_rendered_as_dict(self) -> None:
        record = _make_record(
            extra={'exc': ValueError('bad id')}
        )
        payload = _format(record)
        self.assertEqual(
            payload['exc'],
            {'type': 'ValueError', 'message': 'bad id'},
        )

    def test_output_is_single_line(self) -> None:
        formatted = JsonFormatter().format(
            _make_record('line1\nline2')
        )
        self.assertEqual(formatted.count('\n'), 0)


class ConfigureLoggingTests(unittest.TestCase):

    def setUp(self) -> None:
        self._root = logging.getLogger()
        self._saved_handlers = list(self._root.handlers)
        self._saved_level = self._root.level
        self._saved_logger_methods = {
            name: getattr(logging.Logger, name)
            for name in _ORIGINAL_LOGGER_METHODS
        }
        self._saved_log_format = se_logging._log_format

    def tearDown(self) -> None:
        for handler in list(self._root.handlers):
            self._root.removeHandler(handler)
            handler.close()
        for handler in self._saved_handlers:
            self._root.addHandler(handler)
        self._root.setLevel(self._saved_level)
        for name, fn in self._saved_logger_methods.items():
            setattr(logging.Logger, name, fn)
        se_logging._log_format = self._saved_log_format

    def _install_stream_handler(self, formatter) -> StringIO:
        buf = StringIO()
        for handler in list(self._root.handlers):
            self._root.removeHandler(handler)
        handler = logging.StreamHandler(buf)
        handler.setFormatter(formatter)
        self._root.addHandler(handler)
        self._root.setLevel('DEBUG')
        return buf

    def test_json_format_emits_parseable_record(self) -> None:
        buf = self._install_stream_handler(JsonFormatter())
        logging.getLogger('test.tool').info(
            'scraped', extra={'video_id': 'xyz'}
        )
        line = buf.getvalue().strip().splitlines()[-1]
        payload = json.loads(line)
        self.assertEqual(payload['message'], 'scraped')
        self.assertEqual(payload['video_id'], 'xyz')
        self.assertEqual(payload['level'], 'INFO')

    def test_configure_logging_json_uses_json_formatter(self) -> None:
        configure_logging(
            level='INFO', filename='/dev/null', log_format='json'
        )
        handlers = logging.getLogger().handlers
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0].formatter, JsonFormatter)

    def test_configure_logging_text_uses_plain_formatter(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='text'
        )
        handlers = logging.getLogger().handlers
        self.assertEqual(len(handlers), 1)
        self.assertNotIsInstance(handlers[0].formatter, JsonFormatter)
        self.assertEqual(logging.getLogger().level, logging.DEBUG)

    def test_exc_kwarg_json_emits_top_level_field(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        err = RuntimeError('boom')
        logging.warning('upload failed', exc=err)

        line = buf.getvalue().strip().splitlines()[-1]
        payload = json.loads(line)
        self.assertEqual(payload['message'], 'upload failed')
        self.assertEqual(
            payload['exc'],
            {'type': 'RuntimeError', 'message': 'boom'},
        )

    def test_exc_kwarg_text_appends_to_message(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='text'
        )
        buf = self._install_stream_handler(
            logging.Formatter('%(message)s')
        )

        logging.info('failed task', exc=ValueError('bad'))

        output = buf.getvalue().strip()
        self.assertIn('failed task: bad', output)

    def test_exc_kwarg_preserves_existing_extra(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        logging.error(
            'multi-field',
            exc=KeyError('missing'),
            extra={'video_id': 'abc'},
        )

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['video_id'], 'abc')
        self.assertEqual(payload['exc']['type'], 'KeyError')

    def test_calls_without_exc_still_work(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        logging.info('plain message')

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['message'], 'plain message')
        self.assertNotIn('exc', payload)

    def test_named_logger_instance_accepts_exc(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        named = logging.getLogger('scrape_exchange.test')
        named.warning('named failure', exc=ValueError('oops'))

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['logger'], 'scrape_exchange.test')
        self.assertEqual(payload['message'], 'named failure')
        self.assertEqual(
            payload['exc'],
            {'type': 'ValueError', 'message': 'oops'},
        )

    def test_reserved_extra_key_is_renamed_not_raised(self) -> None:
        '''
        Regression: ``extra={'name': ...}`` used to raise
        ``KeyError('Attempt to overwrite ...')`` because ``name`` is a
        reserved ``LogRecord`` attribute. The wrapper now suffixes the
        colliding key with an underscore so the call succeeds and the
        data is still emitted.
        '''

        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        # Would previously raise KeyError inside Logger.makeRecord.
        logging.debug(
            'normalise channel',
            extra={'name': 'JohnDoe', 'filename': 'foo.json'},
        )

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['name_'], 'JohnDoe')
        self.assertEqual(payload['filename_'], 'foo.json')
        # The logger's own name/file fields remain intact.
        self.assertEqual(payload['logger'], 'root')
        self.assertNotEqual(payload['file'], 'foo.json')

    def test_source_location_reports_caller_not_wrapper(self) -> None:
        '''
        Regression: records emitted through the wrapped logger used to
        report ``file=logging.py, func=wrapper, line=77`` instead of
        the actual call site. The wrapper bumps ``stacklevel`` so
        ``findCaller`` reports the real source location.
        '''

        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        def caller() -> None:
            logging.info('hello from caller')

        caller()

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['func'], 'caller')
        self.assertEqual(payload['file'], 'test_logging.py')
        # And the same through a named Logger instance.
        named = logging.getLogger('scrape_exchange.test')

        def named_caller() -> None:
            named.warning('hello from named caller')

        named_caller()

        payload = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload['func'], 'named_caller')
        self.assertEqual(payload['file'], 'test_logging.py')

    def test_all_wrapped_functions_accept_exc(self) -> None:
        configure_logging(
            level='DEBUG', filename='/dev/null', log_format='json'
        )
        buf = self._install_stream_handler(JsonFormatter())

        err = RuntimeError('x')
        for name in ('debug', 'info', 'warning', 'error', 'critical'):
            getattr(logging, name)(f'{name} msg', exc=err)

        lines = [
            json.loads(line)
            for line in buf.getvalue().strip().splitlines()
        ]
        # exception() itself requires being inside an except block;
        # verify the other five wrappers all attached the exc field.
        self.assertEqual(len(lines), 5)
        for payload in lines:
            self.assertEqual(
                payload['exc'],
                {'type': 'RuntimeError', 'message': 'x'},
            )


if __name__ == '__main__':
    unittest.main()
