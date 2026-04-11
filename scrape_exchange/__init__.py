'''
Package init for ``scrape_exchange``.

Importing this subpackage eagerly loads :mod:`scrape_exchange.logging`,
which installs the JSON-friendly wrappers on ``logging.Logger`` (adding
``exc=`` kwarg support, sanitising ``extra=`` keys that collide with
reserved ``LogRecord`` attributes, and fixing ``findCaller`` stacklevel
accounting). Every module under ``scrape_exchange`` relies on this, so
making the side effect unconditional guarantees the wrappers are
active regardless of which submodule a caller imports first.
'''

from . import logging as _logging  # noqa: F401
