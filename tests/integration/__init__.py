'''Integration tests — require live network / YouTube access.

Gated by RUN_INTEGRATION=1 so a plain ``python -m unittest discover``
from the repo root skips this directory cleanly.
'''

import os
import unittest


if not os.environ.get('RUN_INTEGRATION'):
    raise unittest.SkipTest(
        'Integration tests disabled; set RUN_INTEGRATION=1 to enable.',
    )
