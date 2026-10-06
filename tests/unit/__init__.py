'''Unit tests never talk to a real MongoDB: an empty MONGO_DSN in the
environment overrides any value in a developer's .env file, so
VideoScrapeQueueSettings() stays in Redis-only mode unless a test
injects a backlog explicitly.'''

import os

os.environ['MONGO_DSN'] = ''
