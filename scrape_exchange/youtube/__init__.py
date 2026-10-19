'''
Bring your own data and algorithms data import module

:maintainer : Boinko (boinko@scrape.exchange)
:copyright  : Copyright 2026
:license    : GPLv3
'''

from scrape_exchange.youtube import innertube_orjson

# Installed here so every InnerTube client, wherever it is created,
# decodes responses with orjson.
innertube_orjson.install()
