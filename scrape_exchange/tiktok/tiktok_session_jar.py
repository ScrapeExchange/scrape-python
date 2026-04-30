'''
Persistent ms_token store and per-proxy Chromium profile-dir
allocator for the TikTok session pool. Mirrors the role of
``scrape_exchange.youtube.youtube_cookiejar``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path


@dataclass
class SessionRecord:
    '''A captured ms_token plus capture timestamp.'''
    ms_token: str
    captured_at: float = field(default_factory=time.time)
    ttl_seconds: int = 14400

    def is_expired(self) -> bool:
        return (
            time.time() - self.captured_at
            >= self.ttl_seconds
        )

    def age_seconds(self) -> float:
        return time.time() - self.captured_at


class TikTokSessionJar:
    '''
    Persists per-proxy ms_token records and Chromium profile dirs
    under ``state_dir``. Layout::

        state_dir/
        ├── tokens.json            # { proxy_ip: { ms_token,
        │                          #               captured_at,
        │                          #               ttl_seconds } }
        └── profiles/
            └── <proxy_ip>/        # Playwright user_data_dir per
                                   #   proxy
    '''

    _STORE: str = 'tokens.json'

    def __init__(
        self, state_dir: str, ttl_seconds: int,
    ) -> None:
        self._dir: Path = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        (self._dir / 'profiles').mkdir(exist_ok=True)
        self._ttl_seconds: int = ttl_seconds
        self._records: dict[str, SessionRecord] = {}
        self._load()

    def _store_path(self) -> Path:
        return self._dir / self._STORE

    def _load(self) -> None:
        path: Path = self._store_path()
        if not path.is_file():
            return
        raw: dict = json.loads(path.read_text())
        for proxy_ip, fields in raw.items():
            self._records[proxy_ip] = SessionRecord(
                ms_token=fields['ms_token'],
                captured_at=fields.get(
                    'captured_at', time.time(),
                ),
                ttl_seconds=fields.get(
                    'ttl_seconds', self._ttl_seconds,
                ),
            )

    def set_token(self, proxy_ip: str, ms_token: str) -> None:
        self._records[proxy_ip] = SessionRecord(
            ms_token=ms_token,
            ttl_seconds=self._ttl_seconds,
        )

    def get(self, proxy_ip: str) -> SessionRecord | None:
        return self._records.get(proxy_ip)

    def profile_dir(self, proxy_ip: str) -> Path:
        d: Path = self._dir / 'profiles' / proxy_ip
        d.mkdir(parents=True, exist_ok=True)
        return d

    def flush(self) -> None:
        path: Path = self._store_path()
        payload: dict = {
            proxy_ip: asdict(rec)
            for proxy_ip, rec in self._records.items()
        }
        path.write_text(json.dumps(payload, indent=2))

    def tokens(self) -> list[str]:
        return [rec.ms_token for rec in self._records.values()]
