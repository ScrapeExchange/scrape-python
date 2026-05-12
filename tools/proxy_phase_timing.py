#!/usr/bin/env python3
'''Measure proxy CONNECT vs TLS handshake time separately.

Splits the cost of an HTTPS-through-proxy request into:
  1. TCP connect to proxy
  2. HTTP CONNECT tunnel establishment (proxy <-> origin)
  3. TLS handshake to origin via the tunnel

Run from a scraper host so the measurement reflects production
egress. Repeats five times so transient variance is visible.

Usage:
    python3 tools/proxy_phase_timing.py http://user:pass@host:port [origin]

Default origin is www.youtube.com:443.
'''

import asyncio
import base64
import ssl
import sys
import time
from urllib.parse import urlparse


async def measure(
    proxy_url: str, host: str, port: int = 443,
) -> dict[str, float]:
    p = urlparse(proxy_url)
    auth_header: str = ''
    if p.username:
        cred: str = f'{p.username}:{p.password or ""}'
        b64: str = base64.b64encode(cred.encode()).decode()
        auth_header = f'Proxy-Authorization: Basic {b64}\r\n'

    t0: float = time.perf_counter()

    # Phase 1: TCP to proxy
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(p.hostname, p.port),
        timeout=30,
    )
    t1: float = time.perf_counter()

    # Phase 2: HTTP CONNECT request + response
    req: str = (
        f'CONNECT {host}:{port} HTTP/1.1\r\n'
        f'Host: {host}:{port}\r\n'
        f'{auth_header}'
        f'\r\n'
    )
    writer.write(req.encode())
    await writer.drain()
    status_line: bytes = await reader.readline()
    if not status_line.startswith(b'HTTP/1.1 200'):
        writer.close()
        raise RuntimeError(
            f'CONNECT failed: {status_line!r}'
        )
    while True:
        line: bytes = await reader.readline()
        if line in (b'\r\n', b''):
            break
    t2: float = time.perf_counter()

    # Phase 3: TLS handshake to origin via tunnel
    ctx: ssl.SSLContext = ssl.create_default_context()
    loop: asyncio.AbstractEventLoop = (
        asyncio.get_event_loop()
    )
    transport = writer.transport
    new_transport = await loop.start_tls(
        transport,
        transport.get_protocol(),
        ctx,
        server_hostname=host,
    )
    t3: float = time.perf_counter()

    new_transport.close()

    return {
        'tcp_ms': (t1 - t0) * 1000,
        'http_connect_ms': (t2 - t1) * 1000,
        'tls_ms': (t3 - t2) * 1000,
        'total_ms': (t3 - t0) * 1000,
    }


async def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    proxy: str = sys.argv[1]
    origin: str = (
        sys.argv[2] if len(sys.argv) > 2
        else 'www.youtube.com'
    )
    for i in range(5):
        try:
            r: dict[str, float] = (
                await measure(proxy, origin)
            )
            print(
                f'#{i + 1}: '
                f'tcp={r["tcp_ms"]:7.1f}ms  '
                f'connect={r["http_connect_ms"]:7.1f}ms  '
                f'tls={r["tls_ms"]:7.1f}ms  '
                f'total={r["total_ms"]:7.1f}ms'
            )
        except Exception as exc:
            print(f'#{i + 1}: ERROR {exc!r}')
        await asyncio.sleep(0.5)


if __name__ == '__main__':
    asyncio.run(main())
