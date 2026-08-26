#!/usr/bin/env python3
'''Remove duplicate scraped-video files across scraper hosts.

Duplicates are matched by basename only, non-recursively, under the
same directory on each host. The newest mtime is kept; older copies are
printed in dry-run mode or deleted with ``--delete``.
'''

from __future__ import annotations

import argparse
import fnmatch
import shlex
import socket
import subprocess
import sys

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


DEFAULT_DIR: str = '~/byoda/data/scraped/youtube/videos'

_STATUS_LINE_LEN: int = 0
DELETE_BATCH_SIZE: int = 500


class SshAuthError(RuntimeError):
    '''Raised with operator-focused guidance for Paramiko auth failures.'''


@dataclass(frozen=True)
class RemoteFile:
    host: str
    mtime: float
    path: str

    @property
    def name(self) -> str:
        return PurePosixPath(self.path).name


@dataclass(frozen=True)
class DuplicateGroup:
    keep: RemoteFile
    delete: list[RemoteFile]


def _host_aliases() -> set[str]:
    names: set[str] = {
        'localhost',
        '127.0.0.1',
        '::1',
    }
    for name in (
        socket.gethostname(),
        socket.getfqdn(),
    ):
        if not name:
            continue
        names.add(name)
        names.add(name.split('.', 1)[0])
    return names


def is_local_host(host: str) -> bool:
    return host in _host_aliases()


def _remote_directory_arg(directory: str) -> str:
    '''Quote a remote path while preserving current-user home expansion.'''
    if directory == '~':
        return '"$HOME"'
    if directory.startswith('~/'):
        relative: str = directory.removeprefix('~/')
        return f'"$HOME"/{shlex.quote(relative)}'
    return shlex.quote(directory)


def _run_local(command: list[str], *, input: bytes | None = None) -> bytes:
    result: subprocess.CompletedProcess[bytes] = subprocess.run(
        command,
        input=input,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f'local command failed with rc={result.returncode}: '
            f'{result.stderr.decode("utf-8", "replace").strip()}'
        )
    return result.stdout


class HostRunner:
    '''Run local commands directly and remote commands over Paramiko.

    One Paramiko SSHClient is kept open per remote host so delete
    batches do not pay SSH connection setup repeatedly.
    '''

    def __init__(self) -> None:
        self._clients: dict[str, Any] = {}

    def close(self) -> None:
        for client in self._clients.values():
            client.close()
        self._clients.clear()

    def run_remote(self, host: str, command: str) -> bytes:
        client: Any = self._client(host)
        stdin: Any
        stdout: Any
        stderr: Any
        stdin, stdout, stderr = client.exec_command(
            command,
        )
        stdin.close()
        out: bytes = stdout.read()
        err: bytes = stderr.read()
        rc: int = stdout.channel.recv_exit_status()
        if rc != 0:
            raise RuntimeError(
                f'remote command on {host!r} failed rc={rc}: '
                f'{err.decode("utf-8", "replace").strip()}'
            )
        return out

    def _client(self, host: str) -> Any:
        if host not in self._clients:
            self._clients[host] = self._connect(host)
        return self._clients[host]

    def _connect(self, host: str) -> Any:
        try:
            import paramiko
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                'Paramiko is required for remote hosts. '
                'Install dependencies with `uv sync` or '
                '`uv add paramiko`.'
            ) from exc

        cfg: dict[str, Any] = self._ssh_config(host, paramiko)
        client: Any = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        connect_kwargs: dict[str, Any] = {
            'hostname': cfg.get('hostname', host),
            'username': cfg.get('user'),
            'port': int(cfg.get('port', 22)),
            'allow_agent': True,
            'look_for_keys': True,
            'timeout': 30,
        }
        identity_files: list[str] | None = cfg.get('identityfile')
        if identity_files:
            connect_kwargs['key_filename'] = [
                str(Path(path).expanduser())
                for path in identity_files
            ]
        proxy_command: str | None = cfg.get('proxycommand')
        if proxy_command:
            connect_kwargs['sock'] = paramiko.ProxyCommand(
                proxy_command,
            )
        try:
            client.connect(**connect_kwargs)
        except (
            paramiko.AuthenticationException,
            paramiko.SSHException,
        ) as exc:
            raise SshAuthError(
                self._format_auth_failure(
                    host, cfg, connect_kwargs, exc,
                )
            ) from exc
        return client

    def _format_auth_failure(
        self,
        host: str,
        cfg: dict[str, Any],
        connect_kwargs: dict[str, Any],
        exc: Exception,
    ) -> str:
        hostname: str = str(connect_kwargs.get('hostname') or host)
        username: str | None = connect_kwargs.get('username')
        port: int = int(connect_kwargs.get('port', 22))
        identity_files: list[str] = (
            connect_kwargs.get('key_filename') or []
        )
        proxy: bool = connect_kwargs.get('sock') is not None
        user_at: str = f'{username}@' if username else ''
        ssh_target: str = f'{user_at}{host}'
        if port != 22:
            ssh_target = f'-p {port} {ssh_target}'

        lines: list[str] = [
            f'SSH authentication failed for host {host!r}: {exc}',
            '',
            'Paramiko resolved this connection as:',
            f'  HostName: {hostname}',
            f'  User: {username or "(current user)"}',
            f'  Port: {port}',
            '  IdentityFile: '
            + (
                ', '.join(identity_files)
                if identity_files else '(none from ~/.ssh/config)'
            ),
            f'  ProxyCommand: {"yes" if proxy else "no"}',
            '',
            'Things to check:',
            f'  1. Verify OpenSSH works: ssh {ssh_target}',
            '  2. If OpenSSH relies on an agent, make sure the key is '
            'loaded: ssh-add -l',
            '  3. Add an explicit IdentityFile for this Host in '
            '~/.ssh/config, or pass a Host alias that has one.',
            '  4. If the key has a passphrase, Paramiko cannot prompt '
            'from this script; use ssh-agent or an unencrypted deploy key.',
        ]
        if cfg:
            lines.extend([
                '',
                'Relevant ~/.ssh/config keys Paramiko read:',
                *(
                    f'  {key}: {value}'
                    for key, value in sorted(cfg.items())
                    if key in {
                        'hostname', 'user', 'port',
                        'identityfile', 'proxycommand',
                    }
                ),
            ])
        return '\n'.join(lines)

    def _ssh_config(self, host: str, paramiko: Any) -> dict[str, Any]:
        simple_cfg: dict[str, Any] = self._simple_ssh_config(host)
        config = paramiko.SSHConfig()
        try:
            with open(
                Path.home() / '.ssh' / 'config',
                encoding='utf-8',
            ) as f:
                config.parse(f)
        except FileNotFoundError:
            return simple_cfg
        try:
            paramiko_cfg: dict[str, Any] = config.lookup(host)
        except KeyError:
            paramiko_cfg = {}
        return simple_cfg | paramiko_cfg

    def _simple_ssh_config(self, host: str) -> dict[str, Any]:
        path: Path = Path.home() / '.ssh' / 'config'
        try:
            lines: list[str] = path.read_text(
                encoding='utf-8',
            ).splitlines()
        except FileNotFoundError:
            return {}

        cfg: dict[str, Any] = {}
        active: bool = False
        for raw in lines:
            line: str = raw.strip()
            if not line or line.startswith('#'):
                continue
            if '#' in line:
                line = line.split('#', 1)[0].strip()
            parts: list[str] = line.split(None, 1)
            if not parts:
                continue
            key: str = parts[0].lower()
            value: str = parts[1].strip() if len(parts) > 1 else ''
            if key == 'host':
                patterns: list[str] = value.split()
                active = (
                    any(
                        fnmatch.fnmatchcase(host, pattern)
                        for pattern in patterns
                        if not pattern.startswith('!')
                    )
                    and not any(
                        fnmatch.fnmatchcase(host, pattern[1:])
                        for pattern in patterns
                        if pattern.startswith('!')
                    )
                )
                continue
            if not active:
                continue
            if key == 'hostname':
                cfg.setdefault('hostname', value)
            elif key == 'user':
                cfg.setdefault('user', value)
            elif key == 'port':
                cfg.setdefault('port', value)
            elif key == 'identityfile':
                cfg.setdefault('identityfile', []).append(value)
            elif key == 'proxycommand':
                cfg.setdefault('proxycommand', value)
            elif key == 'identitiesonly':
                cfg.setdefault('identitiesonly', value)
        return cfg


def list_remote_files(
    runner: HostRunner,
    host: str,
    directory: str,
) -> list[RemoteFile]:
    # Null-delimited output keeps spaces, quotes, and tabs in paths safe.
    if is_local_host(host):
        local_directory: str = str(Path(directory).expanduser())
        out: bytes = _run_local([
            'find', local_directory,
            '-maxdepth', '1',
            '-type', 'f',
            '-printf', r'%T@\t%p\0',
        ])
    else:
        dir_arg: str = _remote_directory_arg(directory)
        remote_command: str = (
            f'find {dir_arg} -maxdepth 1 -type f '
            r"-printf '%T@\t%p\0'"
        )
        out = runner.run_remote(host, remote_command)
    files: list[RemoteFile] = []
    for raw in out.split(b'\0'):
        if not raw:
            continue
        try:
            mtime_raw, path_raw = raw.split(b'\t', 1)
        except ValueError as exc:
            raise RuntimeError(
                f'unparseable find output from {host!r}: '
                f'{raw!r}'
            ) from exc
        files.append(
            RemoteFile(
                host=host,
                mtime=float(mtime_raw.decode('ascii')),
                path=path_raw.decode('utf-8', 'surrogateescape'),
            )
        )
    return files


def plan_deletes(files: list[RemoteFile]) -> dict[str, DuplicateGroup]:
    by_name: dict[str, list[RemoteFile]] = {}
    for item in files:
        by_name.setdefault(item.name, []).append(item)

    deletes: dict[str, DuplicateGroup] = {}
    for name, matches in by_name.items():
        if len(matches) < 2:
            continue
        ordered: list[RemoteFile] = sorted(
            matches,
            key=lambda item: (item.mtime, item.host, item.path),
            reverse=True,
        )
        deletes[name] = DuplicateGroup(
            keep=ordered[0],
            delete=ordered[1:],
        )
    return deletes


def _delete_remote_files_batch(
    runner: HostRunner,
    host: str,
    paths: list[str],
) -> int:
    if not paths:
        return 0
    if is_local_host(host):
        _run_local(
            ['rm', '-f', '--', *paths],
        )
        return len(paths)

    remote_command: str = shlex.join([
        'rm', '-f', '--', *paths,
    ])
    runner.run_remote(host, remote_command)
    return len(paths)


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _status_text(
    hosts: list[str],
    original_by_host: dict[str, int],
    planned_delete_by_host: dict[str, int],
    actual_delete_by_host: dict[str, int] | None,
    *,
    phase: str,
) -> str:
    parts: list[str] = []
    counter_label: str = (
        'deleted' if actual_delete_by_host is not None else 'planned'
    )
    for host in hosts:
        original: int = original_by_host.get(host, 0)
        count: int = (
            actual_delete_by_host.get(host, 0)
            if actual_delete_by_host is not None
            else planned_delete_by_host.get(host, 0)
        )
        parts.append(
            f'{host}:{original} original/'
            f'{count} {counter_label}/{original - count} left'
        )
    return f'{phase} | ' + ' | '.join(parts)


def update_status_line(
    hosts: list[str],
    original_by_host: dict[str, int],
    planned_delete_by_host: dict[str, int],
    actual_delete_by_host: dict[str, int] | None,
    *,
    phase: str,
    final: bool = False,
) -> None:
    global _STATUS_LINE_LEN

    text: str = _status_text(
        hosts,
        original_by_host,
        planned_delete_by_host,
        actual_delete_by_host,
        phase=phase,
    )
    clear_line: str = '\033[K' if sys.stderr.isatty() else ''
    padding: str = (
        ''
        if clear_line
        else ' ' * max(_STATUS_LINE_LEN - len(text), 0)
    )
    sys.stderr.write('\r' + clear_line + text + padding)
    if final:
        sys.stderr.write('\n')
        _STATUS_LINE_LEN = 0
    else:
        _STATUS_LINE_LEN = max(_STATUS_LINE_LEN, len(text))
    sys.stderr.flush()


def clear_status_line() -> None:
    global _STATUS_LINE_LEN

    clear_line: str = '\033[K' if sys.stderr.isatty() else ''
    padding: str = (
        ''
        if clear_line
        else ' ' * _STATUS_LINE_LEN
    )
    sys.stderr.write('\r' + clear_line + padding + '\r')
    sys.stderr.flush()
    _STATUS_LINE_LEN = 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Find duplicate basenames across remote scraped-video '
            'directories and keep the newest mtime.'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--host',
        action='append',
        dest='hosts',
        help='SSH host to inspect. Repeat for multiple hosts.',
    )
    parser.add_argument(
        '--directory',
        default=DEFAULT_DIR,
        help='Remote directory to inspect non-recursively.',
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Actually delete older duplicates. Default is dry-run.',
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Print only summary information.',
    )
    parser.add_argument(
        '--delete-batch-size',
        type=int,
        default=DELETE_BATCH_SIZE,
        help=(
            'Maximum paths to delete per local rm command or '
            'single SSH rm command.'
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser: argparse.ArgumentParser = _build_parser()
    args: argparse.Namespace = parser.parse_args(argv)
    hosts: list[str] = args.hosts
    runner = HostRunner()

    try:
        all_files: list[RemoteFile] = []
        original_by_host: dict[str, int] = {}
        planned_delete_by_host: dict[str, int] = {
            host: 0 for host in hosts
        }
        update_status_line(
            hosts,
            original_by_host,
            planned_delete_by_host,
            None,
            phase='scanning',
        )
        for host in hosts:
            files: list[RemoteFile] = list_remote_files(
                runner, host, args.directory,
            )
            original_by_host[host] = len(files)
            all_files.extend(files)
            update_status_line(
                hosts,
                original_by_host,
                planned_delete_by_host,
                None,
                phase=f'scanned {host}',
            )

        deletes: dict[str, DuplicateGroup] = plan_deletes(all_files)
        delete_count: int = sum(
            len(group.delete) for group in deletes.values()
        )
        update_status_line(
            hosts,
            original_by_host,
            planned_delete_by_host,
            None,
            phase=(
                f'planned {len(deletes)} duplicate names/'
                f'{delete_count} older files'
            ),
        )

        delete_by_host: dict[str, list[str]] = {}
        for index, name in enumerate(sorted(deletes), start=1):
            group: DuplicateGroup = deletes[name]
            keep: RemoteFile = group.keep
            if not args.quiet:
                print(
                    f'KEEP\t{keep.host}\t{keep.mtime:.6f}\t{keep.path}'
                )
            for item in sorted(
                group.delete,
                key=lambda item: (item.host, item.path),
            ):
                if not args.quiet:
                    print(
                        f'DELETE\t{item.host}\t{item.mtime:.6f}\t'
                        f'{item.path}'
                    )
                if args.delete:
                    delete_by_host.setdefault(item.host, []).append(
                        item.path,
                    )
                planned_delete_by_host[item.host] = (
                    planned_delete_by_host.get(item.host, 0) + 1
                )
            if args.quiet and index % 1000 == 0:
                update_status_line(
                    hosts,
                    original_by_host,
                    planned_delete_by_host,
                    None,
                    phase='planning',
                )

        actual_delete_by_host: dict[str, int] | None = None
        if args.delete:
            actual_delete_by_host = {host: 0 for host in hosts}
            for host, paths in sorted(delete_by_host.items()):
                update_status_line(
                    hosts,
                    original_by_host,
                    planned_delete_by_host,
                    actual_delete_by_host,
                    phase=f'deleting {host}',
                )
                for batch in _chunks(
                    paths, max(args.delete_batch_size, 1),
                ):
                    deleted: int = _delete_remote_files_batch(
                        runner, host, batch,
                    )
                    actual_delete_by_host[host] += deleted
                    update_status_line(
                        hosts,
                        original_by_host,
                        planned_delete_by_host,
                        actual_delete_by_host,
                        phase=f'deleting {host}',
                    )

        update_status_line(
            hosts,
            original_by_host,
            planned_delete_by_host,
            actual_delete_by_host,
            phase='done' if args.delete else 'done dry-run',
            final=True,
        )
    except SshAuthError as exc:
        clear_status_line()
        print(str(exc), file=sys.stderr)
        return 2
    finally:
        runner.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
