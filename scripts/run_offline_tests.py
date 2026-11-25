"""Run pytest with network/credential guards installed before collection.

Usage: python -B scripts/run_offline_tests.py [pytest paths/options]
Integration-marked tests are always excluded. No application configuration is written.
"""

from __future__ import annotations

import os
import socket
import sys
import threading
import uuid
from pathlib import Path
from typing import Any, NoReturn
from unittest.mock import patch


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    temp_root = root / "tmp"
    temp_root.mkdir(exist_ok=True)
    run_temp = (temp_root / f"offline-pytest-{uuid.uuid4().hex}").resolve()
    # pytest may clear its base directory; restrict it to this newly allocated run.
    run_temp.relative_to(temp_root.resolve())
    run_temp.mkdir()
    attempts: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> NoReturn:
        attempts.append("external dependency")
        raise AssertionError("Network, credential or secret access forbidden in offline tests")

    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_socketpair = socket.socketpair
    original_mkdir = os.mkdir
    local = threading.local()

    def mkdir(path: Any, mode: int = 0o777, *, dir_fd: Any = None) -> None:
        # Python 3.13+ Windows mode=0700 replaces inherited ACLs and excludes the
        # restricted token used by this workspace. Keep inherited ACLs only for
        # this run's synthetic test artifacts, without changing existing ACLs.
        if os.name == "nt" and mode == 0o700 and Path(path).resolve().is_relative_to(run_temp):
            mode = 0o777
        original_mkdir(path, mode, dir_fd=dir_fd)

    def socketpair(*args: Any, **kwargs: Any) -> tuple[socket.socket, socket.socket]:
        # Windows builds asyncio's wakeup pipe using an ephemeral loopback pair.
        local.socketpair = True
        try:
            return original_socketpair(*args, **kwargs)
        finally:
            local.socketpair = False

    def connect(sock: socket.socket, address: Any) -> None:
        if sock.family in (socket.AF_INET, socket.AF_INET6) and not (
            getattr(local, "socketpair", False) and address[0] in ("127.0.0.1", "::1")
        ):
            forbidden()
        return original_connect(sock, address)

    def connect_ex(sock: socket.socket, address: Any) -> int:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            return forbidden()
        return original_connect_ex(sock, address)

    # Process-local defaults prevent .env/real credentials from selecting live work.
    os.environ.update({
        "GCP_PROJECT_ID": "offline-project",
        "LITELLM_PROXY_BASE_URL": "http://127.0.0.1:9",
        "LITELLM_PROXY_API_KEY": "offline-dummy",
        "TELEGRAM_BOT_TOKEN": "offline-dummy",
        "E2B_API_KEY": "offline-dummy",
        "APIFY_API_TOKEN": "offline-dummy",
        "PUBLICATION_LEDGER_ENABLED": "false",
        "LANGFUSE_TRACING_ENABLED": "false",
        "LITELLM_LOCAL_MODEL_COST_MAP": "True",
        "ENVIRONMENT": "development",
    })
    with (
        patch.object(socket.socket, "connect", connect),
        patch.object(socket.socket, "connect_ex", connect_ex),
        patch.object(socket, "socketpair", socketpair),
        patch.object(socket, "getaddrinfo", forbidden),
        patch.object(socket, "create_connection", forbidden),
        patch.object(socket.socket, "sendto", forbidden),
        patch.object(os, "mkdir", mkdir),
    ):
        import google.auth
        import grpc
        import grpc.aio
        import pytest

        with (
            patch.object(google.auth, "default", forbidden),
            patch.object(grpc, "secure_channel", forbidden),
            patch.object(grpc, "insecure_channel", forbidden),
            patch.object(grpc.aio, "secure_channel", forbidden),
            patch.object(grpc.aio, "insecure_channel", forbidden),
        ):
            import config.settings as settings

            settings.Settings.model_config["env_file"] = None
            with patch.object(settings, "_fetch_secret", forbidden):
                result = pytest.main([*sys.argv[1:], "-p", "no:cacheprovider",
                                      "--basetemp", str(run_temp / "pytest"),
                                      "-m", "not integration"])
    print(f"Offline guard: {len(attempts)} forbidden dependency attempts")
    return 1 if attempts else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
