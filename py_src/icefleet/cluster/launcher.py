"""`local[N]`: N worker subprocesses on 127.0.0.1 (decision 4, what survives of it).

Each worker binds port 0 (the OS picks a free one) and writes the port to a file the
launcher polls for; loopback only, so no firewall prompt. DuckDB's threads and memory are
divided between the N processes so they do not oversubscribe the machine. Workers are
terminated on `session.stop()` and, as a backstop, at interpreter exit.
"""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import IO

from icefleet.errors import ClusterError

__all__ = ["LocalCluster", "physical_memory_bytes"]


def physical_memory_bytes() -> int | None:
    """Total RAM, where the platform says (Linux and macOS); None elsewhere."""
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, ValueError, OSError):
        return None


class LocalCluster:
    """Starts and stops N local workers."""

    def __init__(
        self,
        workers: int,
        *,
        slots: int,
        threads: int | None = None,
        memory_limit: str | None = None,
        startup_timeout: float = 60.0,
    ) -> None:
        self.workers = workers
        self.slots = slots
        cores = os.cpu_count() or 1
        self.threads = threads or max(1, cores // workers)
        if memory_limit is None:
            total = physical_memory_bytes()
            # Half the machine for the fleet; the coordinator and the OS keep the rest.
            memory_limit = f"{max(256, total // 2 // workers // 1024**2)}MiB" if total else None
        self.memory_limit = memory_limit
        self.startup_timeout = startup_timeout
        self.processes: list[subprocess.Popen[bytes]] = []
        self._logs: list[IO[bytes]] = []
        self._dir: Path | None = None
        self.endpoints: list[str] = []

    def start(self) -> list[str]:
        """Start the workers and wait until each is listening. Returns their endpoints."""
        self._dir = Path(tempfile.mkdtemp(prefix="icefleet-local-"))
        atexit.register(self.stop)
        try:
            for index in range(self.workers):
                self._spawn(index)
            self.endpoints = [self._await_port(index) for index in range(self.workers)]
        except BaseException:
            self.stop()
            raise
        return self.endpoints

    def _spawn(self, index: int) -> None:
        assert self._dir is not None
        command = [
            sys.executable, "-m", "icefleet.worker", "serve",
            "--host", "127.0.0.1", "--port", "0",
            "--slots", str(self.slots),
            "--threads", str(self.threads),
            "--temp-dir", str(self._dir / f"spill-{index}"),
            "--port-file", str(self._dir / f"worker-{index}.port"),
        ]  # fmt: skip
        if self.memory_limit:
            command += ["--memory-limit", self.memory_limit]
        log = open(self._dir / f"worker-{index}.log", "wb")  # noqa: SIM115 - closed in stop()
        self._logs.append(log)
        self.processes.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT))

    def _await_port(self, index: int) -> str:
        assert self._dir is not None
        port_file = self._dir / f"worker-{index}.port"
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if port_file.exists():
                return f"127.0.0.1:{int(port_file.read_text(encoding='utf-8'))}"
            if self.processes[index].poll() is not None:
                raise ClusterError(
                    f"local worker {index} exited with code {self.processes[index].returncode} "
                    f"before listening:\n{self._log_tail(index)}"
                )
            time.sleep(0.05)
        raise ClusterError(
            f"local worker {index} did not start within {self.startup_timeout:.0f}s:\n"
            f"{self._log_tail(index)}"
        )

    def _log_tail(self, index: int, lines: int = 20) -> str:
        if self._dir is None:
            return ""
        path = self._dir / f"worker-{index}.log"
        if not path.exists():
            return "(no log)"
        text = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(text[-lines:])

    def stop(self) -> None:
        """Terminate every worker and remove the launcher's directory. Safe to call twice."""
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        for log in self._logs:
            log.close()
        self._logs = []
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)
            self._dir = None
        atexit.unregister(self.stop)
