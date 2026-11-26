"""The image's own smoke test, run by scripts/image.ps1 with networking off.

    docker run --rm --network none --entrypoint python icefleet:dev \
        /opt/icefleet-image/check_image.py

Checks what a pod needs and must not fetch: the engine imports (pyarrow's Parquet and
Dataset modules included -- the ones Windows blocks), httpfs loads with no network, the
worker CLI answers, and the process is not root.
"""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    import duckdb
    import icetl
    import pyarrow
    import pyarrow.dataset
    import pyarrow.parquet
    import pyiceberg
    from icetl.sql import Session  # noqa: F401 -- the coordinator's import

    import icefleet
    from icefleet.session import ClusterSession  # noqa: F401

    connection = duckdb.connect()
    connection.execute("INSTALL httpfs")  # must find it in place: there is no network
    connection.execute("LOAD httpfs")

    worker = subprocess.run(
        ["icefleet-worker", "--version"], capture_output=True, text=True, check=True
    )
    uid = os.getuid()
    if uid == 0:
        print("check_image: running as root", file=sys.stderr)
        return 1
    print(
        f"check_image: OK -- icefleet {icefleet.__version__}, icetl {icetl.__version__}, "
        f"duckdb {duckdb.__version__} (httpfs loaded offline), pyarrow {pyarrow.__version__}, "
        f"pyiceberg {pyiceberg.__version__}, uid {uid}, {worker.stdout.strip()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
