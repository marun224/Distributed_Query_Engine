"""Install DuckDB's httpfs extension from a staged file, at image build time.

    python install_httpfs.py /stage/httpfs.duckdb_extension.gz

The file is the one `scripts/image.ps1` downloaded on Windows for the locked DuckDB
version and linux_amd64. It lands in the running user's extension directory
(~/.duckdb/extensions/<version>/<platform>/), where icetl's `INSTALL httpfs` at run time
finds it and does nothing.
"""

from __future__ import annotations

import gzip
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb


def main(source: str) -> int:
    with tempfile.TemporaryDirectory() as scratch:
        extension = Path(scratch) / "httpfs.duckdb_extension"
        with gzip.open(source, "rb") as packed, extension.open("wb") as plain:
            shutil.copyfileobj(packed, plain)
        connection = duckdb.connect()
        connection.execute(f"INSTALL '{extension}'")
        connection.execute("LOAD httpfs")
        row = connection.execute(
            "SELECT extension_version, install_path FROM duckdb_extensions() "
            "WHERE extension_name = 'httpfs'"
        ).fetchone()
    print(f"install_httpfs: duckdb {duckdb.__version__}, httpfs {row}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
