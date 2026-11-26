"""The integration tier as the coordinator step of an Argo fleet (kickoff answer 9).

    .\\scripts\\image.ps1 -Test -Load
    .\\scripts\\k8s.ps1 run integration.py image=icefleet-test:dev
    .\\scripts\\k8s.ps1 run integration.py image=icefleet-test:dev "script-args=-m integration"

Runs `py_test/integration` from the test image (`/opt/icefleet-tests`) against the
workflow's own daemon workers: the tier's conftest takes the k8s:// master the template
sets. Default selection: `integration and not slow`; any arguments go to pytest.
"""

from __future__ import annotations

import os
import sys

import pytest


def main() -> int:
    os.chdir(os.environ.get("ICEFLEET_TESTS", "/opt/icefleet-tests"))
    extra = sys.argv[1:]
    markers = [] if "-m" in extra else ["-m", "integration and not slow"]
    return int(
        pytest.main(["py_test/integration", *markers, "-p", "no:cacheprovider", "-rs", *extra])
    )


if __name__ == "__main__":
    sys.exit(main())
