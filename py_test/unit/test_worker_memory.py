"""The worker's memory check at start-up (PLAN §3.5): headroom inside a cgroup limit."""

from __future__ import annotations

from pathlib import Path

import pytest

from icefleet.config import GiB, MiB, parse_bytes
from icefleet.errors import ClusterConfigurationError
from icefleet.worker import cli
from icefleet.worker.memory import (
    cgroup_memory_limit,
    check_memory_limit,
    default_memory_limit,
)


def test_cgroup_v2_limit(tmp_path: Path) -> None:
    (tmp_path / "memory.max").write_text(f"{2 * GiB}\n")
    assert cgroup_memory_limit(tmp_path) == 2 * GiB


def test_cgroup_v2_unlimited(tmp_path: Path) -> None:
    (tmp_path / "memory.max").write_text("max\n")
    assert cgroup_memory_limit(tmp_path) is None


def test_cgroup_v1_limit_and_its_unlimited_sentinel(tmp_path: Path) -> None:
    v1 = tmp_path / "memory"
    v1.mkdir()
    (v1 / "memory.limit_in_bytes").write_text(f"{3 * GiB}\n")
    assert cgroup_memory_limit(tmp_path) == 3 * GiB
    (v1 / "memory.limit_in_bytes").write_text("9223372036854771712\n")
    assert cgroup_memory_limit(tmp_path) is None


def test_no_cgroup_files_means_no_limit(tmp_path: Path) -> None:
    assert cgroup_memory_limit(tmp_path) is None
    (tmp_path / "memory.max").write_text("garbage\n")
    assert cgroup_memory_limit(tmp_path) is None


@pytest.mark.parametrize(
    ("cgroup", "budget"),
    [
        (2 * GiB, 1 * GiB),  # 20% is 0.4 GiB: the 1 GiB floor wins
        (int(2.5 * GiB), int(1.5 * GiB)),
        (10 * GiB, 8 * GiB),  # 20% is 2 GiB, above the floor
    ],
)
def test_default_budget_keeps_twenty_percent_and_at_least_a_gib(cgroup: int, budget: int) -> None:
    assert default_memory_limit(cgroup) == budget


def test_a_pod_too_small_for_the_headroom_is_refused() -> None:
    with pytest.raises(ClusterConfigurationError, match="Give the pod more memory"):
        default_memory_limit(1 * GiB)


def test_outside_a_cgroup_nothing_changes() -> None:
    assert check_memory_limit(None, None) is None
    assert check_memory_limit("12GB", None) == "12GB"


def test_unset_inside_a_cgroup_gets_the_default_budget() -> None:
    chosen = check_memory_limit(None, int(2.5 * GiB))
    assert chosen == "1536MiB"
    assert parse_bytes(chosen) == int(1.5 * GiB)


def test_a_limit_with_room_to_spare_is_kept() -> None:
    assert check_memory_limit("1536MiB", int(2.5 * GiB)) == "1536MiB"
    # exactly 85% of the cgroup limit is still allowed
    assert check_memory_limit(f"{int(0.85 * 4 * GiB) // MiB}MiB", 4 * GiB) is not None


def test_a_limit_within_fifteen_percent_is_refused() -> None:
    with pytest.raises(ClusterConfigurationError, match="within 15%"):
        check_memory_limit("1900MiB", 2 * GiB)
    with pytest.raises(ClusterConfigurationError, match="within 15%"):
        check_memory_limit("3GB", 3 * GiB)


def test_serve_refuses_to_start_over_the_cgroup_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("icefleet.worker.memory.cgroup_memory_limit", lambda: 2 * GiB)
    code = cli.main(["serve", "--port", "0", "--memory-limit", "1950MiB"])
    assert code == 2
    assert "within 15%" in capsys.readouterr().err
