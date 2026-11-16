from __future__ import annotations

from collections import Counter

from hypothesis import given
from hypothesis import strategies as st

from icefleet.plan.splits import SplitFile, plan_splits

MiB = 1024**2


def files(*sizes: int) -> list[SplitFile]:
    return [SplitFile(path=f"f{i:03d}.parquet", bytes=size) for i, size in enumerate(sizes)]


def test_no_files_no_splits() -> None:
    assert plan_splits([], target_bytes=MiB, min_splits=4, max_splits=10) == ()


def test_small_table_still_gives_every_slot_work() -> None:
    splits = plan_splits(files(*[1000] * 8), target_bytes=128 * MiB, min_splits=4, max_splits=100)
    assert len(splits) == 4
    assert {s.bytes for s in splits} == {2000}


def test_target_size_decides_when_it_asks_for_more() -> None:
    splits = plan_splits(
        files(*[10 * MiB] * 20), target_bytes=40 * MiB, min_splits=2, max_splits=100
    )
    assert len(splits) == 5
    assert all(s.bytes == 40 * MiB for s in splits)


def test_caps() -> None:
    assert len(plan_splits(files(1, 1, 1), target_bytes=1, min_splits=10, max_splits=100)) == 3
    assert len(plan_splits(files(*[MiB] * 50), target_bytes=1, min_splits=1, max_splits=6)) == 6


def test_balanced_within_two_x_for_skewed_sizes() -> None:
    sizes = [50, 40, 30, 20, 10, 10, 5, 5, 5, 5, 3, 3, 2, 1, 1, 1]
    splits = plan_splits(files(*sizes), target_bytes=1000, min_splits=4, max_splits=100)
    loads = [s.bytes for s in splits]
    assert max(loads) <= 2 * min(loads)


def test_deterministic() -> None:
    a = plan_splits(files(5, 3, 9, 1, 7), target_bytes=1, min_splits=3, max_splits=3)
    b = plan_splits(files(5, 3, 9, 1, 7), target_bytes=1, min_splits=3, max_splits=3)
    assert a == b


@given(
    sizes=st.lists(st.integers(min_value=0, max_value=10**9), min_size=1, max_size=200),
    target=st.integers(min_value=1, max_value=10**9),
    min_splits=st.integers(min_value=1, max_value=64),
    max_splits=st.integers(min_value=1, max_value=4096),
)
def test_every_file_lands_in_exactly_one_nonempty_split(
    sizes: list[int], target: int, min_splits: int, max_splits: int
) -> None:
    given_files = files(*sizes)
    splits = plan_splits(
        given_files, target_bytes=target, min_splits=min_splits, max_splits=max_splits
    )
    placed = Counter(f for split in splits for f in split.files)
    assert placed == Counter(given_files)
    assert all(split.files for split in splits)
    assert 1 <= len(splits) <= min(len(given_files), max_splits)
