"""Join strategy helpers (PLAN §4.3): equality keys, outer-join NULLs, broadcast rules."""

from __future__ import annotations

from typing import cast

import sqlglot
from sqlglot import exp

from icefleet.plan.joins import BROADCASTABLE, equi_keys, is_deterministic, null_supplied


def condition(sql: str) -> exp.Expression:
    return cast(exp.Expression, sqlglot.parse_one(sql, dialect="duckdb"))


def rendered(keys: list[tuple[exp.Expression, exp.Expression]]) -> list[tuple[str, str]]:
    return [(a.sql(dialect="duckdb"), b.sql(dialect="duckdb")) for a, b in keys]


def test_equality_conjuncts_are_keys_oriented_left_to_right() -> None:
    on = condition("u.a = t.a AND t.b + 1 = u.b AND t.c > u.c AND t.d = 5")
    assert rendered(equi_keys(on, {"t"}, "u")) == [("t.a", "u.a"), ("t.b + 1", "u.b")]


def test_null_safe_equality_is_a_key() -> None:
    on = condition("t.a IS NOT DISTINCT FROM u.a")
    assert rendered(equi_keys(on, {"t"}, "u")) == [("t.a", "u.a")]


def test_a_key_may_read_every_relation_joined_so_far() -> None:
    on = condition("t.a + v.a = u.a AND u.b = u.c")
    assert rendered(equi_keys(on, {"t", "v"}, "u")) == [("t.a + v.a", "u.a")]


def test_no_keys_without_an_equality_across_the_sides() -> None:
    assert equi_keys(condition("t.a < u.a"), {"t"}, "u") == []
    assert equi_keys(None, {"t"}, "u") == []
    assert equi_keys(condition("t.a = random() + u.a"), {"t"}, "u") == []


def test_outer_joins_null_supply_the_right_relations() -> None:
    aliases = ["a", "b", "c", "d"]
    assert null_supplied(aliases, ["inner", "inner", "inner"]) == set()
    assert null_supplied(aliases, ["left", "inner", "inner"]) == {"b"}
    assert null_supplied(aliases, ["inner", "right", "inner"]) == {"a", "b"}
    assert null_supplied(aliases, ["inner", "inner", "full"]) == {"a", "b", "c", "d"}


def test_only_a_side_whose_unmatched_rows_are_dropped_may_be_broadcast() -> None:
    assert BROADCASTABLE["left"] == ("right",)
    assert BROADCASTABLE["right"] == ("left",)
    assert BROADCASTABLE["semi"] == ("right",)
    assert BROADCASTABLE["anti"] == ("right",)
    assert BROADCASTABLE["full"] == ()
    assert set(BROADCASTABLE["inner"]) == {"left", "right"}


def test_volatile_calls_are_not_moved() -> None:
    assert is_deterministic(condition("t.a > 1"))
    assert not is_deterministic(condition("t.a > random()"))
