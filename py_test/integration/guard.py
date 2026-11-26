"""What makes it safe to run write tests against the live dev catalog (icetl's pattern).

Three layers, in the order they fire:

  1. `it_namespace()` refuses to resolve to a protected namespace, so a stray
     `ICEFLEET_IT_NAMESPACE=nyc` fails before a table is built.
  2. `safe_identifier()` / `safe_drop()` refuse any table outside that namespace.
  3. `Witness` reads every protected table's snapshot id, record count and schema id
     before the run and after it, and fails the run if anything moved -- the backstop
     that does not depend on the first two being right.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

__all__ = [
    "PROTECTED_NAMESPACES",
    "ProtectedNamespaceError",
    "Witness",
    "ensure_namespace",
    "it_namespace",
    "safe_drop",
    "safe_identifier",
]

PROTECTED_NAMESPACES = frozenset(
    part.strip()
    for part in os.environ.get("ICEFLEET_IT_PROTECTED", "nyc,amazon").split(",")
    if part.strip()
)
DEFAULT_IT_NAMESPACE = "icefleet_it"


class ProtectedNamespaceError(AssertionError):
    """Something addressed a namespace the suite does not own (a harness bug)."""


def it_namespace() -> str:
    name = os.environ.get("ICEFLEET_IT_NAMESPACE", DEFAULT_IT_NAMESPACE).strip()
    if not name or name in PROTECTED_NAMESPACES:
        raise ProtectedNamespaceError(
            f"ICEFLEET_IT_NAMESPACE={name!r} is empty or protected "
            f"({', '.join(sorted(PROTECTED_NAMESPACES))}); the suite creates and drops "
            f"tables in it."
        )
    return name


def safe_identifier(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 2 or parts[0] != it_namespace():
        raise ProtectedNamespaceError(f"{name!r} is not a table in {it_namespace()!r}.")
    return name


def safe_drop(catalog: Catalog, name: str) -> None:
    """Purge a table the suite owns, if it exists (teardown: never raises past the check)."""
    safe_identifier(name)
    try:
        if catalog.table_exists(name):
            catalog.purge_table(name)
    except Exception:
        pass


def ensure_namespace(catalog: Catalog) -> str:
    namespace = it_namespace()
    catalog.create_namespace_if_not_exists(namespace)
    return namespace


@dataclass(frozen=True)
class _State:
    snapshot_id: int | None
    records: str | None
    schema_id: int


@dataclass
class Witness:
    """Before-and-after proof that no protected table was touched (metadata only)."""

    catalog: Catalog
    before: dict[str, _State]

    @classmethod
    def capture(cls, catalog: Catalog) -> Witness:
        return cls(catalog=catalog, before=_read_protected(catalog))

    def verify(self) -> None:
        after = _read_protected(self.catalog)
        moved = [
            f"{name}: {self.before[name]} -> {after.get(name)}"
            for name in sorted(self.before)
            if after.get(name) != self.before[name]
        ]
        appeared = sorted(set(after) - set(self.before))
        if moved or appeared:
            raise ProtectedNamespaceError(
                "the integration run touched tables it does not own: "
                + "; ".join(moved + [f"{name} appeared" for name in appeared])
            )


def _read_protected(catalog: Catalog) -> dict[str, _State]:
    state: dict[str, _State] = {}
    for parts in catalog.list_namespaces():
        if ".".join(parts) not in PROTECTED_NAMESPACES:
            continue
        for identifier in catalog.list_tables(parts):
            table = catalog.load_table(identifier)
            snapshot = table.current_snapshot()
            state[".".join(identifier)] = _State(
                snapshot_id=snapshot.snapshot_id if snapshot else None,
                records=snapshot.summary.get("total-records")
                if snapshot is not None and snapshot.summary is not None
                else None,
                schema_id=table.schema().schema_id,
            )
    return state
