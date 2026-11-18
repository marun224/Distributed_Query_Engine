# Where a distributed answer may differ from icetl's

icetl is the oracle (PLAN P1): a distributed answer must equal the single-node one. This
file lists the only places where it may not be *bit for bit* equal, and why that is
accepted (PLAN decision 14). Anything not listed here is a bug.

| # | Since | What | Why | How the tests compare |
|---|---|---|---|---|
| 1 | Phase 2 | `sum`, `avg` of `DOUBLE` / `FLOAT` differ in the last digits | The partial stage adds each split's values, the final stage adds the partial sums: the same numbers, added in a different order. Floating-point addition is not associative. icetl's own FINDINGS 3.7 already tolerates this between DuckDB thread counts. | relative tolerance `1e-9` on floating-point values; exact on everything else (`py_test/oracle.py`) |
| 2 | Phase 2 | `avg` of an integer or `DECIMAL` column may differ in the last digit of its `DOUBLE` | Computed as `CAST(sum(sums) AS DOUBLE) / sum(counts)` instead of DuckDB's internal average. The sum is exact (`HUGEINT` / `DECIMAL(38,s)`); only the final division rounds. | as 1 |
| 3 | Phase 2 | `first`, `last`, `any_value`, `list` / `string_agg` *without* `ORDER BY` return some value or order | Nondeterministic on the reference too. These run on the raw path (rows co-located by key, aggregate as written), which keeps them exactly as nondeterministic as DuckDB single-node, no more. | tests use only their ordered forms |
| 4 | Phase 2 | `LIMIT n` without `ORDER BY` returns *some* n rows; ties under `ORDER BY ... LIMIT n` may be broken differently; rows tied under a range-sorted `ORDER BY` (Phase 4) may come in another order | As on the reference (PLAN §4.7). | tests check membership, or order by unique keys, or check sortedness and the same rows |
| 5 | Phase 4 | `dropDuplicates(subset)` / `DISTINCT ON` without an `ORDER BY` keeps *some* row per key; a window function whose value depends on the order of tied rows (`row_number()` over a non-unique order) numbers the ties in some order | Nondeterministic on the reference too (icetl says so of `dropDuplicates`). With an `ORDER BY`, `DISTINCT ON` keeps the first row of it, as single-node: that `ORDER BY` stays in its SELECT. | keys and membership only; windows are tested over unique orders |

Differences from icetl, chosen (not distribution's):

- **A failed `saveAsTable` leaves no table** (Phase 5, kickoff answer 5): a table the write
  created is dropped again when filling it fails, as the reference does. icetl leaves
  the empty table it created. A table that existed before is never dropped.

Not divergences, by construction:

- **Result types.** Every decomposed aggregate is cast back to the type DuckDB gives it
  single-node (`sum(INTEGER)` is `HUGEINT`), and columns whose type does not survive
  parquet cross the exchange as a lossless stand-in and are cast back
  (`icefleet/plan/types.py`). The Arrow schema of a distributed result equals icetl's;
  every oracle test asserts it.
- **A `HUGEINT` past 38 digits** cannot cross the exchange as `DECIMAL(38,0)`: the query
  *fails* with DuckDB's conversion error rather than returning a different number (P3:
  degrade, never fail wrong).
- **NULL group keys** form one group, as single-node: `hash(NULL)` is one value.
- **Writes** (Phase 5) accept and refuse what icetl's writer does -- PyIceberg's own schema
  check runs first -- create tables with icetl's schema (the same Arrow types, through
  `iceberg_ready`), and make icetl's snapshots: an append one, an overwrite a delete and
  an append.
- **NULL join keys** (Phase 3) never match under `=` and always match under
  `IS NOT DISTINCT FROM`, as single-node; a row an outer join keeps for a NULL key is
  emitted once, by the one task its partition went to. Only a side whose unmatched rows
  are dropped is ever broadcast, so no unmatched row is emitted once per task.
- **Join keys of different types** (Phase 3) are hashed as one type both sides convert to
  exactly (or, for `DOUBLE`, as DuckDB's own comparison converts them), so keys that
  compare equal always meet; keys with no such type are not shuffled at all.
- **`x IN (SELECT y ...)`** (Phase 3) runs as a semi join on `x = y`. In a WHERE, NULL and
  false both drop the row, so the two agree on every input, NULLs included
  (`test_rel.py::test_the_semi_join_returns_what_in_returns_nulls_included`).
