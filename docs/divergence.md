# Where a distributed answer may differ from icetl's

icetl is the oracle (PLAN P1): a distributed answer must equal the single-node one. This
file lists the only places where it may not be *bit for bit* equal, and why that is
accepted (PLAN decision 14). Anything not listed here is a bug.

| # | Since | What | Why | How the tests compare |
|---|---|---|---|---|
| 1 | Phase 2 | `sum`, `avg` of `DOUBLE` / `FLOAT` differ in the last digits | The partial stage adds each split's values, the final stage adds the partial sums: the same numbers, added in a different order. Floating-point addition is not associative. icetl's own FINDINGS 3.7 already tolerates this between DuckDB thread counts. | relative tolerance `1e-9` on floating-point values; exact on everything else (`py_test/oracle.py`) |
| 2 | Phase 2 | `avg` of an integer or `DECIMAL` column may differ in the last digit of its `DOUBLE` | Computed as `CAST(sum(sums) AS DOUBLE) / sum(counts)` instead of DuckDB's internal average. The sum is exact (`HUGEINT` / `DECIMAL(38,s)`); only the final division rounds. | as 1 |
| 3 | Phase 2 | `first`, `last`, `any_value`, `list` / `string_agg` *without* `ORDER BY` return some value or order | Nondeterministic on the reference too. These run on the raw path (rows co-located by key, aggregate as written), which keeps them exactly as nondeterministic as DuckDB single-node, no more. | tests use only their ordered forms |
| 4 | Phase 2 | `LIMIT n` without `ORDER BY` returns *some* n rows; ties under `ORDER BY ... LIMIT n` may be broken differently | As on the reference (PLAN §4.7). | tests check membership, or order by unique keys |

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
