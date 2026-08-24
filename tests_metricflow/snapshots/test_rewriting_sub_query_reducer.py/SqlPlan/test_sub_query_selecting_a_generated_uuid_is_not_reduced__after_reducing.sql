test_name: test_sub_query_selecting_a_generated_uuid_is_not_reduced
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  A sub-query that materializes a non-deterministic value must not be collapsed.

  GEN_RANDOM_UUID() selected under an alias is evaluated once; substituting the
  expression into the outer query re-evaluates it at every reference site. This is
  the mechanism behind the conversion-metric overcount in issue #2111, where the
  de-duplication window went from partitioning by a materialized mf_internal_uuid
  column to partitioning by a fresh GEN_RANDOM_UUID() per row.
---
-- outer_query
SELECT
  src1.mf_internal_uuid
  , src1.visits
FROM (
  -- uuid_source
  SELECT
    UUID() AS mf_internal_uuid
    , src0.visits
  FROM demo.fct_visits src0
) src1
