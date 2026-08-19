test_name: test_joined_sub_query_selecting_a_generated_uuid_is_not_reduced
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  The join-reduction path must also leave a non-deterministic sub-query in place.

  The joined source here is otherwise simple enough to reduce, so without the
  determinism check its GEN_RANDOM_UUID() column would be substituted into the
  outer query.
---
-- outer_query
SELECT
  visits_src.visits AS visits
  , uuid_src.mf_internal_uuid AS mf_internal_uuid
FROM demo.fct_visits visits_src
LEFT OUTER JOIN (
  -- uuid_source
  SELECT
    UUID() AS mf_internal_uuid
    , src0.visit_id
  FROM demo.fct_buys src0
) uuid_src
ON
  visits_src.visit_id = uuid_src.visit_id
