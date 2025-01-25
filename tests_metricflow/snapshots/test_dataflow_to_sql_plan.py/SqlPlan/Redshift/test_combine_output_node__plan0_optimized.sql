test_name: test_combine_output_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests combining AggregateMeasuresNode.
sql_engine: Redshift
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_2.is_instant, subq_3.is_instant) AS is_instant
  , MAX(subq_2.bookings) AS bookings
  , COALESCE(MAX(subq_3.instant_bookings), 1) AS instant_bookings
  , COALESCE(MAX(subq_3.bookers), 1) AS bookers
FROM (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Pass Only Elements: ['bookings', 'is_instant']
    SELECT
      is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_1
  GROUP BY
    is_instant
) subq_2
FULL OUTER JOIN (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(instant_bookings) AS instant_bookings
    , COUNT(DISTINCT bookers) AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Pass Only Elements: ['instant_bookings', 'bookers', 'is_instant']
    SELECT
      is_instant
      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_3
  GROUP BY
    is_instant
) subq_3
ON
  subq_2.is_instant = subq_3.is_instant
GROUP BY
  COALESCE(subq_2.is_instant, subq_3.is_instant)
