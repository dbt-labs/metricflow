test_name: test_derived_metric_that_defines_the_same_alias_in_different_components
test_filename: test_derived_metric_rendering.py
docstring:
  Tests querying a derived metric which give the same alias to its components.
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
-- Order By [] Limit 1
SELECT
  COALESCE(nr_subq_15.booking__is_instant, nr_subq_20.booking__is_instant) AS booking__is_instant
  , MAX(nr_subq_15.derived_shared_alias_1a) AS derived_shared_alias_1a
  , MAX(nr_subq_20.derived_shared_alias_2) AS derived_shared_alias_2
FROM (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias - 10 AS derived_shared_alias_1a
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(bookings) AS shared_alias
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'booking__is_instant']
      SELECT
        is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_12
    GROUP BY
      booking__is_instant
  ) nr_subq_14
) nr_subq_15
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias + 10 AS derived_shared_alias_2
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(instant_bookings) AS shared_alias
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['instant_bookings', 'booking__is_instant']
      SELECT
        is_instant AS booking__is_instant
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    GROUP BY
      booking__is_instant
  ) nr_subq_19
) nr_subq_20
ON
  nr_subq_15.booking__is_instant = nr_subq_20.booking__is_instant
GROUP BY
  booking__is_instant
LIMIT 1
