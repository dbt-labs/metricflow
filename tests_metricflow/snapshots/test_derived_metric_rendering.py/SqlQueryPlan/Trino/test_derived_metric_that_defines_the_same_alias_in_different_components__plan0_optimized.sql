test_name: test_derived_metric_that_defines_the_same_alias_in_different_components
test_filename: test_derived_metric_rendering.py
docstring:
  Tests querying a derived metric which give the same alias to its components.
sql_engine: Trino
---
-- Combine Aggregated Outputs
-- Order By [] Limit 1
WITH cm_8_cte AS (
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
  ) subq_15
  GROUP BY
    booking__is_instant
)

, cm_9_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias - 10 AS derived_shared_alias_1a
  FROM (
    -- Read From CTE For node_id=cm_8
    SELECT
      booking__is_instant
      , shared_alias
    FROM cm_8_cte cm_8_cte
  ) subq_17
)

, cm_10_cte AS (
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
  ) subq_21
  GROUP BY
    booking__is_instant
)

, cm_11_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias + 10 AS derived_shared_alias_2
  FROM (
    -- Read From CTE For node_id=cm_10
    SELECT
      booking__is_instant
      , shared_alias
    FROM cm_10_cte cm_10_cte
  ) subq_23
)

SELECT
  COALESCE(cm_9_cte.booking__is_instant, cm_11_cte.booking__is_instant) AS booking__is_instant
  , MAX(cm_9_cte.derived_shared_alias_1a) AS derived_shared_alias_1a
  , MAX(cm_11_cte.derived_shared_alias_2) AS derived_shared_alias_2
FROM cm_9_cte cm_9_cte
FULL OUTER JOIN
  cm_11_cte cm_11_cte
ON
  cm_9_cte.booking__is_instant = cm_11_cte.booking__is_instant
GROUP BY
  COALESCE(cm_9_cte.booking__is_instant, cm_11_cte.booking__is_instant)
LIMIT 1
