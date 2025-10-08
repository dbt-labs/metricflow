test_name: test_derived_metric_that_defines_the_same_alias_in_different_components
test_filename: test_derived_metric_rendering.py
docstring:
  Tests querying a derived metric which give the same alias to its components.
sql_engine: Redshift
---
-- Combine Aggregated Outputs
-- Order By [] Limit 1
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    is_instant AS booking__is_instant
    , 1 AS bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COALESCE(subq_18.booking__is_instant, subq_23.booking__is_instant) AS booking__is_instant
  , MAX(subq_18.derived_shared_alias_1a) AS derived_shared_alias_1a
  , MAX(subq_23.derived_shared_alias_2) AS derived_shared_alias_2
FROM (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias - 10 AS derived_shared_alias_1a
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'booking__is_instant']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(bookings) AS shared_alias
    FROM sma_28009_cte
    GROUP BY
      booking__is_instant
  ) subq_17
) subq_18
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias + 10 AS derived_shared_alias_2
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['instant_bookings', 'booking__is_instant']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(instant_bookings) AS shared_alias
    FROM sma_28009_cte
    GROUP BY
      booking__is_instant
  ) subq_22
) subq_23
ON
  subq_18.booking__is_instant = subq_23.booking__is_instant
GROUP BY
  COALESCE(subq_18.booking__is_instant, subq_23.booking__is_instant)
LIMIT 1
