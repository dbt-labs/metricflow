test_name: test_combine_output_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests combining AggregateSimpleMetricInputsNode.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
WITH rss_28001_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    guest_id AS bookers
    , 1 AS bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    , is_instant
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COALESCE(subq_8.is_instant, subq_11.is_instant) AS is_instant
  , MAX(subq_8.bookings) AS bookings
  , COALESCE(MAX(subq_11.bookers), 1) AS bookers
  , COALESCE(MAX(subq_11.instant_bookings), 1) AS instant_bookings
FROM (
  -- Read From CTE For node_id=rss_28001
  -- Pass Only Elements: ['bookings', 'is_instant']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    is_instant
    , SUM(bookings) AS bookings
  FROM rss_28001_cte
  GROUP BY
    is_instant
) subq_8
FULL OUTER JOIN (
  -- Read From CTE For node_id=rss_28001
  -- Pass Only Elements: ['instant_bookings', 'bookers', 'is_instant']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    is_instant
    , COUNT(DISTINCT bookers) AS bookers
    , SUM(instant_bookings) AS instant_bookings
  FROM rss_28001_cte
  GROUP BY
    is_instant
) subq_11
ON
  subq_8.is_instant = subq_11.is_instant
GROUP BY
  COALESCE(subq_8.is_instant, subq_11.is_instant)
