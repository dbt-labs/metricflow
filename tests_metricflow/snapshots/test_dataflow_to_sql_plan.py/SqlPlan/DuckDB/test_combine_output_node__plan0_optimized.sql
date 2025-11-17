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
    1 AS __bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
    , guest_id AS __bookers
    , is_instant
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COALESCE(subq_8.is_instant, subq_11.is_instant) AS is_instant
  , MAX(subq_8.__bookings) AS __bookings
  , COALESCE(MAX(subq_11.__instant_bookings), 1) AS __instant_bookings
  , COALESCE(MAX(subq_11.__bookers), 1) AS __bookers
FROM (
  -- Read From CTE For node_id=rss_28001
  -- Pass Only Elements: ['__bookings', 'is_instant']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    is_instant
    , SUM(__bookings) AS __bookings
  FROM rss_28001_cte
  GROUP BY
    is_instant
) subq_8
FULL OUTER JOIN (
  -- Read From CTE For node_id=rss_28001
  -- Pass Only Elements: ['__instant_bookings', '__bookers', 'is_instant']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    is_instant
    , SUM(__instant_bookings) AS __instant_bookings
    , COUNT(DISTINCT __bookers) AS __bookers
  FROM rss_28001_cte
  GROUP BY
    is_instant
) subq_11
ON
  subq_8.is_instant = subq_11.is_instant
GROUP BY
  COALESCE(subq_8.is_instant, subq_11.is_instant)
