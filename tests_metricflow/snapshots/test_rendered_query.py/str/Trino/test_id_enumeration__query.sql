test_name: test_id_enumeration
test_filename: test_rendered_query.py
sql_engine: Trino
---
-- Combine Aggregated Outputs
WITH cm_2_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10000
  ) subq_2
  GROUP BY
    metric_time__day
)

, cm_3_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', created_at) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_10000
  ) subq_7
  GROUP BY
    metric_time__day
)

SELECT
  COALESCE(cm_2_cte.metric_time__day, cm_3_cte.metric_time__day) AS metric_time__day
  , MAX(cm_2_cte.bookings) AS bookings
  , MAX(cm_3_cte.listings) AS listings
FROM cm_2_cte cm_2_cte
FULL OUTER JOIN
  cm_3_cte cm_3_cte
ON
  cm_2_cte.metric_time__day = cm_3_cte.metric_time__day
GROUP BY
  COALESCE(cm_2_cte.metric_time__day, cm_3_cte.metric_time__day)
