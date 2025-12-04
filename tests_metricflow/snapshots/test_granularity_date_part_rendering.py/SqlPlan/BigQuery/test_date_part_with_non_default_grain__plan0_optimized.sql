test_name: test_date_part_with_non_default_grain
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__archived_users', 'metric_time__extract_year']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__extract_year
  , SUM(archived_users) AS archived_users
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  -- Pass Only Elements: ['__archived_users', 'metric_time__extract_year', 'metric_time__extract_day']
  SELECT
    EXTRACT(year FROM archived_at) AS metric_time__extract_year
    , EXTRACT(day FROM archived_at) AS metric_time__extract_day
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_9
WHERE metric_time__extract_day = '2020-01-01'
GROUP BY
  metric_time__extract_year
