test_name: test_simple_query_with_multiple_date_parts
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: [
  --   '__bookings',
  --   'metric_time__extract_day',
  --   'metric_time__extract_dow',
  --   'metric_time__extract_doy',
  --   'metric_time__extract_month',
  --   'metric_time__extract_quarter',
  --   'metric_time__extract_year',
  -- ]
  -- Pass Only Elements: [
  --   '__bookings',
  --   'metric_time__extract_day',
  --   'metric_time__extract_dow',
  --   'metric_time__extract_doy',
  --   'metric_time__extract_month',
  --   'metric_time__extract_quarter',
  --   'metric_time__extract_year',
  -- ]
  SELECT
    EXTRACT(year FROM ds) AS metric_time__extract_year
    , EXTRACT(quarter FROM ds) AS metric_time__extract_quarter
    , EXTRACT(month FROM ds) AS metric_time__extract_month
    , EXTRACT(day FROM ds) AS metric_time__extract_day
    , EXTRACT(DAY_OF_WEEK FROM ds) AS metric_time__extract_dow
    , EXTRACT(doy FROM ds) AS metric_time__extract_doy
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_9
GROUP BY
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
