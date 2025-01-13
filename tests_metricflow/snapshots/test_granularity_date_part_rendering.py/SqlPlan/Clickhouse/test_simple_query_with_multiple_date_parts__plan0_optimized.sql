test_name: test_simple_query_with_multiple_date_parts
test_filename: test_granularity_date_part_rendering.py
sql_engine: Clickhouse
---
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: [
  --   'bookings',
  --   'metric_time__extract_day',
  --   'metric_time__extract_dow',
  --   'metric_time__extract_doy',
  --   'metric_time__extract_month',
  --   'metric_time__extract_quarter',
  --   'metric_time__extract_year',
  -- ]
  SELECT
    EXTRACT(toYear FROM ds) AS metric_time__extract_year
    , EXTRACT(toQuarter FROM ds) AS metric_time__extract_quarter
    , EXTRACT(toMonth FROM ds) AS metric_time__extract_month
    , EXTRACT(toDayOfMonth FROM ds) AS metric_time__extract_day
    , EXTRACT(toDayOfWeek FROM ds) AS metric_time__extract_dow
    , EXTRACT(toDayOfYear FROM ds) AS metric_time__extract_doy
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_6
GROUP BY
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
