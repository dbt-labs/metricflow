-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_week
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings',
  --    'metric_time__extract_day',
  --    'metric_time__extract_dow',
  --    'metric_time__extract_doy',
  --    'metric_time__extract_week',
  --    'metric_time__extract_month',
  --    'metric_time__extract_quarter',
  --    'metric_time__extract_year']
  SELECT
    EXTRACT(year FROM ds) AS metric_time__extract_year
    , EXTRACT(quarter FROM ds) AS metric_time__extract_quarter
    , EXTRACT(month FROM ds) AS metric_time__extract_month
    , EXTRACT(isoweek FROM ds) AS metric_time__extract_week
    , EXTRACT(day FROM ds) AS metric_time__extract_day
    , EXTRACT(dayofweek FROM ds) AS metric_time__extract_dow
    , EXTRACT(dayofyear FROM ds) AS metric_time__extract_doy
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_6
GROUP BY
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_week
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
