test_name: test_constrain_time_range_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting the ConstrainTimeRangeNode to SQL.
sql_engine: BigQuery
---
-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements: ['bookings', 'ds__day']
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  DATETIME_TRUNC(ds, day) AS ds__day
  , DATETIME_TRUNC(ds, day) AS metric_time__day
  , 1 AS bookings
FROM ***************************.fct_bookings bookings_source_src_28000
WHERE DATETIME_TRUNC(ds, day) BETWEEN '2020-01-01' AND '2020-01-02'
