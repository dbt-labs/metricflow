test_name: test_simple_fill_nulls_with_0_with_categorical_dimension
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  booking__is_instant
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    booking__is_instant
    , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
  FROM (
    SELECT
      is_instant AS booking__is_instant
      , 1 AS __bookings_fill_nulls_with_0
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  GROUP BY
    booking__is_instant
) subq_10
