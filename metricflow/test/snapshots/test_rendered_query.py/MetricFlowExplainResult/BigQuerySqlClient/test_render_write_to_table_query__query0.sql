CREATE TABLE ***************************.test_table AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    ds
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'ds']
    SELECT
      ds
      , 1 AS bookings
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_0
  ) subq_2
  GROUP BY
    ds
)
