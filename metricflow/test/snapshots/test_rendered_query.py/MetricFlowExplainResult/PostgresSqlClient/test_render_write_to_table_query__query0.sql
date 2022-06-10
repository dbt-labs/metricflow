CREATE TABLE ***************************.test_table AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(bookings) AS bookings
    , ds
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'ds']
    SELECT
      1 AS bookings
      , ds
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_0
    WHERE (
      ds >= CAST('2000-01-01' AS TIMESTAMP)
    ) AND (
      ds <= CAST('2040-12-31' AS TIMESTAMP)
    )
  ) subq_2
  GROUP BY
    ds
)
