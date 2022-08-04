-- Read Elements From Data Source 'bookings_source'
-- Pass Only Elements:
--   ['bookings', 'ds']
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  1 AS bookings
  , ds
  , ds AS metric_time
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_bookings
) bookings_source_src_10000
WHERE (
  ds >= CAST('2020-01-01' AS TEXT)
) AND (
  ds <= CAST('2020-01-02' AS TEXT)
)
