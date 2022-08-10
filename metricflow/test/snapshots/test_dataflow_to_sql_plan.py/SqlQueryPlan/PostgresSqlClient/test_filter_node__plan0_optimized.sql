-- Read Elements From Data Source 'bookings_source'
-- Pass Only Elements:
--   ['bookings']
SELECT
  1 AS bookings
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_bookings
) bookings_source_src_10001
