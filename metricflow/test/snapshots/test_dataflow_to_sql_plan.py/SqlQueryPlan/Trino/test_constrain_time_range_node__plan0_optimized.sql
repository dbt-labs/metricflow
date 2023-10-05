-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements:
--   ['bookings', 'ds__day']
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  ds AS ds__day
  , ds AS metric_time__day
  , 1 AS bookings
FROM ***************************.fct_bookings bookings_source_src_10001
WHERE ds BETWEEN '2020-01-01' AND '2020-01-02'
