-- Constrain Output with WHERE
SELECT
  ds__day
  , bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'ds__day']
  SELECT
    DATE_TRUNC('day', ds) AS ds__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_3
WHERE booking__ds__day = '2020-01-01'
