-- Constrain Output with WHERE
SELECT
  ds
  , bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements:
  --   ['bookings', 'ds']
  SELECT
    ds
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_3
WHERE ds = '2020-01-01'
