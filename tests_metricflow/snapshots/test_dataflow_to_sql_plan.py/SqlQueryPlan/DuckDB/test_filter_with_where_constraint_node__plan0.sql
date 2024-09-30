-- Constrain Output with WHERE
SELECT
  subq_0.ds__day
  , subq_0.bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'ds__day']
  SELECT
    1 AS bookings
    , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_0
WHERE booking__ds__day = '2020-01-01'
