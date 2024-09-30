-- Constrain Output with WHERE
SELECT
  ds__day
  , bookings
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'ds__day']
  SELECT
    1 AS bookings
    , DATE_TRUNC('day', ds) AS ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_1
WHERE booking__ds__day = '2020-01-01'
