-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  subq_1.ds__day
  , subq_1.metric_time__day
  , subq_1.bookings
FROM (
  -- Metric Time Dimension 'ds'
  SELECT
    subq_0.ds__day
    , subq_0.ds__day AS metric_time__day
    , subq_0.bookings
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['bookings', 'ds__day']
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_0
) subq_1
WHERE subq_1.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
