-- Order By ['ds__day', 'bookings']
SELECT
  subq_2.ds__day
  , subq_2.is_instant
  , subq_2.bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_1.ds__day
    , subq_1.is_instant
    , subq_1.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_0.ds__day
      , subq_0.is_instant
      , SUM(subq_0.bookings) AS bookings
    FROM (
      -- Read From SemanticModelDataSet('bookings_source')
      -- Pass Only Elements: ['bookings', 'is_instant', 'ds__day']
      SELECT
        1 AS bookings
        , bookings_source_src_28000.is_instant
        , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_0
    GROUP BY
      subq_0.ds__day
      , subq_0.is_instant
  ) subq_1
) subq_2
ORDER BY subq_2.ds__day, subq_2.bookings DESC
