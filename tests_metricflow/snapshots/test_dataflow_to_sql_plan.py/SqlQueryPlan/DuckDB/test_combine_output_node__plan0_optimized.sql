-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_5.is_instant, subq_7.is_instant) AS is_instant
  , MAX(subq_5.bookings) AS bookings
  , COALESCE(MAX(subq_7.instant_bookings), 1) AS instant_bookings
  , COALESCE(MAX(subq_7.bookers), 1) AS bookers
FROM (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(bookings) AS bookings
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['bookings', 'is_instant']
    SELECT
      1 AS bookings
      , is_instant
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_4
  GROUP BY
    is_instant
) subq_5
FULL OUTER JOIN (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(instant_bookings) AS instant_bookings
    , COUNT(DISTINCT bookers) AS bookers
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['instant_bookings', 'bookers', 'is_instant']
    SELECT
      CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      , guest_id AS bookers
      , is_instant
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_6
  GROUP BY
    is_instant
) subq_7
ON
  subq_5.is_instant = subq_7.is_instant
GROUP BY
  COALESCE(subq_5.is_instant, subq_7.is_instant)
