-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_1.is_instant, subq_3.is_instant) AS is_instant
  , MAX(subq_1.bookings) AS bookings
  , COALESCE(MAX(subq_3.instant_bookings), 1) AS instant_bookings
  , COALESCE(MAX(subq_3.bookers), 1) AS bookers
FROM (
  -- Aggregate Measures
  SELECT
    subq_0.is_instant
    , SUM(subq_0.bookings) AS bookings
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['bookings', 'is_instant']
    SELECT
      1 AS bookings
      , bookings_source_src_28000.is_instant
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_0
  GROUP BY
    subq_0.is_instant
) subq_1
FULL OUTER JOIN (
  -- Aggregate Measures
  SELECT
    subq_2.is_instant
    , SUM(subq_2.instant_bookings) AS instant_bookings
    , COUNT(DISTINCT subq_2.bookers) AS bookers
  FROM (
    -- Read From SemanticModelDataSet('bookings_source')
    -- Pass Only Elements: ['instant_bookings', 'bookers', 'is_instant']
    SELECT
      CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      , bookings_source_src_28000.guest_id AS bookers
      , bookings_source_src_28000.is_instant
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_2
  GROUP BY
    subq_2.is_instant
) subq_3
ON
  subq_1.is_instant = subq_3.is_instant
GROUP BY
  COALESCE(subq_1.is_instant, subq_3.is_instant)
