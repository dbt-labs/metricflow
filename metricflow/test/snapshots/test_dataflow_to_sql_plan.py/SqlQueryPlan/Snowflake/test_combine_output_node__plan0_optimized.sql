-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_8.is_instant, subq_11.is_instant) AS is_instant
  , MAX(subq_8.bookings) AS bookings
  , COALESCE(MAX(subq_11.instant_bookings), 1) AS instant_bookings
  , COALESCE(MAX(subq_11.bookers), 1) AS bookers
FROM (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Pass Only Elements:
    --   ['bookings', 'is_instant']
    SELECT
      is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_7
  GROUP BY
    is_instant
) subq_8
FULL OUTER JOIN (
  -- Aggregate Measures
  SELECT
    is_instant
    , SUM(instant_bookings) AS instant_bookings
    , COUNT(DISTINCT bookers) AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Pass Only Elements:
    --   ['instant_bookings', 'bookers', 'is_instant']
    SELECT
      is_instant
      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_10
  GROUP BY
    is_instant
) subq_11
ON
  subq_8.is_instant = subq_11.is_instant
GROUP BY
  COALESCE(subq_8.is_instant, subq_11.is_instant)
