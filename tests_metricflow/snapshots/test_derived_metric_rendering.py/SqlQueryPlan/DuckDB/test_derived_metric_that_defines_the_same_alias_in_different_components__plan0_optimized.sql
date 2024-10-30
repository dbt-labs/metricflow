-- Combine Aggregated Outputs
-- Order By [] Limit 1
SELECT
  COALESCE(subq_18.booking__is_instant, subq_24.booking__is_instant) AS booking__is_instant
  , MAX(subq_18.derived_sharedalias_1a) AS derived_sharedalias_1a
  , MAX(subq_24.derived_sharedalias_2) AS derived_sharedalias_2
FROM (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias - 10 AS derived_sharedalias_1a
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(bookings) AS shared_alias
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'booking__is_instant']
      SELECT
        is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    GROUP BY
      booking__is_instant
  ) subq_17
) subq_18
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    booking__is_instant
    , shared_alias + 10 AS derived_sharedalias_2
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__is_instant
      , SUM(instant_bookings) AS shared_alias
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['instant_bookings', 'booking__is_instant']
      SELECT
        is_instant AS booking__is_instant
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_21
    GROUP BY
      booking__is_instant
  ) subq_23
) subq_24
ON
  subq_18.booking__is_instant = subq_24.booking__is_instant
GROUP BY
  COALESCE(subq_18.booking__is_instant, subq_24.booking__is_instant)
LIMIT 1
