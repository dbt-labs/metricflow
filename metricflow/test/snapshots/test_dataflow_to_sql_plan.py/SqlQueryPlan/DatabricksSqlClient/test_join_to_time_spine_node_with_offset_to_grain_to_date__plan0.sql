-- Join to Time Spine Dataset
SELECT
  subq_3.ds AS ds
  , subq_3.metric_time AS metric_time
  , subq_3.listing AS listing
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Date Spine
  SELECT
    subq_5.ds AS metric_time
  FROM ***************************.mf_time_spine subq_5
  WHERE subq_5.ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2021-01-01' AS TIMESTAMP)
) subq_5
LEFT OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_3.ds
    , subq_3.metric_time
    , subq_3.listing
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Aggregate Measures
    SELECT
      subq_2.ds
      , subq_2.metric_time
      , subq_2.listing
      , SUM(subq_2.booking_value) AS booking_value
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        subq_1.ds
        , subq_1.ds AS metric_time
        , subq_1.listing
        , subq_1.booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'ds', 'listing']
        SELECT
          subq_0.ds
          , subq_0.listing
          , subq_0.booking_value
        FROM (
          -- Read Elements From Data Source 'bookings_source'
          SELECT
            1 AS bookings
            , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
            , bookings_source_src_10001.booking_value
            , bookings_source_src_10001.booking_value AS max_booking_value
            , bookings_source_src_10001.booking_value AS min_booking_value
            , bookings_source_src_10001.guest_id AS bookers
            , bookings_source_src_10001.booking_value AS average_booking_value
            , bookings_source_src_10001.booking_value AS booking_payments
            , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
            , bookings_source_src_10001.is_instant
            , bookings_source_src_10001.ds
            , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
            , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
            , bookings_source_src_10001.ds_partitioned
            , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
            , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
            , bookings_source_src_10001.booking_paid_at
            , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__week
            , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__year
            , bookings_source_src_10001.is_instant AS create_a_cycle_in_the_join_graph__is_instant
            , bookings_source_src_10001.ds AS create_a_cycle_in_the_join_graph__ds
            , DATE_TRUNC('week', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__week
            , DATE_TRUNC('month', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__year
            , bookings_source_src_10001.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
            , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
            , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
            , bookings_source_src_10001.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
            , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
            , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
            , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
            , bookings_source_src_10001.listing_id AS listing
            , bookings_source_src_10001.guest_id AS guest
            , bookings_source_src_10001.host_id AS host
            , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph
            , bookings_source_src_10001.listing_id AS create_a_cycle_in_the_join_graph__listing
            , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph__guest
            , bookings_source_src_10001.host_id AS create_a_cycle_in_the_join_graph__host
          FROM (
            -- User Defined SQL Query
            SELECT * FROM ***************************.fct_bookings
          ) bookings_source_src_10001
        ) subq_0
      ) subq_1
    ) subq_2
    GROUP BY
      subq_2.ds
      , subq_2.metric_time
      , subq_2.listing
  ) subq_3
) subq_4
ON
  DATE_TRUNC('month', subq_5.metric_time) = subq_4.metric_time
