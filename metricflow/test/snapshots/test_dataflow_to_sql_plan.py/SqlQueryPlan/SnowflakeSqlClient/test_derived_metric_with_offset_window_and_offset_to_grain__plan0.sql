-- Compute Metrics via Expressions
SELECT
  subq_16.metric_time
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_7.metric_time, subq_15.metric_time) AS metric_time
    , subq_7.month_start_bookings AS month_start_bookings
    , subq_15.bookings_1_month_ago AS bookings_1_month_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_6.metric_time
      , subq_6.bookings AS month_start_bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_5.metric_time
        , SUM(subq_5.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          subq_4.metric_time
          , subq_4.bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            DATE_TRUNC('day', subq_2.metric_time) AS metric_time
            , subq_1.ds AS ds
            , subq_1.ds__week AS ds__week
            , subq_1.ds__month AS ds__month
            , subq_1.ds__quarter AS ds__quarter
            , subq_1.ds__year AS ds__year
            , subq_1.ds_partitioned AS ds_partitioned
            , subq_1.ds_partitioned__week AS ds_partitioned__week
            , subq_1.ds_partitioned__month AS ds_partitioned__month
            , subq_1.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_1.ds_partitioned__year AS ds_partitioned__year
            , subq_1.booking_paid_at AS booking_paid_at
            , subq_1.booking_paid_at__week AS booking_paid_at__week
            , subq_1.booking_paid_at__month AS booking_paid_at__month
            , subq_1.booking_paid_at__quarter AS booking_paid_at__quarter
            , subq_1.booking_paid_at__year AS booking_paid_at__year
            , subq_1.create_a_cycle_in_the_join_graph__ds AS create_a_cycle_in_the_join_graph__ds
            , subq_1.create_a_cycle_in_the_join_graph__ds__week AS create_a_cycle_in_the_join_graph__ds__week
            , subq_1.create_a_cycle_in_the_join_graph__ds__month AS create_a_cycle_in_the_join_graph__ds__month
            , subq_1.create_a_cycle_in_the_join_graph__ds__quarter AS create_a_cycle_in_the_join_graph__ds__quarter
            , subq_1.create_a_cycle_in_the_join_graph__ds__year AS create_a_cycle_in_the_join_graph__ds__year
            , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__week AS create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__month AS create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__quarter AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_1.create_a_cycle_in_the_join_graph__ds_partitioned__year AS create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__week AS create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__month AS create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__quarter AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_1.create_a_cycle_in_the_join_graph__booking_paid_at__year AS create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_1.listing AS listing
            , subq_1.guest AS guest
            , subq_1.host AS host
            , subq_1.create_a_cycle_in_the_join_graph AS create_a_cycle_in_the_join_graph
            , subq_1.create_a_cycle_in_the_join_graph__listing AS create_a_cycle_in_the_join_graph__listing
            , subq_1.create_a_cycle_in_the_join_graph__guest AS create_a_cycle_in_the_join_graph__guest
            , subq_1.create_a_cycle_in_the_join_graph__host AS create_a_cycle_in_the_join_graph__host
            , subq_1.is_instant AS is_instant
            , subq_1.create_a_cycle_in_the_join_graph__is_instant AS create_a_cycle_in_the_join_graph__is_instant
            , subq_1.bookings AS bookings
            , subq_1.instant_bookings AS instant_bookings
            , subq_1.booking_value AS booking_value
            , subq_1.max_booking_value AS max_booking_value
            , subq_1.min_booking_value AS min_booking_value
            , subq_1.bookers AS bookers
            , subq_1.average_booking_value AS average_booking_value
            , subq_1.referred_bookings AS referred_bookings
            , subq_1.median_booking_value AS median_booking_value
            , subq_1.booking_value_p99 AS booking_value_p99
            , subq_1.discrete_booking_value_p99 AS discrete_booking_value_p99
            , subq_1.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , subq_1.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Date Spine
            SELECT
              subq_3.ds AS metric_time
            FROM ***************************.mf_time_spine subq_3
            GROUP BY
              subq_3.ds
          ) subq_2
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_0.ds
              , subq_0.ds__week
              , subq_0.ds__month
              , subq_0.ds__quarter
              , subq_0.ds__year
              , subq_0.ds_partitioned
              , subq_0.ds_partitioned__week
              , subq_0.ds_partitioned__month
              , subq_0.ds_partitioned__quarter
              , subq_0.ds_partitioned__year
              , subq_0.booking_paid_at
              , subq_0.booking_paid_at__week
              , subq_0.booking_paid_at__month
              , subq_0.booking_paid_at__quarter
              , subq_0.booking_paid_at__year
              , subq_0.create_a_cycle_in_the_join_graph__ds
              , subq_0.create_a_cycle_in_the_join_graph__ds__week
              , subq_0.create_a_cycle_in_the_join_graph__ds__month
              , subq_0.create_a_cycle_in_the_join_graph__ds__quarter
              , subq_0.create_a_cycle_in_the_join_graph__ds__year
              , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned
              , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__week
              , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__month
              , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
              , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__year
              , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at
              , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__week
              , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__month
              , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
              , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__year
              , subq_0.ds AS metric_time
              , subq_0.ds__week AS metric_time__week
              , subq_0.ds__month AS metric_time__month
              , subq_0.ds__quarter AS metric_time__quarter
              , subq_0.ds__year AS metric_time__year
              , subq_0.listing
              , subq_0.guest
              , subq_0.host
              , subq_0.create_a_cycle_in_the_join_graph
              , subq_0.create_a_cycle_in_the_join_graph__listing
              , subq_0.create_a_cycle_in_the_join_graph__guest
              , subq_0.create_a_cycle_in_the_join_graph__host
              , subq_0.is_instant
              , subq_0.create_a_cycle_in_the_join_graph__is_instant
              , subq_0.bookings
              , subq_0.instant_bookings
              , subq_0.booking_value
              , subq_0.max_booking_value
              , subq_0.min_booking_value
              , subq_0.bookers
              , subq_0.average_booking_value
              , subq_0.referred_bookings
              , subq_0.median_booking_value
              , subq_0.booking_value_p99
              , subq_0.discrete_booking_value_p99
              , subq_0.approximate_continuous_booking_value_p99
              , subq_0.approximate_discrete_booking_value_p99
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
                , bookings_source_src_10001.booking_value AS median_booking_value
                , bookings_source_src_10001.booking_value AS booking_value_p99
                , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
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
          ON
            DATE_TRUNC('month', subq_2.metric_time) = subq_1.metric_time
        ) subq_4
      ) subq_5
      GROUP BY
        subq_5.metric_time
    ) subq_6
  ) subq_7
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_14.metric_time
      , subq_14.bookings AS bookings_1_month_ago
    FROM (
      -- Aggregate Measures
      SELECT
        subq_13.metric_time
        , SUM(subq_13.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          subq_12.metric_time
          , subq_12.bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            DATE_TRUNC('day', subq_10.metric_time) AS metric_time
            , subq_9.ds AS ds
            , subq_9.ds__week AS ds__week
            , subq_9.ds__month AS ds__month
            , subq_9.ds__quarter AS ds__quarter
            , subq_9.ds__year AS ds__year
            , subq_9.ds_partitioned AS ds_partitioned
            , subq_9.ds_partitioned__week AS ds_partitioned__week
            , subq_9.ds_partitioned__month AS ds_partitioned__month
            , subq_9.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_9.ds_partitioned__year AS ds_partitioned__year
            , subq_9.booking_paid_at AS booking_paid_at
            , subq_9.booking_paid_at__week AS booking_paid_at__week
            , subq_9.booking_paid_at__month AS booking_paid_at__month
            , subq_9.booking_paid_at__quarter AS booking_paid_at__quarter
            , subq_9.booking_paid_at__year AS booking_paid_at__year
            , subq_9.create_a_cycle_in_the_join_graph__ds AS create_a_cycle_in_the_join_graph__ds
            , subq_9.create_a_cycle_in_the_join_graph__ds__week AS create_a_cycle_in_the_join_graph__ds__week
            , subq_9.create_a_cycle_in_the_join_graph__ds__month AS create_a_cycle_in_the_join_graph__ds__month
            , subq_9.create_a_cycle_in_the_join_graph__ds__quarter AS create_a_cycle_in_the_join_graph__ds__quarter
            , subq_9.create_a_cycle_in_the_join_graph__ds__year AS create_a_cycle_in_the_join_graph__ds__year
            , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__week AS create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__month AS create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__quarter AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_9.create_a_cycle_in_the_join_graph__ds_partitioned__year AS create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_9.create_a_cycle_in_the_join_graph__booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_9.create_a_cycle_in_the_join_graph__booking_paid_at__week AS create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_9.create_a_cycle_in_the_join_graph__booking_paid_at__month AS create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_9.create_a_cycle_in_the_join_graph__booking_paid_at__quarter AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_9.create_a_cycle_in_the_join_graph__booking_paid_at__year AS create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_9.listing AS listing
            , subq_9.guest AS guest
            , subq_9.host AS host
            , subq_9.create_a_cycle_in_the_join_graph AS create_a_cycle_in_the_join_graph
            , subq_9.create_a_cycle_in_the_join_graph__listing AS create_a_cycle_in_the_join_graph__listing
            , subq_9.create_a_cycle_in_the_join_graph__guest AS create_a_cycle_in_the_join_graph__guest
            , subq_9.create_a_cycle_in_the_join_graph__host AS create_a_cycle_in_the_join_graph__host
            , subq_9.is_instant AS is_instant
            , subq_9.create_a_cycle_in_the_join_graph__is_instant AS create_a_cycle_in_the_join_graph__is_instant
            , subq_9.bookings AS bookings
            , subq_9.instant_bookings AS instant_bookings
            , subq_9.booking_value AS booking_value
            , subq_9.max_booking_value AS max_booking_value
            , subq_9.min_booking_value AS min_booking_value
            , subq_9.bookers AS bookers
            , subq_9.average_booking_value AS average_booking_value
            , subq_9.referred_bookings AS referred_bookings
            , subq_9.median_booking_value AS median_booking_value
            , subq_9.booking_value_p99 AS booking_value_p99
            , subq_9.discrete_booking_value_p99 AS discrete_booking_value_p99
            , subq_9.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , subq_9.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Date Spine
            SELECT
              subq_11.ds AS metric_time
            FROM ***************************.mf_time_spine subq_11
            GROUP BY
              subq_11.ds
          ) subq_10
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_8.ds
              , subq_8.ds__week
              , subq_8.ds__month
              , subq_8.ds__quarter
              , subq_8.ds__year
              , subq_8.ds_partitioned
              , subq_8.ds_partitioned__week
              , subq_8.ds_partitioned__month
              , subq_8.ds_partitioned__quarter
              , subq_8.ds_partitioned__year
              , subq_8.booking_paid_at
              , subq_8.booking_paid_at__week
              , subq_8.booking_paid_at__month
              , subq_8.booking_paid_at__quarter
              , subq_8.booking_paid_at__year
              , subq_8.create_a_cycle_in_the_join_graph__ds
              , subq_8.create_a_cycle_in_the_join_graph__ds__week
              , subq_8.create_a_cycle_in_the_join_graph__ds__month
              , subq_8.create_a_cycle_in_the_join_graph__ds__quarter
              , subq_8.create_a_cycle_in_the_join_graph__ds__year
              , subq_8.create_a_cycle_in_the_join_graph__ds_partitioned
              , subq_8.create_a_cycle_in_the_join_graph__ds_partitioned__week
              , subq_8.create_a_cycle_in_the_join_graph__ds_partitioned__month
              , subq_8.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
              , subq_8.create_a_cycle_in_the_join_graph__ds_partitioned__year
              , subq_8.create_a_cycle_in_the_join_graph__booking_paid_at
              , subq_8.create_a_cycle_in_the_join_graph__booking_paid_at__week
              , subq_8.create_a_cycle_in_the_join_graph__booking_paid_at__month
              , subq_8.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
              , subq_8.create_a_cycle_in_the_join_graph__booking_paid_at__year
              , subq_8.ds AS metric_time
              , subq_8.ds__week AS metric_time__week
              , subq_8.ds__month AS metric_time__month
              , subq_8.ds__quarter AS metric_time__quarter
              , subq_8.ds__year AS metric_time__year
              , subq_8.listing
              , subq_8.guest
              , subq_8.host
              , subq_8.create_a_cycle_in_the_join_graph
              , subq_8.create_a_cycle_in_the_join_graph__listing
              , subq_8.create_a_cycle_in_the_join_graph__guest
              , subq_8.create_a_cycle_in_the_join_graph__host
              , subq_8.is_instant
              , subq_8.create_a_cycle_in_the_join_graph__is_instant
              , subq_8.bookings
              , subq_8.instant_bookings
              , subq_8.booking_value
              , subq_8.max_booking_value
              , subq_8.min_booking_value
              , subq_8.bookers
              , subq_8.average_booking_value
              , subq_8.referred_bookings
              , subq_8.median_booking_value
              , subq_8.booking_value_p99
              , subq_8.discrete_booking_value_p99
              , subq_8.approximate_continuous_booking_value_p99
              , subq_8.approximate_discrete_booking_value_p99
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
                , bookings_source_src_10001.booking_value AS median_booking_value
                , bookings_source_src_10001.booking_value AS booking_value_p99
                , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
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
            ) subq_8
          ) subq_9
          ON
            DATEADD(month, -1, subq_10.metric_time) = subq_9.metric_time
        ) subq_12
      ) subq_13
      GROUP BY
        subq_13.metric_time
    ) subq_14
  ) subq_15
  ON
    (
      subq_7.metric_time = subq_15.metric_time
    ) OR (
      (subq_7.metric_time IS NULL) AND (subq_15.metric_time IS NULL)
    )
) subq_16
