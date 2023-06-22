-- Compute Metrics via Expressions
SELECT
  subq_22.metric_time
  , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_11.metric_time, subq_16.metric_time, subq_21.metric_time) AS metric_time
    , subq_11.non_referred AS non_referred
    , subq_16.instant AS instant
    , subq_21.bookings AS bookings
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time
      , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
    FROM (
      -- Combine Metrics
      SELECT
        COALESCE(subq_4.metric_time, subq_9.metric_time) AS metric_time
        , subq_4.ref_bookings AS ref_bookings
        , subq_9.bookings AS bookings
      FROM (
        -- Compute Metrics via Expressions
        SELECT
          subq_3.metric_time
          , subq_3.referred_bookings AS ref_bookings
        FROM (
          -- Aggregate Measures
          SELECT
            subq_2.metric_time
            , SUM(subq_2.referred_bookings) AS referred_bookings
          FROM (
            -- Pass Only Elements:
            --   ['referred_bookings', 'metric_time']
            SELECT
              subq_1.metric_time
              , subq_1.referred_bookings
            FROM (
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
                -- Read Elements From Semantic Model 'bookings_source'
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
                FROM ***************************.fct_bookings bookings_source_src_10001
              ) subq_0
            ) subq_1
          ) subq_2
          GROUP BY
            subq_2.metric_time
        ) subq_3
      ) subq_4
      INNER JOIN (
        -- Compute Metrics via Expressions
        SELECT
          subq_8.metric_time
          , subq_8.bookings
        FROM (
          -- Aggregate Measures
          SELECT
            subq_7.metric_time
            , SUM(subq_7.bookings) AS bookings
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'metric_time']
            SELECT
              subq_6.metric_time
              , subq_6.bookings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_5.ds
                , subq_5.ds__week
                , subq_5.ds__month
                , subq_5.ds__quarter
                , subq_5.ds__year
                , subq_5.ds_partitioned
                , subq_5.ds_partitioned__week
                , subq_5.ds_partitioned__month
                , subq_5.ds_partitioned__quarter
                , subq_5.ds_partitioned__year
                , subq_5.booking_paid_at
                , subq_5.booking_paid_at__week
                , subq_5.booking_paid_at__month
                , subq_5.booking_paid_at__quarter
                , subq_5.booking_paid_at__year
                , subq_5.create_a_cycle_in_the_join_graph__ds
                , subq_5.create_a_cycle_in_the_join_graph__ds__week
                , subq_5.create_a_cycle_in_the_join_graph__ds__month
                , subq_5.create_a_cycle_in_the_join_graph__ds__quarter
                , subq_5.create_a_cycle_in_the_join_graph__ds__year
                , subq_5.create_a_cycle_in_the_join_graph__ds_partitioned
                , subq_5.create_a_cycle_in_the_join_graph__ds_partitioned__week
                , subq_5.create_a_cycle_in_the_join_graph__ds_partitioned__month
                , subq_5.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                , subq_5.create_a_cycle_in_the_join_graph__ds_partitioned__year
                , subq_5.create_a_cycle_in_the_join_graph__booking_paid_at
                , subq_5.create_a_cycle_in_the_join_graph__booking_paid_at__week
                , subq_5.create_a_cycle_in_the_join_graph__booking_paid_at__month
                , subq_5.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                , subq_5.create_a_cycle_in_the_join_graph__booking_paid_at__year
                , subq_5.ds AS metric_time
                , subq_5.ds__week AS metric_time__week
                , subq_5.ds__month AS metric_time__month
                , subq_5.ds__quarter AS metric_time__quarter
                , subq_5.ds__year AS metric_time__year
                , subq_5.listing
                , subq_5.guest
                , subq_5.host
                , subq_5.create_a_cycle_in_the_join_graph
                , subq_5.create_a_cycle_in_the_join_graph__listing
                , subq_5.create_a_cycle_in_the_join_graph__guest
                , subq_5.create_a_cycle_in_the_join_graph__host
                , subq_5.is_instant
                , subq_5.create_a_cycle_in_the_join_graph__is_instant
                , subq_5.bookings
                , subq_5.instant_bookings
                , subq_5.booking_value
                , subq_5.max_booking_value
                , subq_5.min_booking_value
                , subq_5.bookers
                , subq_5.average_booking_value
                , subq_5.referred_bookings
                , subq_5.median_booking_value
                , subq_5.booking_value_p99
                , subq_5.discrete_booking_value_p99
                , subq_5.approximate_continuous_booking_value_p99
                , subq_5.approximate_discrete_booking_value_p99
              FROM (
                -- Read Elements From Semantic Model 'bookings_source'
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
                FROM ***************************.fct_bookings bookings_source_src_10001
              ) subq_5
            ) subq_6
          ) subq_7
          GROUP BY
            subq_7.metric_time
        ) subq_8
      ) subq_9
      ON
        (
          subq_4.metric_time = subq_9.metric_time
        ) OR (
          (subq_4.metric_time IS NULL) AND (subq_9.metric_time IS NULL)
        )
    ) subq_10
  ) subq_11
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_15.metric_time
      , subq_15.instant_bookings AS instant
    FROM (
      -- Aggregate Measures
      SELECT
        subq_14.metric_time
        , SUM(subq_14.instant_bookings) AS instant_bookings
      FROM (
        -- Pass Only Elements:
        --   ['instant_bookings', 'metric_time']
        SELECT
          subq_13.metric_time
          , subq_13.instant_bookings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_12.ds
            , subq_12.ds__week
            , subq_12.ds__month
            , subq_12.ds__quarter
            , subq_12.ds__year
            , subq_12.ds_partitioned
            , subq_12.ds_partitioned__week
            , subq_12.ds_partitioned__month
            , subq_12.ds_partitioned__quarter
            , subq_12.ds_partitioned__year
            , subq_12.booking_paid_at
            , subq_12.booking_paid_at__week
            , subq_12.booking_paid_at__month
            , subq_12.booking_paid_at__quarter
            , subq_12.booking_paid_at__year
            , subq_12.create_a_cycle_in_the_join_graph__ds
            , subq_12.create_a_cycle_in_the_join_graph__ds__week
            , subq_12.create_a_cycle_in_the_join_graph__ds__month
            , subq_12.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_12.create_a_cycle_in_the_join_graph__ds__year
            , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_12.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_12.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_12.ds AS metric_time
            , subq_12.ds__week AS metric_time__week
            , subq_12.ds__month AS metric_time__month
            , subq_12.ds__quarter AS metric_time__quarter
            , subq_12.ds__year AS metric_time__year
            , subq_12.listing
            , subq_12.guest
            , subq_12.host
            , subq_12.create_a_cycle_in_the_join_graph
            , subq_12.create_a_cycle_in_the_join_graph__listing
            , subq_12.create_a_cycle_in_the_join_graph__guest
            , subq_12.create_a_cycle_in_the_join_graph__host
            , subq_12.is_instant
            , subq_12.create_a_cycle_in_the_join_graph__is_instant
            , subq_12.bookings
            , subq_12.instant_bookings
            , subq_12.booking_value
            , subq_12.max_booking_value
            , subq_12.min_booking_value
            , subq_12.bookers
            , subq_12.average_booking_value
            , subq_12.referred_bookings
            , subq_12.median_booking_value
            , subq_12.booking_value_p99
            , subq_12.discrete_booking_value_p99
            , subq_12.approximate_continuous_booking_value_p99
            , subq_12.approximate_discrete_booking_value_p99
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
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
            FROM ***************************.fct_bookings bookings_source_src_10001
          ) subq_12
        ) subq_13
      ) subq_14
      GROUP BY
        subq_14.metric_time
    ) subq_15
  ) subq_16
  ON
    (
      subq_11.metric_time = subq_16.metric_time
    ) OR (
      (subq_11.metric_time IS NULL) AND (subq_16.metric_time IS NULL)
    )
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_20.metric_time
      , subq_20.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_19.metric_time
        , SUM(subq_19.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          subq_18.metric_time
          , subq_18.bookings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_17.ds
            , subq_17.ds__week
            , subq_17.ds__month
            , subq_17.ds__quarter
            , subq_17.ds__year
            , subq_17.ds_partitioned
            , subq_17.ds_partitioned__week
            , subq_17.ds_partitioned__month
            , subq_17.ds_partitioned__quarter
            , subq_17.ds_partitioned__year
            , subq_17.booking_paid_at
            , subq_17.booking_paid_at__week
            , subq_17.booking_paid_at__month
            , subq_17.booking_paid_at__quarter
            , subq_17.booking_paid_at__year
            , subq_17.create_a_cycle_in_the_join_graph__ds
            , subq_17.create_a_cycle_in_the_join_graph__ds__week
            , subq_17.create_a_cycle_in_the_join_graph__ds__month
            , subq_17.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_17.create_a_cycle_in_the_join_graph__ds__year
            , subq_17.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_17.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_17.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_17.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_17.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_17.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_17.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_17.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_17.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_17.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_17.ds AS metric_time
            , subq_17.ds__week AS metric_time__week
            , subq_17.ds__month AS metric_time__month
            , subq_17.ds__quarter AS metric_time__quarter
            , subq_17.ds__year AS metric_time__year
            , subq_17.listing
            , subq_17.guest
            , subq_17.host
            , subq_17.create_a_cycle_in_the_join_graph
            , subq_17.create_a_cycle_in_the_join_graph__listing
            , subq_17.create_a_cycle_in_the_join_graph__guest
            , subq_17.create_a_cycle_in_the_join_graph__host
            , subq_17.is_instant
            , subq_17.create_a_cycle_in_the_join_graph__is_instant
            , subq_17.bookings
            , subq_17.instant_bookings
            , subq_17.booking_value
            , subq_17.max_booking_value
            , subq_17.min_booking_value
            , subq_17.bookers
            , subq_17.average_booking_value
            , subq_17.referred_bookings
            , subq_17.median_booking_value
            , subq_17.booking_value_p99
            , subq_17.discrete_booking_value_p99
            , subq_17.approximate_continuous_booking_value_p99
            , subq_17.approximate_discrete_booking_value_p99
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
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
            FROM ***************************.fct_bookings bookings_source_src_10001
          ) subq_17
        ) subq_18
      ) subq_19
      GROUP BY
        subq_19.metric_time
    ) subq_20
  ) subq_21
  ON
    (
      subq_11.metric_time = subq_21.metric_time
    ) OR (
      (subq_11.metric_time IS NULL) AND (subq_21.metric_time IS NULL)
    )
) subq_22
