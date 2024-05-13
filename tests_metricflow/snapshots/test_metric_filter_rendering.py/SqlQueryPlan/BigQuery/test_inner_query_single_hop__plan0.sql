-- Compute Metrics via Expressions
SELECT
  subq_21.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_20.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_19.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_18.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        , subq_18.third_hop_count
      FROM (
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
        SELECT
          subq_17.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
          , subq_17.third_hop_count
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_8.customer_third_hop_id AS customer_third_hop_id
            , subq_16.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
            , subq_16.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
            , subq_8.third_hop_count AS third_hop_count
          FROM (
            -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
            SELECT
              subq_7.customer_third_hop_id
              , subq_7.third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_6.third_hop_ds__day
                , subq_6.third_hop_ds__week
                , subq_6.third_hop_ds__month
                , subq_6.third_hop_ds__quarter
                , subq_6.third_hop_ds__year
                , subq_6.third_hop_ds__extract_year
                , subq_6.third_hop_ds__extract_quarter
                , subq_6.third_hop_ds__extract_month
                , subq_6.third_hop_ds__extract_day
                , subq_6.third_hop_ds__extract_dow
                , subq_6.third_hop_ds__extract_doy
                , subq_6.customer_third_hop_id__third_hop_ds__day
                , subq_6.customer_third_hop_id__third_hop_ds__week
                , subq_6.customer_third_hop_id__third_hop_ds__month
                , subq_6.customer_third_hop_id__third_hop_ds__quarter
                , subq_6.customer_third_hop_id__third_hop_ds__year
                , subq_6.customer_third_hop_id__third_hop_ds__extract_year
                , subq_6.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_6.customer_third_hop_id__third_hop_ds__extract_month
                , subq_6.customer_third_hop_id__third_hop_ds__extract_day
                , subq_6.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_6.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_6.third_hop_ds__day AS metric_time__day
                , subq_6.third_hop_ds__week AS metric_time__week
                , subq_6.third_hop_ds__month AS metric_time__month
                , subq_6.third_hop_ds__quarter AS metric_time__quarter
                , subq_6.third_hop_ds__year AS metric_time__year
                , subq_6.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_6.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_6.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_6.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_6.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_6.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_6.customer_third_hop_id
                , subq_6.value
                , subq_6.customer_third_hop_id__value
                , subq_6.third_hop_count
              FROM (
                -- Read Elements From Semantic Model 'third_hop_table'
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
                  , third_hop_table_src_22000.value
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS third_hop_ds__day
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS third_hop_ds__week
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS third_hop_ds__month
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS third_hop_ds__quarter
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS customer_third_hop_id__third_hop_ds__day
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS customer_third_hop_id__third_hop_ds__week
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS customer_third_hop_id__third_hop_ds__month
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS customer_third_hop_id__third_hop_ds__quarter
                  , DATE_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS customer_third_hop_id__third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_6
            ) subq_7
          ) subq_8
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
            SELECT
              subq_15.customer_id__customer_third_hop_id
              , subq_15.customer_id__customer_third_hop_id__paraguayan_customers
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_14.customer_id__customer_third_hop_id
                , subq_14.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_13.customer_id__customer_third_hop_id
                  , SUM(subq_13.customers_with_other_data) AS customers_with_other_data
                FROM (
                  -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
                  SELECT
                    subq_12.customer_id__customer_third_hop_id
                    , subq_12.customers_with_other_data
                  FROM (
                    -- Constrain Output with WHERE
                    SELECT
                      subq_11.customer_id__customer_third_hop_id
                      , subq_11.customer_id__country
                      , subq_11.customers_with_other_data
                    FROM (
                      -- Pass Only Elements: ['customers_with_other_data', 'customer_id__country', 'customer_id__customer_third_hop_id']
                      SELECT
                        subq_10.customer_id__customer_third_hop_id
                        , subq_10.customer_id__country
                        , subq_10.customers_with_other_data
                      FROM (
                        -- Metric Time Dimension 'acquired_ds'
                        SELECT
                          subq_9.acquired_ds__day
                          , subq_9.acquired_ds__week
                          , subq_9.acquired_ds__month
                          , subq_9.acquired_ds__quarter
                          , subq_9.acquired_ds__year
                          , subq_9.acquired_ds__extract_year
                          , subq_9.acquired_ds__extract_quarter
                          , subq_9.acquired_ds__extract_month
                          , subq_9.acquired_ds__extract_day
                          , subq_9.acquired_ds__extract_dow
                          , subq_9.acquired_ds__extract_doy
                          , subq_9.customer_id__acquired_ds__day
                          , subq_9.customer_id__acquired_ds__week
                          , subq_9.customer_id__acquired_ds__month
                          , subq_9.customer_id__acquired_ds__quarter
                          , subq_9.customer_id__acquired_ds__year
                          , subq_9.customer_id__acquired_ds__extract_year
                          , subq_9.customer_id__acquired_ds__extract_quarter
                          , subq_9.customer_id__acquired_ds__extract_month
                          , subq_9.customer_id__acquired_ds__extract_day
                          , subq_9.customer_id__acquired_ds__extract_dow
                          , subq_9.customer_id__acquired_ds__extract_doy
                          , subq_9.customer_third_hop_id__acquired_ds__day
                          , subq_9.customer_third_hop_id__acquired_ds__week
                          , subq_9.customer_third_hop_id__acquired_ds__month
                          , subq_9.customer_third_hop_id__acquired_ds__quarter
                          , subq_9.customer_third_hop_id__acquired_ds__year
                          , subq_9.customer_third_hop_id__acquired_ds__extract_year
                          , subq_9.customer_third_hop_id__acquired_ds__extract_quarter
                          , subq_9.customer_third_hop_id__acquired_ds__extract_month
                          , subq_9.customer_third_hop_id__acquired_ds__extract_day
                          , subq_9.customer_third_hop_id__acquired_ds__extract_dow
                          , subq_9.customer_third_hop_id__acquired_ds__extract_doy
                          , subq_9.acquired_ds__day AS metric_time__day
                          , subq_9.acquired_ds__week AS metric_time__week
                          , subq_9.acquired_ds__month AS metric_time__month
                          , subq_9.acquired_ds__quarter AS metric_time__quarter
                          , subq_9.acquired_ds__year AS metric_time__year
                          , subq_9.acquired_ds__extract_year AS metric_time__extract_year
                          , subq_9.acquired_ds__extract_quarter AS metric_time__extract_quarter
                          , subq_9.acquired_ds__extract_month AS metric_time__extract_month
                          , subq_9.acquired_ds__extract_day AS metric_time__extract_day
                          , subq_9.acquired_ds__extract_dow AS metric_time__extract_dow
                          , subq_9.acquired_ds__extract_doy AS metric_time__extract_doy
                          , subq_9.customer_id
                          , subq_9.customer_third_hop_id
                          , subq_9.customer_id__customer_third_hop_id
                          , subq_9.customer_third_hop_id__customer_id
                          , subq_9.country
                          , subq_9.customer_id__country
                          , subq_9.customer_third_hop_id__country
                          , subq_9.customers_with_other_data
                        FROM (
                          -- Read Elements From Semantic Model 'customer_other_data'
                          SELECT
                            1 AS customers_with_other_data
                            , customer_other_data_src_22000.country
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS acquired_ds__day
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS acquired_ds__week
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS acquired_ds__month
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS acquired_ds__quarter
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                            , customer_other_data_src_22000.country AS customer_id__country
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_id__acquired_ds__day
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_id__acquired_ds__week
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_id__acquired_ds__month
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_id__acquired_ds__quarter
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_id__acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_id__acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                            , customer_other_data_src_22000.country AS customer_third_hop_id__country
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_third_hop_id__acquired_ds__day
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_third_hop_id__acquired_ds__week
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_third_hop_id__acquired_ds__month
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_third_hop_id__acquired_ds__quarter
                            , DATE_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_third_hop_id__acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_third_hop_id__acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                            , customer_other_data_src_22000.customer_id
                            , customer_other_data_src_22000.customer_third_hop_id
                            , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                            , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                          FROM ***************************.customer_other_data customer_other_data_src_22000
                        ) subq_9
                      ) subq_10
                    ) subq_11
                    WHERE customer_id__country = 'paraguay'
                  ) subq_12
                ) subq_13
                GROUP BY
                  customer_id__customer_third_hop_id
              ) subq_14
            ) subq_15
          ) subq_16
          ON
            subq_8.customer_third_hop_id = subq_16.customer_id__customer_third_hop_id
        ) subq_17
      ) subq_18
      WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
    ) subq_19
  ) subq_20
) subq_21
