test_name: test_inner_query_single_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a one-hop join in the inner query.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_20.third_hop_count
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_19.__third_hop_count AS third_hop_count
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      COUNT(DISTINCT subq_18.__third_hop_count) AS __third_hop_count
    FROM (
      -- Pass Only Elements: ['__third_hop_count']
      SELECT
        subq_17.__third_hop_count
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_16.third_hop_count AS __third_hop_count
          , subq_16.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        FROM (
          -- Pass Only Elements: ['__third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
          SELECT
            subq_15.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
            , subq_15.__third_hop_count AS third_hop_count
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_14.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
              , subq_14.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
              , subq_6.third_hop_ds__day AS third_hop_ds__day
              , subq_6.third_hop_ds__week AS third_hop_ds__week
              , subq_6.third_hop_ds__month AS third_hop_ds__month
              , subq_6.third_hop_ds__quarter AS third_hop_ds__quarter
              , subq_6.third_hop_ds__year AS third_hop_ds__year
              , subq_6.third_hop_ds__extract_year AS third_hop_ds__extract_year
              , subq_6.third_hop_ds__extract_quarter AS third_hop_ds__extract_quarter
              , subq_6.third_hop_ds__extract_month AS third_hop_ds__extract_month
              , subq_6.third_hop_ds__extract_day AS third_hop_ds__extract_day
              , subq_6.third_hop_ds__extract_dow AS third_hop_ds__extract_dow
              , subq_6.third_hop_ds__extract_doy AS third_hop_ds__extract_doy
              , subq_6.customer_third_hop_id__third_hop_ds__day AS customer_third_hop_id__third_hop_ds__day
              , subq_6.customer_third_hop_id__third_hop_ds__week AS customer_third_hop_id__third_hop_ds__week
              , subq_6.customer_third_hop_id__third_hop_ds__month AS customer_third_hop_id__third_hop_ds__month
              , subq_6.customer_third_hop_id__third_hop_ds__quarter AS customer_third_hop_id__third_hop_ds__quarter
              , subq_6.customer_third_hop_id__third_hop_ds__year AS customer_third_hop_id__third_hop_ds__year
              , subq_6.customer_third_hop_id__third_hop_ds__extract_year AS customer_third_hop_id__third_hop_ds__extract_year
              , subq_6.customer_third_hop_id__third_hop_ds__extract_quarter AS customer_third_hop_id__third_hop_ds__extract_quarter
              , subq_6.customer_third_hop_id__third_hop_ds__extract_month AS customer_third_hop_id__third_hop_ds__extract_month
              , subq_6.customer_third_hop_id__third_hop_ds__extract_day AS customer_third_hop_id__third_hop_ds__extract_day
              , subq_6.customer_third_hop_id__third_hop_ds__extract_dow AS customer_third_hop_id__third_hop_ds__extract_dow
              , subq_6.customer_third_hop_id__third_hop_ds__extract_doy AS customer_third_hop_id__third_hop_ds__extract_doy
              , subq_6.metric_time__day AS metric_time__day
              , subq_6.metric_time__week AS metric_time__week
              , subq_6.metric_time__month AS metric_time__month
              , subq_6.metric_time__quarter AS metric_time__quarter
              , subq_6.metric_time__year AS metric_time__year
              , subq_6.metric_time__extract_year AS metric_time__extract_year
              , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_6.metric_time__extract_month AS metric_time__extract_month
              , subq_6.metric_time__extract_day AS metric_time__extract_day
              , subq_6.metric_time__extract_dow AS metric_time__extract_dow
              , subq_6.metric_time__extract_doy AS metric_time__extract_doy
              , subq_6.customer_third_hop_id AS customer_third_hop_id
              , subq_6.value AS value
              , subq_6.customer_third_hop_id__value AS customer_third_hop_id__value
              , subq_6.__third_hop_count AS __third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_5.third_hop_ds__day
                , subq_5.third_hop_ds__week
                , subq_5.third_hop_ds__month
                , subq_5.third_hop_ds__quarter
                , subq_5.third_hop_ds__year
                , subq_5.third_hop_ds__extract_year
                , subq_5.third_hop_ds__extract_quarter
                , subq_5.third_hop_ds__extract_month
                , subq_5.third_hop_ds__extract_day
                , subq_5.third_hop_ds__extract_dow
                , subq_5.third_hop_ds__extract_doy
                , subq_5.customer_third_hop_id__third_hop_ds__day
                , subq_5.customer_third_hop_id__third_hop_ds__week
                , subq_5.customer_third_hop_id__third_hop_ds__month
                , subq_5.customer_third_hop_id__third_hop_ds__quarter
                , subq_5.customer_third_hop_id__third_hop_ds__year
                , subq_5.customer_third_hop_id__third_hop_ds__extract_year
                , subq_5.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_5.customer_third_hop_id__third_hop_ds__extract_month
                , subq_5.customer_third_hop_id__third_hop_ds__extract_day
                , subq_5.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_5.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_5.third_hop_ds__day AS metric_time__day
                , subq_5.third_hop_ds__week AS metric_time__week
                , subq_5.third_hop_ds__month AS metric_time__month
                , subq_5.third_hop_ds__quarter AS metric_time__quarter
                , subq_5.third_hop_ds__year AS metric_time__year
                , subq_5.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_5.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_5.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_5.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_5.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_5.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_5.customer_third_hop_id
                , subq_5.value
                , subq_5.customer_third_hop_id__value
                , subq_5.__third_hop_count
              FROM (
                -- Read Elements From Semantic Model 'third_hop_table'
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS __third_hop_count
                  , third_hop_table_src_22000.value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS customer_third_hop_id__third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS customer_third_hop_id__third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS customer_third_hop_id__third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS customer_third_hop_id__third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS customer_third_hop_id__third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_5
            ) subq_6
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
              SELECT
                subq_13.customer_id__customer_third_hop_id
                , subq_13.customer_id__customer_third_hop_id__paraguayan_customers
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_12.customer_id__customer_third_hop_id
                  , subq_12.__paraguayan_customers AS customer_id__customer_third_hop_id__paraguayan_customers
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_11.customer_id__customer_third_hop_id
                    , SUM(subq_11.__paraguayan_customers) AS __paraguayan_customers
                  FROM (
                    -- Pass Only Elements: ['__paraguayan_customers', 'customer_id__customer_third_hop_id']
                    SELECT
                      subq_10.customer_id__customer_third_hop_id
                      , subq_10.__paraguayan_customers
                    FROM (
                      -- Constrain Output with WHERE
                      SELECT
                        subq_9.paraguayan_customers AS __paraguayan_customers
                        , subq_9.customer_id__country
                        , subq_9.customer_id__customer_third_hop_id
                      FROM (
                        -- Pass Only Elements: ['__paraguayan_customers', 'customer_id__country', 'customer_id__customer_third_hop_id']
                        SELECT
                          subq_8.customer_id__customer_third_hop_id
                          , subq_8.customer_id__country
                          , subq_8.__paraguayan_customers AS paraguayan_customers
                        FROM (
                          -- Metric Time Dimension 'acquired_ds'
                          SELECT
                            subq_7.acquired_ds__day
                            , subq_7.acquired_ds__week
                            , subq_7.acquired_ds__month
                            , subq_7.acquired_ds__quarter
                            , subq_7.acquired_ds__year
                            , subq_7.acquired_ds__extract_year
                            , subq_7.acquired_ds__extract_quarter
                            , subq_7.acquired_ds__extract_month
                            , subq_7.acquired_ds__extract_day
                            , subq_7.acquired_ds__extract_dow
                            , subq_7.acquired_ds__extract_doy
                            , subq_7.customer_id__acquired_ds__day
                            , subq_7.customer_id__acquired_ds__week
                            , subq_7.customer_id__acquired_ds__month
                            , subq_7.customer_id__acquired_ds__quarter
                            , subq_7.customer_id__acquired_ds__year
                            , subq_7.customer_id__acquired_ds__extract_year
                            , subq_7.customer_id__acquired_ds__extract_quarter
                            , subq_7.customer_id__acquired_ds__extract_month
                            , subq_7.customer_id__acquired_ds__extract_day
                            , subq_7.customer_id__acquired_ds__extract_dow
                            , subq_7.customer_id__acquired_ds__extract_doy
                            , subq_7.customer_third_hop_id__acquired_ds__day
                            , subq_7.customer_third_hop_id__acquired_ds__week
                            , subq_7.customer_third_hop_id__acquired_ds__month
                            , subq_7.customer_third_hop_id__acquired_ds__quarter
                            , subq_7.customer_third_hop_id__acquired_ds__year
                            , subq_7.customer_third_hop_id__acquired_ds__extract_year
                            , subq_7.customer_third_hop_id__acquired_ds__extract_quarter
                            , subq_7.customer_third_hop_id__acquired_ds__extract_month
                            , subq_7.customer_third_hop_id__acquired_ds__extract_day
                            , subq_7.customer_third_hop_id__acquired_ds__extract_dow
                            , subq_7.customer_third_hop_id__acquired_ds__extract_doy
                            , subq_7.acquired_ds__day AS metric_time__day
                            , subq_7.acquired_ds__week AS metric_time__week
                            , subq_7.acquired_ds__month AS metric_time__month
                            , subq_7.acquired_ds__quarter AS metric_time__quarter
                            , subq_7.acquired_ds__year AS metric_time__year
                            , subq_7.acquired_ds__extract_year AS metric_time__extract_year
                            , subq_7.acquired_ds__extract_quarter AS metric_time__extract_quarter
                            , subq_7.acquired_ds__extract_month AS metric_time__extract_month
                            , subq_7.acquired_ds__extract_day AS metric_time__extract_day
                            , subq_7.acquired_ds__extract_dow AS metric_time__extract_dow
                            , subq_7.acquired_ds__extract_doy AS metric_time__extract_doy
                            , subq_7.customer_id
                            , subq_7.customer_third_hop_id
                            , subq_7.customer_id__customer_third_hop_id
                            , subq_7.customer_third_hop_id__customer_id
                            , subq_7.country
                            , subq_7.customer_id__country
                            , subq_7.customer_third_hop_id__country
                            , subq_7.__paraguayan_customers
                          FROM (
                            -- Read Elements From Semantic Model 'customer_other_data'
                            SELECT
                              1 AS __paraguayan_customers
                              , customer_other_data_src_22000.country
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS acquired_ds__day
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS acquired_ds__week
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS acquired_ds__month
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS acquired_ds__quarter
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS acquired_ds__year
                              , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                              , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                              , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                              , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                              , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS acquired_ds__extract_dow
                              , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                              , customer_other_data_src_22000.country AS customer_id__country
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_id__acquired_ds__day
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_id__acquired_ds__week
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_id__acquired_ds__month
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_id__acquired_ds__quarter
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_id__acquired_ds__year
                              , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                              , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                              , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                              , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                              , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_id__acquired_ds__extract_dow
                              , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                              , customer_other_data_src_22000.country AS customer_third_hop_id__country
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_third_hop_id__acquired_ds__day
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_third_hop_id__acquired_ds__week
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_third_hop_id__acquired_ds__month
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_third_hop_id__acquired_ds__quarter
                              , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_third_hop_id__acquired_ds__year
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
                          ) subq_7
                        ) subq_8
                      ) subq_9
                      WHERE customer_id__country = 'paraguay'
                    ) subq_10
                  ) subq_11
                  GROUP BY
                    customer_id__customer_third_hop_id
                ) subq_12
              ) subq_13
            ) subq_14
            ON
              subq_6.customer_third_hop_id = subq_14.customer_id__customer_third_hop_id
          ) subq_15
        ) subq_16
        WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
      ) subq_17
    ) subq_18
  ) subq_19
) subq_20
