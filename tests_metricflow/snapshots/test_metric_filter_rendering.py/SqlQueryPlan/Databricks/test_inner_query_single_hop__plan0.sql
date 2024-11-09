test_name: test_inner_query_single_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a one-hop join in the inner query.
---
-- Compute Metrics via Expressions
SELECT
  subq_12.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_11.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_10.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_9.third_hop_ds__day
        , subq_9.third_hop_ds__week
        , subq_9.third_hop_ds__month
        , subq_9.third_hop_ds__quarter
        , subq_9.third_hop_ds__year
        , subq_9.third_hop_ds__extract_year
        , subq_9.third_hop_ds__extract_quarter
        , subq_9.third_hop_ds__extract_month
        , subq_9.third_hop_ds__extract_day
        , subq_9.third_hop_ds__extract_dow
        , subq_9.third_hop_ds__extract_doy
        , subq_9.customer_third_hop_id__third_hop_ds__day
        , subq_9.customer_third_hop_id__third_hop_ds__week
        , subq_9.customer_third_hop_id__third_hop_ds__month
        , subq_9.customer_third_hop_id__third_hop_ds__quarter
        , subq_9.customer_third_hop_id__third_hop_ds__year
        , subq_9.customer_third_hop_id__third_hop_ds__extract_year
        , subq_9.customer_third_hop_id__third_hop_ds__extract_quarter
        , subq_9.customer_third_hop_id__third_hop_ds__extract_month
        , subq_9.customer_third_hop_id__third_hop_ds__extract_day
        , subq_9.customer_third_hop_id__third_hop_ds__extract_dow
        , subq_9.customer_third_hop_id__third_hop_ds__extract_doy
        , subq_9.metric_time__day
        , subq_9.metric_time__week
        , subq_9.metric_time__month
        , subq_9.metric_time__quarter
        , subq_9.metric_time__year
        , subq_9.metric_time__extract_year
        , subq_9.metric_time__extract_quarter
        , subq_9.metric_time__extract_month
        , subq_9.metric_time__extract_day
        , subq_9.metric_time__extract_dow
        , subq_9.metric_time__extract_doy
        , subq_9.customer_third_hop_id
        , subq_9.customer_third_hop_id__customer_id__customer_third_hop_id
        , subq_9.value
        , subq_9.customer_third_hop_id__value
        , subq_9.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        , subq_9.third_hop_count
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_8.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
          , subq_8.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
          , subq_1.third_hop_ds__day AS third_hop_ds__day
          , subq_1.third_hop_ds__week AS third_hop_ds__week
          , subq_1.third_hop_ds__month AS third_hop_ds__month
          , subq_1.third_hop_ds__quarter AS third_hop_ds__quarter
          , subq_1.third_hop_ds__year AS third_hop_ds__year
          , subq_1.third_hop_ds__extract_year AS third_hop_ds__extract_year
          , subq_1.third_hop_ds__extract_quarter AS third_hop_ds__extract_quarter
          , subq_1.third_hop_ds__extract_month AS third_hop_ds__extract_month
          , subq_1.third_hop_ds__extract_day AS third_hop_ds__extract_day
          , subq_1.third_hop_ds__extract_dow AS third_hop_ds__extract_dow
          , subq_1.third_hop_ds__extract_doy AS third_hop_ds__extract_doy
          , subq_1.customer_third_hop_id__third_hop_ds__day AS customer_third_hop_id__third_hop_ds__day
          , subq_1.customer_third_hop_id__third_hop_ds__week AS customer_third_hop_id__third_hop_ds__week
          , subq_1.customer_third_hop_id__third_hop_ds__month AS customer_third_hop_id__third_hop_ds__month
          , subq_1.customer_third_hop_id__third_hop_ds__quarter AS customer_third_hop_id__third_hop_ds__quarter
          , subq_1.customer_third_hop_id__third_hop_ds__year AS customer_third_hop_id__third_hop_ds__year
          , subq_1.customer_third_hop_id__third_hop_ds__extract_year AS customer_third_hop_id__third_hop_ds__extract_year
          , subq_1.customer_third_hop_id__third_hop_ds__extract_quarter AS customer_third_hop_id__third_hop_ds__extract_quarter
          , subq_1.customer_third_hop_id__third_hop_ds__extract_month AS customer_third_hop_id__third_hop_ds__extract_month
          , subq_1.customer_third_hop_id__third_hop_ds__extract_day AS customer_third_hop_id__third_hop_ds__extract_day
          , subq_1.customer_third_hop_id__third_hop_ds__extract_dow AS customer_third_hop_id__third_hop_ds__extract_dow
          , subq_1.customer_third_hop_id__third_hop_ds__extract_doy AS customer_third_hop_id__third_hop_ds__extract_doy
          , subq_1.metric_time__day AS metric_time__day
          , subq_1.metric_time__week AS metric_time__week
          , subq_1.metric_time__month AS metric_time__month
          , subq_1.metric_time__quarter AS metric_time__quarter
          , subq_1.metric_time__year AS metric_time__year
          , subq_1.metric_time__extract_year AS metric_time__extract_year
          , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_1.metric_time__extract_month AS metric_time__extract_month
          , subq_1.metric_time__extract_day AS metric_time__extract_day
          , subq_1.metric_time__extract_dow AS metric_time__extract_dow
          , subq_1.metric_time__extract_doy AS metric_time__extract_doy
          , subq_1.customer_third_hop_id AS customer_third_hop_id
          , subq_1.value AS value
          , subq_1.customer_third_hop_id__value AS customer_third_hop_id__value
          , subq_1.third_hop_count AS third_hop_count
        FROM (
          -- Metric Time Dimension 'third_hop_ds'
          SELECT
            subq_0.third_hop_ds__day
            , subq_0.third_hop_ds__week
            , subq_0.third_hop_ds__month
            , subq_0.third_hop_ds__quarter
            , subq_0.third_hop_ds__year
            , subq_0.third_hop_ds__extract_year
            , subq_0.third_hop_ds__extract_quarter
            , subq_0.third_hop_ds__extract_month
            , subq_0.third_hop_ds__extract_day
            , subq_0.third_hop_ds__extract_dow
            , subq_0.third_hop_ds__extract_doy
            , subq_0.customer_third_hop_id__third_hop_ds__day
            , subq_0.customer_third_hop_id__third_hop_ds__week
            , subq_0.customer_third_hop_id__third_hop_ds__month
            , subq_0.customer_third_hop_id__third_hop_ds__quarter
            , subq_0.customer_third_hop_id__third_hop_ds__year
            , subq_0.customer_third_hop_id__third_hop_ds__extract_year
            , subq_0.customer_third_hop_id__third_hop_ds__extract_quarter
            , subq_0.customer_third_hop_id__third_hop_ds__extract_month
            , subq_0.customer_third_hop_id__third_hop_ds__extract_day
            , subq_0.customer_third_hop_id__third_hop_ds__extract_dow
            , subq_0.customer_third_hop_id__third_hop_ds__extract_doy
            , subq_0.third_hop_ds__day AS metric_time__day
            , subq_0.third_hop_ds__week AS metric_time__week
            , subq_0.third_hop_ds__month AS metric_time__month
            , subq_0.third_hop_ds__quarter AS metric_time__quarter
            , subq_0.third_hop_ds__year AS metric_time__year
            , subq_0.third_hop_ds__extract_year AS metric_time__extract_year
            , subq_0.third_hop_ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.third_hop_ds__extract_month AS metric_time__extract_month
            , subq_0.third_hop_ds__extract_day AS metric_time__extract_day
            , subq_0.third_hop_ds__extract_dow AS metric_time__extract_dow
            , subq_0.third_hop_ds__extract_doy AS metric_time__extract_doy
            , subq_0.customer_third_hop_id
            , subq_0.value
            , subq_0.customer_third_hop_id__value
            , subq_0.third_hop_count
          FROM (
            -- Read Elements From Semantic Model 'third_hop_table'
            SELECT
              third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
              , third_hop_table_src_22000.value
              , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__day
              , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__week
              , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__month
              , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__quarter
              , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__year
              , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
              , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
              , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
              , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
              , EXTRACT(DAYOFWEEK_ISO FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_dow
              , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
              , third_hop_table_src_22000.value AS customer_third_hop_id__value
              , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__day
              , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__week
              , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__month
              , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__quarter
              , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__year
              , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
              , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
              , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
              , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
              , EXTRACT(DAYOFWEEK_ISO FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_dow
              , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
              , third_hop_table_src_22000.customer_third_hop_id
            FROM ***************************.third_hop_table third_hop_table_src_22000
          ) subq_0
        ) subq_1
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
          SELECT
            subq_7.customer_id__customer_third_hop_id
            , subq_7.customer_id__customer_third_hop_id__paraguayan_customers
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_6.customer_id__customer_third_hop_id
              , subq_6.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
            FROM (
              -- Aggregate Measures
              SELECT
                subq_5.customer_id__customer_third_hop_id
                , SUM(subq_5.customers_with_other_data) AS customers_with_other_data
              FROM (
                -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
                SELECT
                  subq_4.customer_id__customer_third_hop_id
                  , subq_4.customers_with_other_data
                FROM (
                  -- Constrain Output with WHERE
                  SELECT
                    subq_3.acquired_ds__day
                    , subq_3.acquired_ds__week
                    , subq_3.acquired_ds__month
                    , subq_3.acquired_ds__quarter
                    , subq_3.acquired_ds__year
                    , subq_3.acquired_ds__extract_year
                    , subq_3.acquired_ds__extract_quarter
                    , subq_3.acquired_ds__extract_month
                    , subq_3.acquired_ds__extract_day
                    , subq_3.acquired_ds__extract_dow
                    , subq_3.acquired_ds__extract_doy
                    , subq_3.customer_id__acquired_ds__day
                    , subq_3.customer_id__acquired_ds__week
                    , subq_3.customer_id__acquired_ds__month
                    , subq_3.customer_id__acquired_ds__quarter
                    , subq_3.customer_id__acquired_ds__year
                    , subq_3.customer_id__acquired_ds__extract_year
                    , subq_3.customer_id__acquired_ds__extract_quarter
                    , subq_3.customer_id__acquired_ds__extract_month
                    , subq_3.customer_id__acquired_ds__extract_day
                    , subq_3.customer_id__acquired_ds__extract_dow
                    , subq_3.customer_id__acquired_ds__extract_doy
                    , subq_3.customer_third_hop_id__acquired_ds__day
                    , subq_3.customer_third_hop_id__acquired_ds__week
                    , subq_3.customer_third_hop_id__acquired_ds__month
                    , subq_3.customer_third_hop_id__acquired_ds__quarter
                    , subq_3.customer_third_hop_id__acquired_ds__year
                    , subq_3.customer_third_hop_id__acquired_ds__extract_year
                    , subq_3.customer_third_hop_id__acquired_ds__extract_quarter
                    , subq_3.customer_third_hop_id__acquired_ds__extract_month
                    , subq_3.customer_third_hop_id__acquired_ds__extract_day
                    , subq_3.customer_third_hop_id__acquired_ds__extract_dow
                    , subq_3.customer_third_hop_id__acquired_ds__extract_doy
                    , subq_3.metric_time__day
                    , subq_3.metric_time__week
                    , subq_3.metric_time__month
                    , subq_3.metric_time__quarter
                    , subq_3.metric_time__year
                    , subq_3.metric_time__extract_year
                    , subq_3.metric_time__extract_quarter
                    , subq_3.metric_time__extract_month
                    , subq_3.metric_time__extract_day
                    , subq_3.metric_time__extract_dow
                    , subq_3.metric_time__extract_doy
                    , subq_3.customer_id
                    , subq_3.customer_third_hop_id
                    , subq_3.customer_id__customer_third_hop_id
                    , subq_3.customer_third_hop_id__customer_id
                    , subq_3.country
                    , subq_3.customer_id__country
                    , subq_3.customer_third_hop_id__country
                    , subq_3.customers_with_other_data
                  FROM (
                    -- Metric Time Dimension 'acquired_ds'
                    SELECT
                      subq_2.acquired_ds__day
                      , subq_2.acquired_ds__week
                      , subq_2.acquired_ds__month
                      , subq_2.acquired_ds__quarter
                      , subq_2.acquired_ds__year
                      , subq_2.acquired_ds__extract_year
                      , subq_2.acquired_ds__extract_quarter
                      , subq_2.acquired_ds__extract_month
                      , subq_2.acquired_ds__extract_day
                      , subq_2.acquired_ds__extract_dow
                      , subq_2.acquired_ds__extract_doy
                      , subq_2.customer_id__acquired_ds__day
                      , subq_2.customer_id__acquired_ds__week
                      , subq_2.customer_id__acquired_ds__month
                      , subq_2.customer_id__acquired_ds__quarter
                      , subq_2.customer_id__acquired_ds__year
                      , subq_2.customer_id__acquired_ds__extract_year
                      , subq_2.customer_id__acquired_ds__extract_quarter
                      , subq_2.customer_id__acquired_ds__extract_month
                      , subq_2.customer_id__acquired_ds__extract_day
                      , subq_2.customer_id__acquired_ds__extract_dow
                      , subq_2.customer_id__acquired_ds__extract_doy
                      , subq_2.customer_third_hop_id__acquired_ds__day
                      , subq_2.customer_third_hop_id__acquired_ds__week
                      , subq_2.customer_third_hop_id__acquired_ds__month
                      , subq_2.customer_third_hop_id__acquired_ds__quarter
                      , subq_2.customer_third_hop_id__acquired_ds__year
                      , subq_2.customer_third_hop_id__acquired_ds__extract_year
                      , subq_2.customer_third_hop_id__acquired_ds__extract_quarter
                      , subq_2.customer_third_hop_id__acquired_ds__extract_month
                      , subq_2.customer_third_hop_id__acquired_ds__extract_day
                      , subq_2.customer_third_hop_id__acquired_ds__extract_dow
                      , subq_2.customer_third_hop_id__acquired_ds__extract_doy
                      , subq_2.acquired_ds__day AS metric_time__day
                      , subq_2.acquired_ds__week AS metric_time__week
                      , subq_2.acquired_ds__month AS metric_time__month
                      , subq_2.acquired_ds__quarter AS metric_time__quarter
                      , subq_2.acquired_ds__year AS metric_time__year
                      , subq_2.acquired_ds__extract_year AS metric_time__extract_year
                      , subq_2.acquired_ds__extract_quarter AS metric_time__extract_quarter
                      , subq_2.acquired_ds__extract_month AS metric_time__extract_month
                      , subq_2.acquired_ds__extract_day AS metric_time__extract_day
                      , subq_2.acquired_ds__extract_dow AS metric_time__extract_dow
                      , subq_2.acquired_ds__extract_doy AS metric_time__extract_doy
                      , subq_2.customer_id
                      , subq_2.customer_third_hop_id
                      , subq_2.customer_id__customer_third_hop_id
                      , subq_2.customer_third_hop_id__customer_id
                      , subq_2.country
                      , subq_2.customer_id__country
                      , subq_2.customer_third_hop_id__country
                      , subq_2.customers_with_other_data
                    FROM (
                      -- Read Elements From Semantic Model 'customer_other_data'
                      SELECT
                        1 AS customers_with_other_data
                        , customer_other_data_src_22000.country
                        , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS acquired_ds__day
                        , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS acquired_ds__week
                        , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS acquired_ds__month
                        , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS acquired_ds__quarter
                        , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS acquired_ds__year
                        , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                        , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                        , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                        , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                        , EXTRACT(DAYOFWEEK_ISO FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_dow
                        , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                        , customer_other_data_src_22000.country AS customer_id__country
                        , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__day
                        , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__week
                        , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__month
                        , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__quarter
                        , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__year
                        , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                        , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                        , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                        , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                        , EXTRACT(DAYOFWEEK_ISO FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_dow
                        , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                        , customer_other_data_src_22000.country AS customer_third_hop_id__country
                        , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__day
                        , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__week
                        , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__month
                        , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__quarter
                        , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__year
                        , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                        , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                        , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                        , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                        , EXTRACT(DAYOFWEEK_ISO FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_dow
                        , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                        , customer_other_data_src_22000.customer_id
                        , customer_other_data_src_22000.customer_third_hop_id
                        , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                        , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                      FROM ***************************.customer_other_data customer_other_data_src_22000
                    ) subq_2
                  ) subq_3
                  WHERE customer_id__country = 'paraguay'
                ) subq_4
              ) subq_5
              GROUP BY
                subq_5.customer_id__customer_third_hop_id
            ) subq_6
          ) subq_7
        ) subq_8
        ON
          subq_1.customer_third_hop_id = subq_8.customer_id__customer_third_hop_id
      ) subq_9
      WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
    ) subq_10
  ) subq_11
) subq_12
