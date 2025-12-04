test_name: test_homonymous_metric_and_entity
test_filename: test_name_edge_cases.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as an entity.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_6.metric_time__day
  , subq_6.homonymous_metric_and_entity
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_5.metric_time__day
    , subq_5.__homonymous_metric_and_entity AS homonymous_metric_and_entity
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_4.metric_time__day
      , SUM(subq_4.__homonymous_metric_and_entity) AS __homonymous_metric_and_entity
    FROM (
      -- Pass Only Elements: ['__homonymous_metric_and_entity', 'metric_time__day']
      SELECT
        subq_3.metric_time__day
        , subq_3.__homonymous_metric_and_entity
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_2.homonymous_metric_and_entity AS __homonymous_metric_and_entity
          , subq_2.metric_time__day
        FROM (
          -- Pass Only Elements: ['__homonymous_metric_and_entity', 'metric_time__day']
          SELECT
            subq_1.metric_time__day
            , subq_1.__homonymous_metric_and_entity AS homonymous_metric_and_entity
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_0.ds__day
              , subq_0.ds__week
              , subq_0.ds__month
              , subq_0.ds__quarter
              , subq_0.ds__year
              , subq_0.ds__extract_year
              , subq_0.ds__extract_quarter
              , subq_0.ds__extract_month
              , subq_0.ds__extract_day
              , subq_0.ds__extract_dow
              , subq_0.ds__extract_doy
              , subq_0.booking__ds__day
              , subq_0.booking__ds__week
              , subq_0.booking__ds__month
              , subq_0.booking__ds__quarter
              , subq_0.booking__ds__year
              , subq_0.booking__ds__extract_year
              , subq_0.booking__ds__extract_quarter
              , subq_0.booking__ds__extract_month
              , subq_0.booking__ds__extract_day
              , subq_0.booking__ds__extract_dow
              , subq_0.booking__ds__extract_doy
              , subq_0.homonymous_metric_and_entity__ds__day
              , subq_0.homonymous_metric_and_entity__ds__week
              , subq_0.homonymous_metric_and_entity__ds__month
              , subq_0.homonymous_metric_and_entity__ds__quarter
              , subq_0.homonymous_metric_and_entity__ds__year
              , subq_0.homonymous_metric_and_entity__ds__extract_year
              , subq_0.homonymous_metric_and_entity__ds__extract_quarter
              , subq_0.homonymous_metric_and_entity__ds__extract_month
              , subq_0.homonymous_metric_and_entity__ds__extract_day
              , subq_0.homonymous_metric_and_entity__ds__extract_dow
              , subq_0.homonymous_metric_and_entity__ds__extract_doy
              , subq_0.ds__day AS metric_time__day
              , subq_0.ds__week AS metric_time__week
              , subq_0.ds__month AS metric_time__month
              , subq_0.ds__quarter AS metric_time__quarter
              , subq_0.ds__year AS metric_time__year
              , subq_0.ds__extract_year AS metric_time__extract_year
              , subq_0.ds__extract_quarter AS metric_time__extract_quarter
              , subq_0.ds__extract_month AS metric_time__extract_month
              , subq_0.ds__extract_day AS metric_time__extract_day
              , subq_0.ds__extract_dow AS metric_time__extract_dow
              , subq_0.ds__extract_doy AS metric_time__extract_doy
              , subq_0.homonymous_metric_and_entity
              , subq_0.booking__homonymous_metric_and_entity
              , subq_0.homonymous_metric_and_dimension
              , subq_0.booking__homonymous_metric_and_dimension
              , subq_0.homonymous_metric_and_entity__homonymous_metric_and_dimension
              , subq_0.__homonymous_metric_and_dimension
              , subq_0.__homonymous_metric_and_entity
            FROM (
              -- Read Elements From Semantic Model 'bookings_source'
              SELECT
                bookings_source_src_32000.booking_value AS __homonymous_metric_and_dimension
                , bookings_source_src_32000.booking_value AS __homonymous_metric_and_entity
                , 'dummy_dimension_value' AS homonymous_metric_and_dimension
                , DATE_TRUNC('day', bookings_source_src_32000.ds) AS ds__day
                , DATE_TRUNC('week', bookings_source_src_32000.ds) AS ds__week
                , DATE_TRUNC('month', bookings_source_src_32000.ds) AS ds__month
                , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS ds__quarter
                , DATE_TRUNC('year', bookings_source_src_32000.ds) AS ds__year
                , EXTRACT(year FROM bookings_source_src_32000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_32000.ds) AS ds__extract_month
                , EXTRACT(day FROM bookings_source_src_32000.ds) AS ds__extract_day
                , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_32000.ds) AS ds__extract_doy
                , 'dummy_dimension_value' AS booking__homonymous_metric_and_dimension
                , DATE_TRUNC('day', bookings_source_src_32000.ds) AS booking__ds__day
                , DATE_TRUNC('week', bookings_source_src_32000.ds) AS booking__ds__week
                , DATE_TRUNC('month', bookings_source_src_32000.ds) AS booking__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS booking__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_32000.ds) AS booking__ds__year
                , EXTRACT(year FROM bookings_source_src_32000.ds) AS booking__ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS booking__ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_32000.ds) AS booking__ds__extract_month
                , EXTRACT(day FROM bookings_source_src_32000.ds) AS booking__ds__extract_day
                , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS booking__ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_32000.ds) AS booking__ds__extract_doy
                , 'dummy_dimension_value' AS homonymous_metric_and_entity__homonymous_metric_and_dimension
                , DATE_TRUNC('day', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__day
                , DATE_TRUNC('week', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__week
                , DATE_TRUNC('month', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__year
                , EXTRACT(year FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_month
                , EXTRACT(day FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_day
                , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_doy
                , 'dummy_entity_value' AS homonymous_metric_and_entity
                , 'dummy_entity_value' AS booking__homonymous_metric_and_entity
              FROM ***************************.fct_bookings bookings_source_src_32000
            ) subq_0
          ) subq_1
        ) subq_2
        WHERE homonymous_metric_and_entity IS NOT NULL
      ) subq_3
    ) subq_4
    GROUP BY
      subq_4.metric_time__day
  ) subq_5
) subq_6
