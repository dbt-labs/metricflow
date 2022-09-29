-- Compute Metrics via Expressions
SELECT
  subq_3.user_team___team_id
  , subq_3.user_team___user_id
  , subq_3.messages
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.user_team___team_id
    , subq_2.user_team___user_id
    , SUM(subq_2.messages) AS messages
  FROM (
    -- Pass Only Elements:
    --   ['messages', 'user_team']
    SELECT
      subq_1.user_team___team_id
      , subq_1.user_team___user_id
      , subq_1.messages
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        subq_0.ds
        , subq_0.ds__week
        , subq_0.ds__month
        , subq_0.ds__quarter
        , subq_0.ds__year
        , subq_0.user_id__ds
        , subq_0.user_id__ds__week
        , subq_0.user_id__ds__month
        , subq_0.user_id__ds__quarter
        , subq_0.user_id__ds__year
        , subq_0.ds AS metric_time
        , subq_0.ds__week AS metric_time__week
        , subq_0.ds__month AS metric_time__month
        , subq_0.ds__quarter AS metric_time__quarter
        , subq_0.ds__year AS metric_time__year
        , subq_0.user_id
        , subq_0.user_team___team_id
        , subq_0.user_team___user_id
        , subq_0.user_id__user_team___team_id
        , subq_0.user_id__user_team___user_id
        , subq_0.team_id
        , subq_0.user_id__team_id
        , subq_0.messages
      FROM (
        -- Read Elements From Data Source 'messages_source'
        SELECT
          1 AS messages
          , messages_source_src_10015.ds
          , DATE_TRUNC(messages_source_src_10015.ds, isoweek) AS ds__week
          , DATE_TRUNC(messages_source_src_10015.ds, month) AS ds__month
          , DATE_TRUNC(messages_source_src_10015.ds, quarter) AS ds__quarter
          , DATE_TRUNC(messages_source_src_10015.ds, isoyear) AS ds__year
          , messages_source_src_10015.team_id
          , messages_source_src_10015.ds AS user_id__ds
          , DATE_TRUNC(messages_source_src_10015.ds, isoweek) AS user_id__ds__week
          , DATE_TRUNC(messages_source_src_10015.ds, month) AS user_id__ds__month
          , DATE_TRUNC(messages_source_src_10015.ds, quarter) AS user_id__ds__quarter
          , DATE_TRUNC(messages_source_src_10015.ds, isoyear) AS user_id__ds__year
          , messages_source_src_10015.team_id AS user_id__team_id
          , messages_source_src_10015.user_id
          , messages_source_src_10015.team_id AS user_team___team_id
          , messages_source_src_10015.user_id AS user_team___user_id
          , messages_source_src_10015.team_id AS user_id__user_team___team_id
          , messages_source_src_10015.user_id AS user_id__user_team___user_id
        FROM ***************************.fct_messages messages_source_src_10015
      ) subq_0
    ) subq_1
  ) subq_2
  GROUP BY
    subq_2.user_team___team_id
    , subq_2.user_team___user_id
) subq_3
