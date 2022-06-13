-- Order By ['user_team']
SELECT
  subq_3.messages
  , subq_3.user_team___team_id
  , subq_3.user_team___user_id
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_2.messages
    , subq_2.user_team___team_id
    , subq_2.user_team___user_id
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_1.messages) AS messages
      , subq_1.user_team___team_id
      , subq_1.user_team___user_id
    FROM (
      -- Pass Only Elements:
      --   ['messages', 'user_team']
      SELECT
        subq_0.messages
        , subq_0.user_team___team_id
        , subq_0.user_team___user_id
      FROM (
        -- Read Elements From Data Source 'messages_source'
        SELECT
          1 AS messages
          , messages_source_src_10014.ds
          , DATE_TRUNC('week', messages_source_src_10014.ds) AS ds__week
          , DATE_TRUNC('month', messages_source_src_10014.ds) AS ds__month
          , DATE_TRUNC('quarter', messages_source_src_10014.ds) AS ds__quarter
          , DATE_TRUNC('year', messages_source_src_10014.ds) AS ds__year
          , messages_source_src_10014.team_id
          , messages_source_src_10014.ds AS user_id__ds
          , DATE_TRUNC('week', messages_source_src_10014.ds) AS user_id__ds__week
          , DATE_TRUNC('month', messages_source_src_10014.ds) AS user_id__ds__month
          , DATE_TRUNC('quarter', messages_source_src_10014.ds) AS user_id__ds__quarter
          , DATE_TRUNC('year', messages_source_src_10014.ds) AS user_id__ds__year
          , messages_source_src_10014.team_id AS user_id__team_id
          , messages_source_src_10014.user_id
          , messages_source_src_10014.team_id AS user_team___team_id
          , messages_source_src_10014.user_id AS user_team___user_id
          , messages_source_src_10014.team_id AS user_id__user_team___team_id
          , messages_source_src_10014.user_id AS user_id__user_team___user_id
        FROM ***************************.fct_messages messages_source_src_10014
      ) subq_0
    ) subq_1
    GROUP BY
      subq_1.user_team___team_id
      , subq_1.user_team___user_id
  ) subq_2
) subq_3
ORDER BY subq_3.user_team___team_id DESC, subq_3.user_team___user_id DESC
