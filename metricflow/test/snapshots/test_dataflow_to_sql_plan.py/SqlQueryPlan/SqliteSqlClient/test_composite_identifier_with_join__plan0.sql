-- Compute Metrics via Expressions
SELECT
  subq_6.messages
  , subq_6.user_team__country
  , subq_6.user_team___team_id
  , subq_6.user_team___user_id
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_5.messages) AS messages
    , subq_5.user_team__country
    , subq_5.user_team___team_id
    , subq_5.user_team___user_id
  FROM (
    -- Pass Only Elements:
    --   ['messages', 'user_team__country', 'user_team']
    SELECT
      subq_4.messages
      , subq_4.user_team__country
      , subq_4.user_team___team_id
      , subq_4.user_team___user_id
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_1.messages AS messages
        , subq_3.country AS user_team__country
        , subq_1.user_team___team_id AS user_team___team_id
        , subq_1.user_team___user_id AS user_team___user_id
      FROM (
        -- Pass Only Elements:
        --   ['messages', 'user_team', 'user_team']
        SELECT
          subq_0.messages
          , subq_0.user_team___team_id
          , subq_0.user_team___user_id
        FROM (
          -- Read Elements From Data Source 'messages_source'
          SELECT
            1 AS messages
            , messages_source_src_10014.ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
            , messages_source_src_10014.team_id
            , messages_source_src_10014.ds AS user_id__ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__year
            , messages_source_src_10014.team_id AS user_id__team_id
            , messages_source_src_10014.user_id
            , messages_source_src_10014.team_id AS user_team___team_id
            , messages_source_src_10014.user_id AS user_team___user_id
            , messages_source_src_10014.team_id AS user_id__user_team___team_id
            , messages_source_src_10014.user_id AS user_id__user_team___user_id
          FROM ***************************.fct_messages messages_source_src_10014
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['user_team', 'country']
        SELECT
          subq_2.country
          , subq_2.user_team___team_id
          , subq_2.user_team___user_id
        FROM (
          -- Read Elements From Data Source 'users_source'
          SELECT
            users_source_src_10016.created_at AS ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
            , users_source_src_10016.team_id
            , users_source_src_10016.country
            , users_source_src_10016.created_at AS user_id__ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_id__ds__year
            , users_source_src_10016.team_id AS user_id__team_id
            , users_source_src_10016.country AS user_id__country
            , users_source_src_10016.created_at AS user_composite_ident_2__ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_composite_ident_2__ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_composite_ident_2__ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_composite_ident_2__ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_composite_ident_2__ds__year
            , users_source_src_10016.team_id AS user_composite_ident_2__team_id
            , users_source_src_10016.country AS user_composite_ident_2__country
            , users_source_src_10016.created_at AS user_team__ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_team__ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_team__ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_team__ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS user_team__ds__year
            , users_source_src_10016.team_id AS user_team__team_id
            , users_source_src_10016.country AS user_team__country
            , users_source_src_10016.id AS user_id
            , users_source_src_10016.ident_2 AS user_composite_ident_2___ident_2
            , users_source_src_10016.id AS user_composite_ident_2___user_id
            , users_source_src_10016.team_id AS user_team___team_id
            , users_source_src_10016.id AS user_team___user_id
            , users_source_src_10016.ident_2 AS user_id__user_composite_ident_2___ident_2
            , users_source_src_10016.id AS user_id__user_composite_ident_2___user_id
            , users_source_src_10016.team_id AS user_id__user_team___team_id
            , users_source_src_10016.id AS user_id__user_team___user_id
            , users_source_src_10016.id AS user_composite_ident_2__user_id
            , users_source_src_10016.team_id AS user_composite_ident_2__user_team___team_id
            , users_source_src_10016.id AS user_composite_ident_2__user_team___user_id
            , users_source_src_10016.id AS user_team__user_id
            , users_source_src_10016.ident_2 AS user_team__user_composite_ident_2___ident_2
            , users_source_src_10016.id AS user_team__user_composite_ident_2___user_id
          FROM ***************************.fct_users users_source_src_10016
        ) subq_2
      ) subq_3
      ON
        (
          subq_1.user_team___team_id = subq_3.user_team___team_id
        ) AND (
          subq_1.user_team___user_id = subq_3.user_team___user_id
        )
    ) subq_4
  ) subq_5
  GROUP BY
    subq_5.user_team__country
    , subq_5.user_team___team_id
    , subq_5.user_team___user_id
) subq_6
