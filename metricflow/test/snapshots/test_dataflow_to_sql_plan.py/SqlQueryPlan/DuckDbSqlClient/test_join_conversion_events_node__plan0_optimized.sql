-- Find conversions for IdentifierSpec(element_name='user', identifier_links=()) within the range of 7 day
SELECT
  ds
  , ds__week
  , ds__month
  , ds__quarter
  , ds__year
  , subq_7.user
  , session
  , referrer_id
  , buys
  , visits
  , visitors
FROM (
  -- Dedupe the fanout on (MetadataSpec(element_name='mf_internal_uuid'),) in the conversion data set
  SELECT DISTINCT
    first_value(subq_4.visits) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS visits
    , first_value(subq_4.visitors) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS visitors
    , first_value(subq_4.referrer_id) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS referrer_id
    , first_value(subq_4.ds) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS ds
    , first_value(subq_4.ds__week) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS ds__week
    , first_value(subq_4.ds__month) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS ds__month
    , first_value(subq_4.ds__quarter) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS ds__quarter
    , first_value(subq_4.ds__year) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS ds__year
    , first_value(subq_4.user) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS user
    , first_value(subq_4.session) OVER (PARTITION BY subq_6.user, subq_6.ds ORDER BY subq_4.ds DESC) AS session
    , subq_6.mf_internal_uuid AS mf_internal_uuid
    , subq_6.buys AS buys
  FROM (
    -- Read Elements From Data Source 'visits_source'
    SELECT
      1 AS visits
      , user_id AS visitors
      , ds
      , DATE_TRUNC('week', ds) AS ds__week
      , DATE_TRUNC('month', ds) AS ds__month
      , DATE_TRUNC('quarter', ds) AS ds__quarter
      , DATE_TRUNC('year', ds) AS ds__year
      , referrer_id
      , user_id AS user
      , session_id AS session
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_visits
    ) visits_source_src_10011
  ) subq_4
  INNER JOIN (
    -- Read Elements From Data Source 'buys_source'
    -- Add column with generated UUID
    SELECT
      ds
      , user_id AS user
      , 1 AS buys
      , GEN_RANDOM_UUID() AS mf_internal_uuid
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_buys
    ) buys_source_src_10002
  ) subq_6
  ON
    (
      subq_4.user = subq_6.user
    ) AND (
      (
        subq_4.ds <= subq_6.ds
      ) AND (
        subq_4.ds > subq_6.ds - INTERVAL 7 day
      )
    )
) subq_7
