-- Find conversions for IdentifierSpec(element_name='user', identifier_links=()) within the range of 7 day
SELECT
  subq_3.ds
  , subq_3.ds__week
  , subq_3.ds__month
  , subq_3.ds__quarter
  , subq_3.ds__year
  , subq_3.user
  , subq_3.referrer_id
  , subq_3.buys
  , subq_3.visits
  , subq_3.visitors
FROM (
  -- Dedupe the fanout on (MetadataSpec(element_name='mf_internal_uuid'),) in the conversion data set
  SELECT DISTINCT
    first_value(subq_0.visits) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS visits
    , first_value(subq_0.visitors) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS visitors
    , first_value(subq_0.referrer_id) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS referrer_id
    , first_value(subq_0.ds) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS ds
    , first_value(subq_0.ds__week) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS ds__week
    , first_value(subq_0.ds__month) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS ds__month
    , first_value(subq_0.ds__quarter) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS ds__quarter
    , first_value(subq_0.ds__year) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS ds__year
    , first_value(subq_0.user) OVER (PARTITION BY subq_2.user, subq_2.ds ORDER BY subq_0.ds DESC) AS user
    , subq_2.mf_internal_uuid AS mf_internal_uuid
    , subq_2.buys AS buys
  FROM (
    -- Read Elements From Data Source 'visits_source'
    SELECT
      1 AS visits
      , visits_source_src_10011.user_id AS visitors
      , visits_source_src_10011.ds
      , DATE_TRUNC('week', visits_source_src_10011.ds) AS ds__week
      , DATE_TRUNC('month', visits_source_src_10011.ds) AS ds__month
      , DATE_TRUNC('quarter', visits_source_src_10011.ds) AS ds__quarter
      , DATE_TRUNC('year', visits_source_src_10011.ds) AS ds__year
      , visits_source_src_10011.referrer_id
      , visits_source_src_10011.user_id AS user
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_visits
    ) visits_source_src_10011
  ) subq_0
  INNER JOIN (
    -- Add column with generated UUID
    SELECT
      subq_1.ds
      , subq_1.ds__week
      , subq_1.ds__month
      , subq_1.ds__quarter
      , subq_1.ds__year
      , subq_1.user
      , subq_1.buys
      , subq_1.buyers
      , GEN_RANDOM_UUID() AS mf_internal_uuid
    FROM (
      -- Read Elements From Data Source 'buys_source'
      SELECT
        1 AS buys
        , buys_source_src_10002.user_id AS buyers
        , buys_source_src_10002.ds
        , DATE_TRUNC('week', buys_source_src_10002.ds) AS ds__week
        , DATE_TRUNC('month', buys_source_src_10002.ds) AS ds__month
        , DATE_TRUNC('quarter', buys_source_src_10002.ds) AS ds__quarter
        , DATE_TRUNC('year', buys_source_src_10002.ds) AS ds__year
        , buys_source_src_10002.user_id AS user
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_buys
      ) buys_source_src_10002
    ) subq_1
  ) subq_2
  ON
    (
      subq_0.user = subq_2.user
    ) AND (
      (
        subq_0.ds <= subq_2.ds
      ) AND (
        subq_0.ds > subq_2.ds - MAKE_INTERVAL(days => 7)
      )
    )
) subq_3
