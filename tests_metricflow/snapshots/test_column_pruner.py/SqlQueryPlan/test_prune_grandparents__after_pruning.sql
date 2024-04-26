-- src2
SELECT
  col0 AS col0
FROM (
  -- src1
  SELECT
    src1.col0
    , src1.col1
  FROM (
    -- src0
    SELECT
      src0.col0
      , src0.col1
    FROM demo.src0 src0
  ) src1
) src2
