-- Test Discrete Percentile Expression
SELECT
  PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY (a.col0)) AS col0_percentile
FROM foo.bar a
