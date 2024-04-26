-- Test Approximate Continuous Percentile Expression
SELECT
  APPROX_QUANTILES(a.col0, 2)[OFFSET(1)] AS col0_percentile
FROM foo.bar a
