-- Grouping by a grain that is NOT the same AS the custom grain used in the offset window
--------------------------------------------------
-- Use the base grain of the custom grain's time spine in all initial subqueries, apply DATE_TRUNC in final query
-- This also works for custom grain, since we can just join it to the final subquery like usual.
-- Also works if there are multiple grains in the group by

WITH cte AS ( -- CustomGranularityBoundsNode
    SELECT
        fiscal_quarter
        , first_value(date_day) OVER (PARTITION BY fiscal_quarter ORDER BY date_day) AS ds__fiscal_quarter__first_value
        , last_value(date_day) OVER (PARTITION BY fiscal_quarter ORDER BY date_day) AS ds__fiscal_quarter__last_value
        , row_number() OVER (PARTITION BY fiscal_quarter ORDER BY date_day) AS ds__day__row_number
    FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
)

SELECT
    metric_time__week,
    fiscal_year AS metric_time__fiscal_year,
    SUM(total_price) AS revenue_last_fiscal_quarter
FROM ANALYTICS_DEV.DBT_JSTEIN.STG_SALESFORCE__ORDER_ITEMS
INNER JOIN (
    -- OffsetByCustomGranularityNode
    SELECT
        offset_by_custom_grain.date_day,
        DATE_TRUNC(week, offset_by_custom_grain.date_day) AS metric_time__week,
    FROM (
        SELECT
            CASE
                WHEN dateadd(day, ds__day__row_number - 1, ds__fiscal_quarter__first_value__offset) <= ds__fiscal_quarter__last_value__offset
                    THEN dateadd(day, ds__day__row_number - 1, ds__fiscal_quarter__first_value__offset)
                ELSE ds__fiscal_quarter__last_value__offset
                END AS date_day
        FROM cte
        INNER JOIN (
            SELECT
                fiscal_quarter,
                lag(ds__fiscal_quarter__first_value, 1) OVER (ORDER BY fiscal_quarter) AS ds__fiscal_quarter__first_value__offset,
                lag(ds__fiscal_quarter__last_value, 1) OVER (ORDER BY fiscal_quarter) AS ds__fiscal_quarter__last_value__offset
            FROM (
                SELECT -- FilterElementsNode
                    fiscal_quarter,
                    ds__fiscal_quarter__first_value,
                    ds__fiscal_quarter__last_value
                FROM cte
                GROUP BY 1, 2, 3
            ) ts_distinct
        ) ts_with_offset_intervals USING (fiscal_quarter)
    ) AS offset_by_custom_grain
) ts_offset_dates ON ts_offset_dates.date_day = DATE_TRUNC(day, created_at)::date
LEFT JOIN ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS custom ON custom.date_day = ts_offset_dates.date_day -- JoinToCustomGranularityNode (only if needed)
GROUP BY 1, 2
ORDER BY 1, 2;






-- Grouping by the just same custom grain AS what's used in the offset window (and only that grain)
--------------------------------------------------
-- Could follow the same SQL AS above, but this would be a more optimized version (they appear to give the same results)
-- This is likely to be most common for period OVER period, so it might be good to optimize it


SELECT -- existing nodes!
    metric_time__fiscal_quarter,
    SUM(total_price) AS revenue
FROM ANALYTICS_DEV.DBT_JSTEIN.STG_SALESFORCE__ORDER_ITEMS
LEFT JOIN (  -- JoinToTimeSpineNode, no offset, join on custom grain spec
    SELECT
        -- JoinToTimeSpineNode
        -- TransformTimeDimensionsNode??
        date_day,
        fiscal_quarter_offset AS metric_time__fiscal_quarter
    FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
    INNER JOIN (
        -- OffsetCustomGranularityNode
        SELECT
            fiscal_quarter
            , lag(fiscal_quarter, 1) OVER (ORDER BY fiscal_quarter) AS fiscal_quarter_offset
        FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
        GROUP BY 1
    ) ts_offset_dates USING (fiscal_quarter)
) ts ON date_day = DATE_TRUNC(day, created_at)::date
GROUP BY 1
ORDER BY 1;
