-- Grouping by a grain that is NOT the same as the custom grain used in the offset window
--------------------------------------------------
-- Use the base grain of the custom grain's time spine in all initial subqueries, apply DATE_TRUNC in final query
-- This also works for custom grain, since we can just join it to the final subquery like usual.
-- Also works if there are multiple grains in the group by

with cte as (
-- CustomGranularityBoundsNode
    SELECT
        date_day,
        fiscal_quarter,
        row_number() over (partition by fiscal_quarter order by date_day) - 1 as days_from_start_of_fiscal_quarter
        , first_value(date_day) over (partition by fiscal_quarter order by date_day) as fiscal_quarter_start_date
        , last_value(date_day) over (partition by fiscal_quarter order by date_day) as fiscal_quarter_end_date
    FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
)

SELECT
    metric_time__week,
    metric_time__fiscal_year,
    SUM(total_price) AS revenue_last_fiscal_quarter
FROM ANALYTICS_DEV.DBT_JSTEIN.STG_SALESFORCE__ORDER_ITEMS
INNER JOIN (
    -- ApplyStandardGranularityNode
    SELECT
        ts_offset.date_day,
        DATE_TRUNC(week, ts_offset.date_day) AS metric_time__week,
        fiscal_year AS metric_time__fiscal_year
    FROM (
    -- OffsetByCustomGranularityNode
        select
            fiscal_quarter
            , case
                when dateadd(day, days_from_start_of_fiscal_quarter, fiscal_quarter_start_date__offset_by_1) <= fiscal_quarter_end_date__offset_by_1
                    then dateadd(day, days_from_start_of_fiscal_quarter, fiscal_quarter_start_date__offset_by_1)
                else fiscal_quarter_end_date__offset_by_1
                end as date_day
        from cte -- CustomGranularityBoundsNode
        inner join (
        -- OffsetCustomGranularityBoundsNode
            select
                fiscal_quarter,
                lag(fiscal_quarter_start_date, 1) over (order by fiscal_quarter) as fiscal_quarter_start_date__offset_by_1,
                lag(fiscal_quarter_end_date, 1) over (order by fiscal_quarter) as fiscal_quarter_end_date__offset_by_1
            from (
            -- FilterEelementsNode
                select
                    fiscal_quarter,
                    fiscal_quarter_start_date,
                    fiscal_quarter_end_date
                from cte -- CustomGranularityBoundsNode
                GROUP BY 1, 2, 3
            ) ts_distinct
        ) ts_with_offset_intervals USING (fiscal_quarter)
    ) ts_offset
     -- JoinToCustomGranularityNode
    LEFT JOIN ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS custom ON custom.date_day = ts_offset.date_day
) ts_offset_dates ON ts_offset_dates.date_day = DATE_TRUNC(day, created_at)::date -- always join on base time spine column
GROUP BY 1, 2
ORDER BY 1, 2;






-- Grouping by the just same custom grain as what's used in the offset window (and only that grain)
--------------------------------------------------
-- Could follow the same SQL as above, but this would be a more optimized version (they appear to give the same results)
-- This is likely to be most common for period over period, so it might be good to optimize it


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
            , lag(fiscal_quarter, 1) OVER (ORDER BY fiscal_quarter) as fiscal_quarter_offset
        FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
        GROUP BY 1
    ) ts_offset_dates USING (fiscal_quarter)
) ts ON date_day = DATE_TRUNC(day, created_at)::date
GROUP BY 1
ORDER BY 1;
