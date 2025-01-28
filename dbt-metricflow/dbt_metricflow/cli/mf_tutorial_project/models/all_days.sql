{{
    config(
        materialized = 'table',
    )
}}

with days as (
    -- Only generate dates for 2022 since that's what's in the seed data.
    -- Note that `date_spine` does not include the end date.
    {{
        dbt.date_spine(
            'day',
            "make_date(2022, 1, 1)",
            "make_date(2023, 1, 1)"
        )
    }}

),

final as (
    select cast(date_day as date) as date_day
    from days
)


select * from final
where date_day >= DATE '2022-01-01'
and date_day  < DATE '2023-01-01'
