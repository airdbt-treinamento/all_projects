
with date_dimension as
    (
        select * from {{ ref('stg_dates') }}
    ),

fiscal_periods as
    (
        {{ dbt_date.get_fiscal_periods(ref('stg_dates'), year_end_month=1, week_start_day=1, shift_year=1) }}
    ),
traformation as
    (
        select
            {{ dbt_utils.generate_surrogate_key(['d.date_day']) }} as date_key,
            d.*,
            f.fiscal_week_of_year,
            f.fiscal_week_of_period,
            f.fiscal_period_number,
            f.fiscal_quarter_number,
            f.fiscal_period_of_quarter
        from
            date_dimension d
            left join fiscal_periods f on d.date_day = f.date_day
    )

select * from traformation
