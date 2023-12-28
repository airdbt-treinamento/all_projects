with v_dates as
    (
        {{ dbt_date.get_date_dimension('2010-01-01', '2027-12-31') }}
    )

select * from v_dates order by 1
