---- Calories and minutes query for MoM
--- Developer: jbezanilla
--- First Metric: accum calories and training minutes by month
WITH
    unique_records_sports as (
        select
            distinct data_person_id,
            val_minutes,
            calories,
            CAST(date_trunc(date_start, month) AS DATE) as month_act
        from
            `btf-unified-data-platform.pdr_app.betteractivities`
        where
            CAST(date_start AS date) < current_date
    ),
    calendar as (
        select
            DATE_TRUNC(cal.full_date, MONTH) as month,
            min (cal.full_date)
        from
            `btf-unified-data-platform.pdr_calendar.calendar` as cal
        where
            cal.full_date <= current_date
            and cal.full_date >= "2022-01-01"
        group by
            DATE_TRUNC(cal.full_date, MONTH)
    ),
    calories as (
        select
            cal.month,
            'total_calories_burned' as kpi,
            'calories' as unit,
            sum(urs.calories) as val_total
        from
            calendar as cal
            inner join unique_records_sports urs on urs.month_act <= cal.month
        group by
            1
    ),
    training_minutes as (
        select
            cal.month,
            'total_training_minutes' as kpi,
            'minutes' as unit,
            sum(urs.val_minutes) as val_total
        from
            calendar as cal
            inner join unique_records_sports urs on urs.month_act <= cal.month
        group by
            1
    )
select
    month,
    kpi,
    unit,
    val_total
from
    calories
union all
select
    month,
    kpi,
    unit,
    val_total
from
    training_minutes
order by
    1 desc;