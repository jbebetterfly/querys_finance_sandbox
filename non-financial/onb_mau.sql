WITH calendar as (
        select
            DATE_TRUNC(cal.full_date, MONTH) as date,
            min (cal.full_date)
        from
            `btf-unified-data-platform.pdr_calendar.calendar` as cal
        where
            cal.full_date <= current_date
            and cal.full_date >= "2024-01-01"
        group by
            DATE_TRUNC(cal.full_date, MONTH)
    ),
    metrics_value as (
      SELECT
        DATE_TRUNC(date_start, MONTH) as date,
        country_code,
        company_size,
        sum(count_members_onboarding) as onboarded_members,
        sum(count_members_total) as total_members,
        sum(mau) as monthly_active_users

      FROM
        `btf-unified-data-platform.ir_metrics.clients_members_and_mau`

      GROUP BY
      date,
      country_code,
      company_size
    )

    SELECT
      cal.date,
      mv.country_code,
      mv.company_size,
      mv.onboarded_members,
      mv.total_members,
      mv.monthly_active_users

    FROM calendar cal
    INNER JOIN metrics_value mv
    on cal.date = mv.date
    
    WHERE (company_size IS NOT NULL OR country_code IS NOT NULL)

    ORDER by date asc