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
        client_country_code,
        client_size,
        sum(onboarding_users) as onboarded_members,
        sum(total_users) as total_members,
        sum(total_user_mau) as monthly_active_users

      FROM
        `btf-unified-data-platform.ir_metrics.general_metrics_by_client`

      GROUP BY
      date,
      client_country_code,
      client_size
    )

    SELECT
      cal.date,
      mv.client_country_code,
      mv.client_size,
      mv.monthly_active_users,
      mv.onboarded_members,
      mv.total_members,
      

    FROM calendar cal
    INNER JOIN metrics_value mv
    on cal.date = mv.date
    
    WHERE (client_size IS NOT NULL OR client_country_code IS NOT NULL)

    ORDER by date asc