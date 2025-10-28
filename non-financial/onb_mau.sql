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
        business_type,
        CASE
        WHEN client_size in ('corporate','enterprise') THEN 'enterprise'
        WHEN client_size in ('micro','sme') THEN 'sme'
        ELSE 'check' END
        AS client_size,
        sum(total_user_mau) as monthly_active_users,
        sum(onboarding_users) as onboarded_members,
        sum(total_users) as total_members
        

      FROM
        `btf-unified-data-platform.ir_metrics.general_metrics_by_client`

      WHERE NOT REGEXP_CONTAINS(LOWER(COALESCE(client_name, '')), r'(demo|prueba|trial|test)')

      GROUP BY
      date,
      client_country_code,
      business_type,
      client_size
    )

    SELECT
      cal.date,
      mv.client_country_code,
      business_type,
      mv.client_size,
      mv.onboarded_members,
      mv.total_members,
      mv.monthly_active_users

    FROM calendar cal
    INNER JOIN metrics_value mv
    on cal.date = mv.date
    
    WHERE (client_size IS NOT NULL AND client_country_code IS NOT NULL AND client_size IS NOT NULL)
    AND client_country_code in ('CL', 'MX', 'ES')
    

    ORDER by date asc, client_country_code, client_size