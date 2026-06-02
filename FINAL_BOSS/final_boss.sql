-- =============================================
-- TABLA CONSOLIDADA P&L v3
-- Fuente revenue: buildups_v2 (con product)
-- =============================================

WITH buildups AS (
    SELECT
        CAST(full_date AS DATE)   AS period,
        service_country           AS country,
        client_segment            AS segment,
        CASE
            WHEN revenue_stream = 'CB' THEN 'CB'
            WHEN revenue_stream = 'EB' AND product = 'EB-XX-1'                  THEN 'Wellbeing'
            WHEN revenue_stream = 'EB' AND product IN ('S.C', 'S.C cargas')     THEN 'Health'
            WHEN revenue_stream = 'EB' AND product = 'IN-XX-1'                  THEN 'Insurance-ES'
            WHEN revenue_stream = 'EB' AND product = 'VO-XX-1'                  THEN 'Interchange-ES'
            ELSE revenue_stream
        END                       AS revenue_stream,
        SUM(value_lc_eop)         AS rev_eop_lc,
        SUM(value_usd_eop)        AS rev_eop_usd,
        SUM(value_lc_new)         AS rev_new_lc,
        SUM(value_usd_new)        AS rev_new_usd,
        SUM(value_lc_churn)       AS rev_churn_lc,
        SUM(value_usd_churn)      AS rev_churn_usd,
        SUM(value_lc_retention)   AS rev_ret_lc,
        SUM(value_usd_retention)  AS rev_ret_usd,
        SUM(logo_eop)             AS log_eop,
        SUM(logo_new)             AS log_new,
        SUM(logo_churn)           AS log_churn,
        SUM(members_eop)          AS mem_eop,
        SUM(members_new)          AS mem_new,
        SUM(members_churn)        AS mem_churn,
        SUM(members_retention)    AS mem_ret
    FROM `btf-finance-sandbox.Revenue.buildups_v2`
    WHERE year >= 2024
      AND NOT (revenue_stream = 'EB' AND product IN ('FO-XX-1', 'FALSE'))
    GROUP BY 1, 2, 3, 4
)

-- Revenue (4 métricas con LC y USD)
SELECT period, country, segment, revenue_stream, 'Revenue_EoP' AS pnl_line, CAST(NULL AS STRING) AS department, rev_eop_lc AS amount_local, rev_eop_usd AS amount_usd FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Revenue_New', NULL, rev_new_lc, rev_new_usd FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Revenue_Churn', NULL, rev_churn_lc, rev_churn_usd FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Revenue_Retention', NULL, rev_ret_lc, rev_ret_usd FROM buildups

UNION ALL

-- Logos (3 métricas, solo conteo, USD = NULL)
SELECT period, country, segment, revenue_stream, 'Logos_EoP', NULL, CAST(log_eop AS FLOAT64), NULL FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Logos_New', NULL, CAST(log_new AS FLOAT64), NULL FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Logos_Churn', NULL, CAST(log_churn AS FLOAT64), NULL FROM buildups

UNION ALL

-- Members (4 métricas, solo conteo, USD = NULL)
SELECT period, country, segment, revenue_stream, 'Members_EoP', NULL, CAST(mem_eop AS FLOAT64), NULL FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Members_New', NULL, CAST(mem_new AS FLOAT64), NULL FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Members_Churn', NULL, CAST(mem_churn AS FLOAT64), NULL FROM buildups
UNION ALL
SELECT period, country, segment, revenue_stream, 'Members_Retention', NULL, CAST(mem_ret AS FLOAT64), NULL FROM buildups

UNION ALL

-- COGS desde Cubo_Financiero
SELECT
    DATE(year, month, 1)            AS period,
    service_country                 AS country,
    CASE holding_segment
        WHEN 'SMB' THEN 'SME'
        ELSE holding_segment
    END                             AS segment,
    CASE revenue_stream
        WHEN 'Employee Benefit'  THEN 'EB'
        WHEN 'Employee Benefits' THEN 'EB'
        WHEN 'Affinity'          THEN 'CB'
        ELSE revenue_stream
    END                             AS revenue_stream,
    'COGS'                          AS pnl_line,
    management_department           AS department,
    SUM(value_lc)                   AS amount_local,
    SUM(value_usd_bdg)              AS amount_usd
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE mgmt_account_subclasification = 'COGS'
    AND management_department = 'Sales'
    AND holding_segment IN ('SME', 'Enterprise')
    AND revenue_stream IN ('Employee Benefit', 'Affinity')
    AND subversion = 'REAL'
    AND year >= 2024
    AND management_account_name != 'COGS: Rebate'
    AND service_country != 'HQ'
GROUP BY 1, 2, 3, 4, 6

UNION ALL

-- SG&A desde Cubo_Financiero
SELECT
    DATE(year, month, 1)            AS period,
    service_country                 AS country,
    CASE holding_segment
        WHEN 'SMB' THEN 'SME'
        ELSE holding_segment
    END                             AS segment,
    CASE revenue_stream
        WHEN 'Employee Benefit'  THEN 'EB'
        WHEN 'Employee Benefits' THEN 'EB'
        WHEN 'Affinity'          THEN 'CB'
        ELSE revenue_stream
    END                             AS revenue_stream,
    'SGA'                           AS pnl_line,
    management_department           AS department,
    SUM(value_lc)                   AS amount_local,
    SUM(value_usd_bdg)              AS amount_usd
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE mgmt_account_subclasification = 'SG&A'
    AND mgmt_account_classification = 'P&L'
    AND management_account_name NOT IN (
        'Non-Operational', 'Depreciation',
        'Severance', 'Uncollectible Accounts'
    )
    AND accounting_account_id NOT IN ('Vacaciones al Personal')
    AND subversion = 'REAL'
    AND year >= 2024
GROUP BY 1, 2, 3, 4, 6