-- =============================================
-- TABLA CONSOLIDADA P&L
-- Revenue (EoP) + COGS + SG&A
-- =============================================

-- BLOQUE 1: Revenue EoP desde Buildups
SELECT
    CAST(full_date AS DATE)        AS period,
    service_country                AS country,
    client_segment                 AS segment,
    revenue_stream,
    'Revenue_EoP'                  AS pnl_line,
    CAST(NULL AS STRING)           AS department,
    SUM(value_lc_eop)              AS amount_local,
    SUM(value_usd_eop)             AS amount_usd
FROM `btf-finance-sandbox.Revenue.Buildups_EB_CB_Spain`
WHERE year >= 2024
GROUP BY 1, 2, 3, 4

UNION ALL

-- BLOQUE 2: COGS desde Cubo_Financiero
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

-- BLOQUE 3: SG&A desde Cubo_Financiero
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