--- FINAL
-- Table A
WITH expenses_info AS (
    SELECT 
        year,
        month,
        revenue_stream,
        management_department,
        service_country,
        management_account_name,
        holding_segment,
        business_partner_name,
        source,
        SUM(value_usd_bdg) AS value_expense
    FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
    WHERE department_classification = 'S&M'
      AND mgmt_account_subclasification = 'SG&A'
      AND revenue_stream NOT IN ('Vouchers', 'Rewards', 'Affinity')
      AND holding_segment != 'Affinity'
      AND subversion = 'REAL'
      AND management_department NOT IN ('Success', 'Product Marketing')
      AND NOT (management_department = "RevOps" AND management_account_name in ('Payroll', 'Benefits', 'Severance'))
      AND UPPER(management_account_name) NOT IN ('ENGAGEMENT', 'ONBOARDING', 'RETENTION', 'UNCOLLECTIBLE ACCOUNTS')
      AND (year >= 2024 OR month >= 11)
    GROUP BY 
        year, month, revenue_stream, management_department, 
        service_country, management_account_name, 
        holding_segment, business_partner_name, source
),
expenses_info_tagged AS (
    SELECT 
        year,
        month,
        revenue_stream,
        management_department,
        service_country,
        management_account_name,
        holding_segment,
        business_partner_name,
        source,
        value_expense
    FROM expenses_info
),
distribuir_check AS (
    SELECT 
        year,
        month,
        revenue_stream,
        management_department,
        service_country,
        management_account_name,
        holding_segment,
        business_partner_name,
        source,
        value_expense,
        CASE
            WHEN UPPER(holding_segment) = 'SIN HOLDING' THEN 'distribuible'
            WHEN UPPER(holding_segment) IN ('ENTERPRISE', 'SME') THEN 'no_distribuible'
            ELSE NULL 
        END AS distribuir_check_tag
    FROM expenses_info_tagged
),
gastos_por_distribuir AS (
    SELECT 
        tabla_cac.year AS year,
        tabla_cac.month AS month,
        tabla_cac.revenue_stream AS revenue_stream,
        tabla_cac.management_department AS management_department,
        tabla_cac.service_country,
        tabla_cac.management_account_name,
        tabla_cac.holding_segment AS o_holding_segment,
        parametros_cac.holding_segment AS holding_segment,
        tabla_cac.business_partner_name,
        tabla_cac.source,
        tabla_cac.value_expense AS o_value_expense,
        tabla_cac.value_expense * parametros_cac.parametro AS value_expense,
        tabla_cac.distribuir_check_tag
    FROM distribuir_check tabla_cac
    LEFT JOIN `btf-finance-sandbox.cac_v3.parametros_cac_v3` parametros_cac
        ON tabla_cac.holding_segment = parametros_cac.o_holding_segment
        AND tabla_cac.year = parametros_cac.year
        AND tabla_cac.month = parametros_cac.month
    WHERE tabla_cac.distribuir_check_tag = 'distribuible'
      AND tabla_cac.value_expense != 0
),
gastos_sin_distribuir AS (
    SELECT 
        year,
        month,
        revenue_stream,
        management_department,
        service_country,
        management_account_name,
        holding_segment AS o_holding_segment,
        holding_segment,
        business_partner_name,
        source,
        value_expense AS o_value_expense,
        value_expense,
        distribuir_check_tag
    FROM distribuir_check
    WHERE distribuir_check_tag = 'no_distribuible'
      AND value_expense != 0
), 
cac_distribucion_2 AS (
    SELECT * FROM gastos_por_distribuir
    UNION ALL
    SELECT * FROM gastos_sin_distribuir
),
-- CTE para incluir el campo `sub_type`
cac_final_sinfiltro AS (
    SELECT 
        CAC_distribuido.year,
        CAC_distribuido.month,
        CAC_distribuido.revenue_stream,
        CAC_distribuido.management_department,
        CAC_distribuido.service_country,
        CAC_distribuido.management_account_name,
        CAC_distribuido.o_holding_segment,
        CAC_distribuido.holding_segment,
        CAC_distribuido.source,
        CAC_distribuido.distribuir_check_tag,
        CAC_distribuido.o_value_expense,
        CAC_distribuido.value_expense,
        CAST(CONCAT(year, '-', month, '-01') AS DATE) AS full_date,
        -- Definición de `sub_type` según las condiciones
        CASE
            WHEN management_account_name IN ('Paid Media', 'Merchandising', 'Others', 'Memberships', 'Referral Incentives') THEN 'demand'
            WHEN management_account_name IN ('Event','Events','Others') AND management_department IN ('Marketing') THEN 'demand'
            WHEN management_account_name IN ('Incentives', 'Free Month', 'Free trial', 'Free Trial', 'Merchandising', 'Others') THEN 'conversion'
            WHEN management_account_name IN ('Event','Events', 'Others') AND management_department = 'Sales' THEN 'conversion'
            WHEN management_account_name = 'Travels' AND management_department = 'Sales' THEN 'conversion'
            WHEN management_account_name = 'Payroll' AND management_department IN ('Marketing', 'Sales') THEN 'payroll'
            WHEN management_account_name IN ('Severance','Benefits') THEN 'payroll'
            WHEN management_account_name IN ('Digital Tools', 'Memberships', 'Training','Professional Services') THEN 'non_payroll'
            WHEN management_account_name = 'Travels' AND management_department IN ('RevOps','Marketing') THEN 'non_payroll'
            WHEN management_account_name IN ('Event','Events') AND management_department IN ('RevOps') THEN 'non_payroll'
            ELSE NULL 
        END AS sub_type
    FROM cac_distribucion_2 CAC_distribuido
),
-- CTE para agregar el campo `type` basado en `sub_type`
cac_final_con_tipo AS (
    SELECT 
        *,
        CASE
            WHEN sub_type IN ('demand', 'conversion') THEN 'initiatives'
            ELSE 'other'
        END AS type
    FROM cac_final_sinfiltro
)
-- Selección final
SELECT 
    year,
    month,
    revenue_stream,
    management_department,
    service_country,
    management_account_name,
    o_holding_segment,
    holding_segment,
    source,
    distribuir_check_tag,
    o_value_expense,
    value_expense,
    full_date,
    sub_type,
    type
FROM cac_final_con_tipo;