with expenses_info as (
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
    sum(value_usd_bdg) as value_expense
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE department_classification = 'S&M'
AND mgmt_account_subclasification = 'SG&A'
AND revenue_stream not in ( 'Vouchers','Rewards', 'Affinity')
AND holding_segment != 'Affinity'
AND subversion = 'REAL'
AND management_department NOT IN ('Success', 'Product Marketing')
AND NOT (management_department = "RevOps" and management_account_name = 'Payroll')
AND UPPER(management_account_name) NOT IN ('ENGAGEMENT', 'ONBOARDING', 'RETENTION', 'UNCOLLECTIBLE ACCOUNTS')
AND (year >= 2024 OR month >= 11) ----- REVISAR
group by 
year,
month,
revenue_stream,
management_department,
service_country,
management_account_name,
holding_segment,
business_partner_name,
source
),
expenses_info_tagged as (
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
    WHEN upper(management_account_name)
    in ('PAYROLL', 'DIGITAL TOOLS', 'BENEFITS', 'TRAVELS', 'SEVERANCE', 'UNCOLLECTIBLE ACCOUNTS', 'MEMBERSHIPS', 'TRAINING', 'PROFESSIONAL SERVICES') then 'Fijo'
    ELSE 'Variable'
    END AS cac_expense_tag
    FROM expenses_info
),
distribuir_check as (
  SELECT year,
  month,
  revenue_stream,
	management_department,
  service_country,
  management_account_name,	
  holding_segment,
  business_partner_name,
  source,
  cac_expense_tag,
  value_expense,
  CASE
  WHEN UPPER(holding_segment) = 'SIN HOLDING' THEN 'distribuible'
  WHEN UPPER(holding_segment) in ('ENTERPRISE', 'SME') then 'no_distribuible'
  ELSE NULL END AS distribuir_check_tag
  FROM expenses_info_tagged
),
  gastos_por_distribuir AS (
    SELECT 
    tabla_cac.year AS year, -- 1
    tabla_cac.month AS month, -- 2
    tabla_cac.revenue_stream AS revenue_stream, -- 3
    tabla_cac.management_department AS management_department, -- 4
    tabla_cac.service_country, -- 5
    tabla_cac.management_account_name, -- 6
    tabla_cac.holding_segment AS o_holding_segment,
    parametros_cac.holding_segment as holding_segment, -- 7
    tabla_cac.business_partner_name, -- 8
    tabla_cac.source, -- 9
    tabla_cac.cac_expense_tag, -- 10
    tabla_cac.value_expense AS o_value_expense,
    tabla_cac.value_expense*parametros_cac.parametro as value_expense, -- 11
    tabla_cac.distribuir_check_tag -- 12

    FROM  distribuir_check tabla_cac
    LEFT JOIN `btf-finance-sandbox.cac_v3.parametros_cac_v3` parametros_cac
    ON tabla_cac.holding_segment = parametros_cac.o_holding_segment
    AND tabla_cac.cac_expense_tag = parametros_cac.cac_tag_v3
    AND tabla_cac.year = parametros_cac.year
    AND tabla_cac.month = parametros_cac.month
    WHERE tabla_cac.distribuir_check_tag = 'distribuible'
    AND tabla_cac.value_expense != 0
),
gastos_sin_distribuir as (
    SELECT 
    tabla_cac.year AS year, -- 1
    tabla_cac.month, -- 2
    tabla_cac.revenue_stream,	-- 3
	  tabla_cac.management_department, -- 4
    tabla_cac.service_country AS service_country, -- 5
    tabla_cac.management_account_name AS management_account_name, -- 6
    tabla_cac.holding_segment AS o_holding_segment,
    tabla_cac.holding_segment, -- 7
    tabla_cac.business_partner_name, -- 8
    tabla_cac.source, -- 9
    tabla_cac.cac_expense_tag, -- 10
    tabla_cac.value_expense AS o_value_expense,
    tabla_cac.value_expense, -- 11
    tabla_cac.distribuir_check_tag -- 12
    FROM distribuir_check tabla_cac
    WHERE tabla_cac.distribuir_check_tag = 'no_distribuible'
    AND tabla_cac.value_expense != 0
), cac_distribucion_2 AS (
    SELECT * FROM gastos_por_distribuir
    UNION ALL
    SELECT * from gastos_sin_distribuir
),
 cac_final_sinfiltro AS 
 (SELECT 
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
CAC_distribuido.cac_expense_tag,
CAST(CONCAT(year, '-', month, '-01') as DATE) as full_date,
CASE
WHEN CAC_distribuido.management_department = 'Marketing' THEN 'Generation'
WHEN CAC_distribuido.management_department IN ('Sales', 'RevOps') THEN 'Conversion' ------- REVISAR
END AS cac_tag

FROM cac_distribucion_2 CAC_distribuido)

SELECT * FROM cac_final_sinfiltro ------ Elegir columnas a mano con tal de hacer una tabla más limpia