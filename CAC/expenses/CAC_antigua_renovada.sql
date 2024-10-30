with expenses_info as (
SELECT 
    year,
    month,
    revenue_stream,	
    business_unit,	
    management_department,
    service_country,
    mgmt_account_classification,
    management_account_name,
    holding_segment,
    business_partner_name,
    source,
    sum(value_usd_bdg) as value_expense
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE department_classification = 'S&M'
AND mgmt_account_subclasification = 'SG&A'
AND revenue_stream not in ( 'Vouchers','Rewards')
AND business_unit != 'Financial Services'
AND subversion = 'REAL'
AND management_department != 'Success'
AND NOT (management_department = "RevOps" and management_account_name = 'Payroll')
group by 
year,
month,
revenue_stream,
business_unit,
management_department,
service_country,
mgmt_account_classification,
management_account_name,
holding_segment,
business_partner_name,
source
),
employee_benefits as (
  SELECT year,
  month,
  revenue_stream,	
  business_unit,	
	management_department,
  service_country,
  mgmt_account_classification,
  management_account_name,	
  holding_segment,
  business_partner_name,
  source, 
  value_expense,
  case
  when holding_segment = 'Sin holding' and service_country != 'HQ' then 'no_atribuible'
  when holding_segment in ('SMB', 'Enterprise') and service_country = 'HQ' then 'no_atribuible_pais_corp'
  when holding_segment = 'Sin holding' and service_country = 'HQ' then 'no_atribuible_pais_corp'
  else 'atribuible' end as categoria_
  From expenses_info
  where revenue_stream = 'Employee Benefit'
),
affinity as (
  SELECT year,
  month,
  revenue_stream,
  business_unit,
  management_department,
  service_country,
  mgmt_account_classification,
  management_account_name,
  holding_segment,
  business_partner_name,
  source,
  value_expense, 
  CASE
  WHEN  service_country = 'HQ' THEN 'no_atribuible_pais_corp' 
  ELSE 'atribuible' END AS categoria_
  FROM expenses_info
  WHERE revenue_stream = 'Affinity'
),
N_R AS (
  SELECT 
  YEAR,
  MONTH,
  revenue_stream,
  business_unit,
  management_department,
  service_country,
  mgmt_account_classification,
  management_account_name,	
  holding_segment,
  business_partner_name,
  source,
  value_expense, 
  CASE 
  WHEN holding_segment = 'Sin holding' and service_country != 'HQ' THEN 'no_atribuible'
  WHEN  service_country = 'HQ' THEN 'no_atribuible_pais_corp'
  ELSE 'atribuible' END AS categoria_
  FROM expenses_info 
  WHERE revenue_stream = 'N/R'
),
cac_distribucion_1 AS
( SELECT *
FROM employee_benefits
UNION ALL
SELECT *
FROM AFFINITY
UNION ALL
SELECT *
FROM N_R),
  gastos_por_distribuir as (
    SELECT tabla_cac.year,
    tabla_cac.month,
    parametros_cac.revenue_stream,
    tabla_cac.business_unit,
    tabla_cac.management_department,
    tabla_cac.service_country as o_service_country,
    parametros_cac.service_country as final_service_country,
    tabla_cac.mgmt_account_classification,
    tabla_cac.management_account_name,
    parametros_cac.holding_segment,
    tabla_cac.business_partner_name,
    tabla_cac.source,
    tabla_cac.value_expense*parametros_cac.parametro as value_expense,
    tabla_cac.categoria_

    FROM  cac_distribucion_1 tabla_cac
    INNER JOIN `btf-finance-sandbox.CAC.parametros_cac` parametros_cac
    ON tabla_cac.revenue_stream = parametros_cac.o_revenue_stream
    AND tabla_cac.holding_segment = parametros_cac.o_holding_segment
    AND tabla_cac.service_country = parametros_cac.o_service_country
    WHERE (tabla_cac.categoria_ = 'no_atribuible' or tabla_cac.categoria_ = 'no_atribuible_pais_corp')
    AND tabla_cac.value_expense != 0
),
gastos_sin_distribuir as (
    SELECT year,
    month,
    revenue_stream,	
    business_unit,	
	management_department,
    service_country as o_service_country,
    service_country  as final_service_country,
    mgmt_account_classification,
    management_account_name,	
    holding_segment,
    business_partner_name,
    source,
    value_expense,
    categoria_
    FROM cac_distribucion_1 tabla_cac
    WHERE tabla_cac.categoria_ = 'atribuible'
    AND tabla_cac.value_expense != 0
), cac_distribucion_2 AS (
    SELECT * FROM gastos_por_distribuir
    UNION ALL
    SELECT * from gastos_sin_distribuir
),
 cac_final_sinfiltro AS (SELECT 
CAC_distribuido.year,
CAC_distribuido.month,
CAC_distribuido.revenue_stream,
CAC_distribuido.management_department,
CAC_distribuido.o_service_country,
CAC_distribuido.final_service_country,
CAC_distribuido.management_account_name,
CAC_distribuido.holding_segment,
CAC_distribuido.source,
CASE
WHEN CAC_distribuido.categoria_ = 'atribuible' THEN 'Local'
WHEN CAC_distribuido.categoria_ = 'no_atribuible' THEN 'Distribuido local'
WHEN CAC_distribuido.categoria_ = 'no_atribuible_pais_corp' THEN 'Corp'
END AS tag_categoria_cac,
CAC_distribuido.value_expense,
CAST(CONCAT(year, '-', month, '-01') as DATE) as full_date,
CASE
WHEN UPPER(management_account_name) in ('PAYROLL', 'SEVERANCE') THEN 'payroll'
ELSE 'non-payroll'
END AS aux


FROM cac_distribucion_2 cac_distribuido),

tags_initiatives_cac AS (
  SELECT * FROM `btf-finance-sandbox.CAC.tags_cac`
),
cac_pre_tag AS (
SELECT 

year,
month,
revenue_stream,
management_department,
o_service_country,
final_service_country,
management_account_name,
holding_segment,
source,
tag_categoria_cac,
value_expense,
CASE
WHEN month >= 10 THEN CONCAT(year,'-',month,'-01')
WHEN month < 10 THEN CONCAT(year,'-0',month,'-01')
END AS full_date,
CONCAT(management_department,'-',aux,'-',tag_categoria_cac) as aux_2

FROM cac_final_sinfiltro)

SELECT 

year,
month,
revenue_stream,
management_department,
o_service_country,
final_service_country,
management_account_name,
holding_segment,
source,
tag_categoria_cac,
value_expense,
full_date,
aux_2 as aux,
tag_cac

FROM cac_pre_tag
LEFT JOIN tags_initiatives_cac
ON cac_pre_tag.aux_2 = tags_initiatives_cac.aux

ORDER BY year,month,revenue_stream, final_service_country, o_service_country, tag_cac