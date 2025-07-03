WITH first_extract_gastos AS 

(SELECT 

year,
month,
service_country as o_service_country,
management_department,
management_account_name,
business_partner_name,
sum(value_usd_bdg) as value_usd_bdg






FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`


WHERE
year >= 2024
AND department_classification = 'S&M'
AND management_account_name NOT IN ('Non-Operational', 'Uncollectible Accounts', 'Payroll', 'Severance', 'Intercompany')

GROUP BY
year,
month,
management_department,
management_account_name,
business_partner_name,
service_country

ORDER BY
year,
month,
management_department,
management_account_name,
business_partner_name,
service_country
),

distribucion_hq AS (
  SELECT 'HQ' AS o_service_country,'CL' AS f_service_country, 0.3333 AS percentage
  UNION ALL
  SELECT 'HQ','MX', 0.3333
  UNION ALL
  SELECT 'HQ','ES', 0.3333
  UNION ALL
  SELECT 'CL','CL', 1
  UNION ALL
  SELECT 'MX','MX', 1
  UNION ALL
  SELECT 'ES','ES', 1
  UNION ALL
  SELECT 'AR','AR', 1
  UNION ALL
  SELECT 'BR','BR', 1
  UNION ALL
  SELECT 'EC','EC', 1
  UNION ALL
  SELECT 'CO','CO', 1
  UNION ALL
  SELECT 'PE','PE', 1
)


SELECT 

year,
month,
fg.o_service_country,
dhq.f_service_country,
management_department,
management_account_name,
business_partner_name,
sum(value_usd_bdg) AS original_value,
sum(value_usd_bdg)*dhq.percentage as value_usd_distr


FROM first_extract_gastos fg
LEFT JOIN distribucion_hq dhq
ON fg.o_service_country = dhq.o_service_country


WHERE
year >= 2025
AND fg.o_service_country = 'HQ'



GROUP BY
year,
month,
o_service_country,
f_service_country,
management_department,
management_account_name,
business_partner_name,
percentage

