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
mgmt_account_subclasification = 'SG&A'
AND year >= 2024
AND department_classification = 'S&M' -- Incluímos Insurance ??
AND management_department != 'Success'
AND management_account_name NOT IN ('Non-Operational', 'Uncollectible Accounts', 'Payroll', 'Severance', 'Intercompany', 'Benefits', 'Professional Services')
-- Agregamos valor de professional services a mano. 350$ por país
AND business_partner_name NOT IN (
'Chubb',
'Vida Security',
'Bono Asistencia',
'Gerty',
'Metlife',
'Icatu',
'Coomeva',
'Interseguro',
'Que Plan',
'Mawdy',
'Busuu',
'Hanufit',
'Celmedia',		
'Teledoc',
'Legal Chile',
'MuvPass',
'Everhealth',
'Meeting Lawyers',
'Griky',
'Instafit',
'Pura Mente',
'Headspace',
'BenefitHub',
'Rufy',
'Zen'	,
'Jardines del Valle',
'Total Pass',
'Clube Ben'	,
'Wited',
'Betterfly',
'Conexa',
'BigBox',
'Doc 24',		
'Udemy',
'Monkey Fit',
'Fiton',
'Gift Point',
'Medismart',
'Almamedis',
'Market Care',
'Pharma Benefits',
'Bettercoins',
'Amazon',
'Microsoft',
'Google Cloud'
)

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
),

professional_services as (
  SELECT 'CL' as o_service_country, 'CL' as f_service_country, 'Marketing' as management_department, 'Professional Services' as management_account_name, 'Diseñadores' as business_partner_name, 350 as original_value, 350 as value_usd_distr
  UNION ALL
  SELECT 'MX' as o_service_country, 'MX' as f_service_country, 'Marketing' as management_department, 'Professional Services' as management_account_name, 'Diseñadores' as business_partner_name, 350 as original_value, 350 as value_usd_distr
  UNION ALL
  SELECT 'ES' as o_service_country, 'ES' as f_service_country, 'Marketing' as management_department, 'Professional Services' as management_account_name, 'Diseñadores' as business_partner_name, 350 as original_value, 350 as value_usd_distr
),

meses_hasta_ahora as (
  SELECT DISTINCT
  year, month, 'Professional Services' as ps
  FROM first_extract_gastos
),

join_meses_ps as (
  SELECT year, month, o_service_country, f_service_country, management_department, management_Account_name, business_partner_name, original_value, value_usd_distr
  FROM meses_hasta_ahora
  LEFT JOIN
  professional_services
  on meses_hasta_ahora.ps = professional_services.management_account_name
  --WHERE year >= 2025
),

gastos_distribuidos as (
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
--year >= 2025
--AND 
(management_account_name = 'Digital Tools' AND business_partner_name in (
  'EMAIL HIPPO',
'TREBBLE',
'AIRCALL',
'STRIPO',
'GOLDCAST',
'HOOTSUITE',
'APOLLO',
'LINKEDIN IRELAND UNLIMITED COMPANY'
) OR management_account_name != 'Digital Tools')
--AND fg.o_service_country = 'HQ'. filtro para QA distribución

GROUP BY
year,
month,
o_service_country,
f_service_country,
management_department,
management_account_name,
business_partner_name,
percentage)

SELECT * FROM gastos_distribuidos
UNION ALL
SELECT * from join_meses_ps


ORDER by year DESC, month DESC, original_value ASC


