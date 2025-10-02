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
accounting_account_name != 'Vacaciones al Personal'
AND mgmt_account_subclasification = 'SG&A'
AND year >= 2024
AND department_classification = 'S&M'
AND management_department NOT IN ('Success', 'Insurance')
AND management_account_name NOT IN ('Non-Operational', 'Uncollectible Accounts', 'Severance', 'Intercompany', 'Benefits', 'Travels', 'Onboarding', 'Engagement', 
'Office Supplies',
'Retention',
'Revenue',
'Uncollectible accounts')
AND subversion = 'REAL' and version = 'REAL'
AND UPPER(management_account_name) NOT LIKE '%COGS%'
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
'Benefithub',
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
'Doc24',
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
'Google Cloud',
'Headspace - Minimum Revenue'
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


ORDER by year DESC, month DESC, original_value ASC


