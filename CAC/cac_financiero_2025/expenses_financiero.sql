WITH first_extract_gastos AS 

(SELECT 

year,
month,
service_country as o_service_country,
management_department,
management_account_name,
business_partner_name,
CASE
  WHEN business_partner_name = 'HUBSPOT' THEN sum(value_usd_bdg)*0.7
  ELSE sum(value_usd_bdg)
  END AS value_usd_bdg

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
'Uncollectible accounts',
'Payroll') -- Exclusión payroll por inclusión CTC *de agosto en adelante*
AND subversion = 'REAL' and version = 'REAL'
AND UPPER(source) NOT LIKE '%CONECTEN%'
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

payroll_non_ctc_pre_agosto as (
SELECT
year,
month,
service_country as o_service_country,
management_department,
management_account_name,
business_partner_name,
sum(value_usd_bdg)

from `btf-finance-sandbox.Expenses.Cubo_Financiero`

WHERE

management_account_name = 'Payroll'
AND accounting_account_name != 'Vacaciones al Personal'
AND mgmt_account_subclasification = 'SG&A'
AND year >= 2024
AND department_classification = 'S&M'
AND management_department NOT IN ('Success', 'Insurance', 'RevOps')
AND full_date < '2025-08-01'
AND subversion = 'REAL' and version = 'REAL'
AND UPPER(source) NOT LIKE '%CONECTEN%'
AND UPPER(management_account_name) NOT LIKE '%COGS%'

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

payroll_ctc as (
SELECT * FROM
`btf-finance-sandbox.cac_v3.ctc_cac_2025`


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

gastos_pre_distr as (
  SELECT * from first_extract_gastos
  UNION ALL
  SELECT * FROM payroll_non_ctc_pre_agosto
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


FROM gastos_pre_distr fg
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
percentage),

gastos_distribuidos_ctc_payroll_append as (

  SELECT * FROM gastos_distribuidos
  UNION ALL
  SELECT * FROM payroll_ctc
)

SELECT *,
CASE
WHEN management_account_name IN ('Payroll','Others', 'Memberships') then 'cac_financiero'
WHEN business_partner_name IN ('EMAIL HIPPO',
'TREBBLE',
'TREBLE',
'TREBLE.AI'
'AIRCALL',
'STRIPO',
'GOLDCAST',
'HOOTSUITE',
'APOLLO',
'LINKEDIN IRELAND UNLIMITED COMPANY',
'LINKEDIN',
'WINCLAP') AND management_account_name = 'Digital Tools' then 'gestion'
WHEN business_partner_name in (
  'WINCLAP') 
then 'gestion'
WHEN management_account_name IN ('Events', 'Merchandising', 'Free Month', 'Paid Media') THEN 'gestion'
ELSE 'cac_financiero'
END as separacion_cac
 FROM gastos_distribuidos_ctc_payroll_append


ORDER by year DESC, month DESC, original_value ASC