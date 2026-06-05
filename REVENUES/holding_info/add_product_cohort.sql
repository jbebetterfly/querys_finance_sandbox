WITH acq as (



SELECT DISTINCT
betterfly_country,
company_id,
company_name,
product_modules,
activation_date,
development_type_use


FROM  `btf-unified-data-platform.pdr_acquisition.deals`


WHERE activation_date >= '2026-03-01' ----Editar para acomodar fecha
AND betterfly_country IN ('Chile', 'Mexico')
AND development_type_use NOT IN ('Renewal')
---------
---------
---------

ORDER by activation_date ASC
),

companies as (
  SELECT DISTINCT
  company_id,
  company_legal_id,
  company_name
  FROM `btf-unified-data-platform.pdr_acquisition.companies`
)

SELECT 
acq.betterfly_country,
companies.company_legal_id,
companies.company_name,
acq.product_modules,
development_type_use,
activation_date

FROM acq
LEFT JOIN companies
ON acq.company_id = companies.company_id

ORDER by company_name