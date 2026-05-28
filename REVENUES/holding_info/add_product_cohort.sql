WITH acq as (



SELECT DISTINCT
betterfly_country,
company_id,
company_name,
product_modules,
activation_date


FROM  `btf-unified-data-platform.pdr_acquisition.deals`


WHERE activation_date >= '2026-05-01'
AND betterfly_country IN ('Chile', 'Mexico')
AND LOWER(development_type_use) IN ('new logo')

ORDER by activation_date ASC
),

companies as (
  SELECT DISTINCT
  company_id,
  company_legal_id
  FROM `btf-unified-data-platform.pdr_acquisition.companies`
)

SELECT 
acq.betterfly_country,
acq.company_id,
companies.company_legal_id,
acq.product_modules,
activation_date

FROM acq
LEFT JOIN companies
ON acq.company_id = companies.company_id