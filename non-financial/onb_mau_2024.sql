WITH core_comp_id AS (

SELECT DISTINCT

cast(company_id as string) as company_id,
cast(company_tax_id as string) as company_tax_id

FROM `btf-source-of-truth.clients.clients_companies`

),

clients_data as (
  SELECT DISTINCT
  cast(client_id as string) as client_id,
  cast(client_tax_id as string) as client_tax_id 

  from `btf-source-of-truth.clients.clients`
),

--- aqui calculamos EMPLOYEE BENEFITS

mau_eb AS (

SELECT
date_start_month AS service_date,
cast(company_id as string) as company_id, 
"EB" as revenue_stream,
sum(members_onboard) as onboarded_members,
sum(mau) AS mau,

FROM `btf-source-of-truth.metrics.mt_mau_eb`

GROUP BY
date_start_month,
company_id
),

--- aqui calculamos AFFINITY

mau_cb AS (

SELECT
date_start_month AS service_date,
cast(company_id as string) as company_id, 
"CB" as revenue_stream,
sum(mau) AS mau,
sum(members_onboard) as onboarded_members

FROM `btf-source-of-truth.metrics.mt_mau_cb`

GROUP BY
date_start_month,
company_id
),

mau_app4 AS (

SELECT
date_start_month AS service_date,
cast(client_id as string) as client_id,
CASE
WHEN business_type = 'customer_engagement' THEN "CB" 
WHEN business_type = 'employee_engagement' THEN "EB" 
END as revenue_stream,
sum(mau) AS mau,
sum(members_onboard) as onboarded_members

FROM `btf-source-of-truth.metrics.mt_mau_app4`

GROUP BY
date_start_month,
client_id,
revenue_stream
),

--- aqui HOLDINGS

holdings AS (

SELECT DISTINCT
revenue_stream,
holding_name,
service_country,
client_segment,
replace(replace(replace(tax_id_client, '/',''),'-',''),'.','') as tax_id_client

FROM `btf-finance-sandbox.Holdings.holdings_new`


),

cb AS (

SELECT 
mau.revenue_stream,
mau.service_date AS service_date,
mau.company_id, 
cci.company_tax_id,
COALESCE(mau.onboarded_members,0) AS onboarded_members,
COALESCE(mau.mau,0) AS mau

FROM mau_cb as mau 

LEFT JOIN core_comp_id AS cci
ON mau.company_id = cast(cci.company_id as string)
),

eb AS (

SELECT 
mau.revenue_stream,
mau.service_date AS service_date,
mau.company_id, 
cci.company_tax_id,
COALESCE(mau.onboarded_members,0) AS onboarded_members,
COALESCE(mau.mau,0) AS mau

FROM mau_eb as mau

LEFT JOIN core_comp_id AS cci
ON mau.company_id = cast(cci.company_id as string)
),

app4 AS (

SELECT 
mau.revenue_stream,
mau.service_date AS service_date,
CAST(mau.client_id as string) as client_id,
cci.client_tax_id as company_tax_id,
COALESCE(mau.onboarded_members,0) AS onboarded_members,
COALESCE(mau.mau,0) AS mau

FROM mau_app4 as mau

LEFT JOIN clients_data AS cci
ON mau.client_id = cast(cci.client_id as string)
),

casi AS (

SELECT
*
FROM eb
UNION ALL 
SELECT
*
FROM cb
UNION ALL 
SELECT
*
FROM app4

)


SELECT
casi.revenue_stream,
casi.service_date,
hg.service_country,
hg.client_segment,
SUM(onboarded_members) AS onboarded_members,
SUM(mau) AS mau

FROM casi

LEFT JOIN holdings as hg
ON casi.company_tax_id = hg.tax_id_client
AND casi.revenue_stream = hg.revenue_stream

GROUP BY 
casi.revenue_stream,
casi.service_date,
hg.service_country,
hg.client_segment

ORDER BY revenue_stream, service_date, service_country, client_segment