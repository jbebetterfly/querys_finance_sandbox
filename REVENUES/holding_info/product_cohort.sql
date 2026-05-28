WITH base_io AS

(

SELECT DISTINCT
MIN(service_date) as product_cohort,
legal_entity_country,
REPLACE(REPLACE(UPPER(legal_client_id), '.', ''), '-', '') as legal_client_id,
CASE
WHEN product IN ('S.C', 'S.C cargas') THEN 'health'
ELSE 'legacy'
END as product_


FROM `btf-source-of-truth.cubo.ingresos_operaciones`

WHERE product IN ('S.C', 'S.C cargas')
AND legal_entity_country IN ('CL', 'MX')

GROUP BY
legal_entity_country,
legal_client_id,
product_
),

holdings_new as (
  SELECT DISTINCT
  tax_id_client,
  holding_name
  
  FROM `btf-finance-sandbox.Holdings.holdings_new`
)


SELECT DISTINCT
product_cohort,
legal_entity_country as service_country,
legal_client_id,
holding_name,
product_ as product


FROM base_io io
LEFT JOIN holdings_new hn
ON io.legal_client_id = hn.tax_id_client

ORDER by product_cohort ASC, holding_name ASC