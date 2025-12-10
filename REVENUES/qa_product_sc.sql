---- QA CHECK SC

WITH holdings_algun_sc as (
SELECT
DISTINCT
service_country,
legal_client_id,
holding_name,
product,
MAX(CASE
WHEN UPPER(product) IN ('S.C', 'S.C CARGAS') THEN 1
ELSE 0 END)
OVER (PARTITION BY holding_name ORDER BY holding_name, legal_client_id) AS check_product_por_holding

FROM `btf-source-of-truth.cubo.ingresos_operaciones` io
LEFT JOIN `btf-finance-sandbox.Holdings.holdings_new` holdings
  ON UPPER(REPLACE(REPLACE(io.legal_client_id, '.', ''), '-', '')) = UPPER(REPLACE(REPLACE(holdings.tax_id_client, '.', ''), '-', ''))
  AND io.revenue_stream = holdings.revenue_stream
  AND io.legal_entity_country = holdings.service_country

WHERE service_date >= '2024-11-01'
AND service_country in ('CL', 'MX')
AND document_type != 'Free'
AND io.revenue_stream = 'EB')

SELECT * FROM holdings_algun_sc

WHERE product IS NULL and check_product_por_holding = 1

-- EXCLUSIÓN HOLDINGS QUE EFECTIVAMENTE TUVIERON LEGACY E HICIERON UPGRADE A HEALTH (PROD)

AND holding_name NOT IN (
  'HEAV', -- OK
  'INELCOM',-- OK
  'ADIPA',-- OK
  'TRIIBU',-- OK
  'CLÍNICA LORETO CID',
  'CONVERTIBLE',
  'DFM',
  'HOLDING RECICLADOS INDUSTRIALES',
  'JOOYCAR',
  'TRAZOLABS',
  'VOLTA',
  'INFORMSOFTWARESPA'

)

ORDER BY


holding_name,
legal_client_id
---- QA CHECK S.C
