--- DEV_ JOAQUIM
---PASO 1: Crea la tabla revenue cubo en base a ingresos ops mas los holdings y otras cosas, consiliando facturas con provisioens

-- ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW ON REVIEW 

WITH 
merge_operaciones AS(
  SELECT document_number,
document_date, 
legal_client_id, 
legal_client_name, 
invoice_description,	
document_type,	
canceled_document,	
revenue_stream,	
quantity_charged,	
value_lc,	
local_currency, 
service_date, 
legal_entity_country, 
sponsor, 
product,
contract_plan_id
FROM `btf-source-of-truth.cubo.ingresos_operaciones` 
  UNION ALL SELECT *, '' as contract_plan_id FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos` WHERE (legal_client_id IS NOT NULL OR legal_client_id != '') -- revenues 2023
  UNION ALL SELECT *, '' as contract_plan_id FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos_pre_2022` WHERE (legal_client_id IS NOT NULL OR legal_client_id != '') -- revenues 2022 y anteriores
  UNION ALL SELECT * FROM `btf-finance-sandbox.Revenue.temp-fix_ingresos-ops` -- parches para diferencias con facturas de holdings
),

holdings_new as (
  SELECT DISTINCT
  tax_id_client,
  holding_name,
  revenue_stream,
  service_country,
  client_segment,
  business_unit,
  holding_cohort
  FROM `btf-finance-sandbox.Holdings.holdings_new`
),

revenue_document_check AS
(
SELECT
  ingresos_operaciones.document_number,
  REPLACE(REPLACE(ingresos_operaciones.legal_client_id, '.', ''), '-', '') as tax_id_client,
  ingresos_operaciones.legal_client_name as client_legal_name,
  ingresos_operaciones.invoice_description,
  ingresos_operaciones.document_type,
  ingresos_operaciones.revenue_stream,
  ingresos_operaciones.document_type as type,
  ingresos_operaciones.quantity_charged,
  --- AQUI AJUSTAMOS EL INGRESO DE BRAZIL POR EL TAX DE APROX 12%
  CASE
    WHEN ingresos_operaciones.local_currency = "BRL" THEN ingresos_operaciones.value_lc*0.88
    ELSE ingresos_operaciones.value_lc
  END AS value_lc,
  ----
  ingresos_operaciones.local_currency,
  ingresos_operaciones.service_date as full_date,
  'Real' as version,
  safe_divide(ingresos_operaciones.value_lc,ingresos_operaciones.quantity_charged) AS arpu,
  holdings.client_segment,
  '' as holding_id,
  holdings.holding_name,
  ingresos_operaciones.sponsor,
  holdings.holding_cohort,
  ingresos_operaciones.legal_entity_country as service_country,
  CASE
    WHEN ingresos_operaciones.legal_entity_country = 'CL' THEN 'Chile'
    WHEN ingresos_operaciones.legal_entity_country = 'BR' THEN 'Brazil'
    WHEN ingresos_operaciones.legal_entity_country = 'AR' THEN 'Argentina'
    WHEN ingresos_operaciones.legal_entity_country = 'PE' THEN 'Peru'
    WHEN ingresos_operaciones.legal_entity_country = 'CO' THEN 'Colombia'
    WHEN ingresos_operaciones.legal_entity_country = 'MX' THEN 'Mexico'
    WHEN ingresos_operaciones.legal_entity_country = 'PE' THEN 'Peru'
    WHEN ingresos_operaciones.legal_entity_country = 'ES' THEN 'España'
    WHEN ingresos_operaciones.legal_entity_country = 'EC' THEN 'Ecuador'
  END AS legal_entity_country,
  CASE
    WHEN ingresos_operaciones.legal_entity_country = 'CL' THEN 'BTF Chile'
    WHEN ingresos_operaciones.legal_entity_country = 'BR' THEN 'BTF Brazil'
    WHEN ingresos_operaciones.legal_entity_country = 'AR' THEN 'BTF Argentina'
    WHEN ingresos_operaciones.legal_entity_country = 'PE' THEN 'BTF Peru'
    WHEN ingresos_operaciones.legal_entity_country = 'CO' THEN 'BTF Colombia'
    WHEN ingresos_operaciones.legal_entity_country = 'MX' THEN 'BTF Mexico'
    WHEN ingresos_operaciones.legal_entity_country = 'PE' THEN 'BTF Peru'
    WHEN ingresos_operaciones.legal_entity_country = 'ES' THEN 'BTF España'
    WHEN ingresos_operaciones.legal_entity_country = 'EC' THEN 'BTF Ecuador'
  END AS legal_entity_name,
  'Direct' as sales_channel,
  CASE
    WHEN ingresos_operaciones.revenue_stream = 'EB' AND product = 'S.C' THEN ingresos_operaciones.product
    WHEN ingresos_operaciones.revenue_stream = 'EB' AND product = 'S.C cargas' THEN ingresos_operaciones.product
    WHEN ingresos_operaciones.revenue_stream = 'EB' AND (product = '' or product IS NULL) THEN 'EB-XX-1'
    WHEN ingresos_operaciones.revenue_stream = 'CB' THEN ingresos_operaciones.product
  END AS product,
  fx.Value as fx_value_bdg,
  CASE
  WHEN local_currency = 'BRL' THEN safe_divide(ingresos_operaciones.value_lc*0.88,fx.Value)
  ELSE safe_divide(ingresos_operaciones.value_lc,fx.Value)
  END AS value_usd_bdg,
  'Cubo' as source,
  EXTRACT(MONTH FROM ingresos_operaciones.service_date) as month,
  EXTRACT(YEAR FROM ingresos_operaciones.service_date) as year,
  MAX(
    CASE
    WHEN document_type = 'Provision' THEN 0
    WHEN document_type in ('Invoice', 'Credit Note', 'Bill') THEN 1 END) 
    OVER (PARTITION BY holdings.holding_name, ingresos_operaciones.service_date,ingresos_operaciones.revenue_stream)
  AS document_binary,
  contract_plan_id

FROM merge_operaciones AS ingresos_operaciones

LEFT JOIN holdings_new holdings
  ON REPLACE(REPLACE(UPPER(ingresos_operaciones.legal_client_id), '.', ''), '-', '') = REPLACE(REPLACE(UPPER(holdings.tax_id_client), '.', ''), '-', '')
  AND ingresos_operaciones.revenue_stream = holdings.revenue_stream
  AND ingresos_operaciones.legal_entity_country = holdings.service_country

LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` fx
  ON CAST(EXTRACT(YEAR FROM ingresos_operaciones.service_date) as string) = fx.Year
  AND ingresos_operaciones.local_currency = fx.Currency

WHERE 
  ingresos_operaciones.revenue_stream NOT IN ('Betterflyer' , 'EB-Seguro_Covid')
  AND document_type != 'Free'
),

revenue_cubo AS (

SELECT 
document_number,
tax_id_client,
client_legal_name,
invoice_description,
document_type,
revenue_stream,
type,
quantity_charged,
value_lc,
local_currency,
full_date,
version,
arpu,
client_segment,
holding_id,
holding_name,
sponsor,
holding_cohort,
service_country,
legal_entity_country,
legal_entity_name,
sales_channel,
product,
fx_value_bdg,
value_usd_bdg,
source,
month,
year,
CASE
  WHEN document_binary = 0 THEN 'Provisionado'
  ELSE 'Facturado' 
END AS document_status,
contract_plan_id
FROM revenue_document_check 
WHERE 
  (document_binary = 1 AND document_type in ('Invoice','Bill','Credit Note')) 
  OR (document_binary = 0 and document_type = 'Provision') 

ORDER by holding_name, full_date
),

revenue_cubo_spain AS (
  SELECT * FROM `btf-finance-sandbox.Revenue.Revenue_Cubo_Spain`
  ORDER BY holding_name, full_date
),

revenue_historico_non_subs AS (
  SELECT *
  FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos_non_subs`
),

conecten AS (
SELECT 
*,
CASE WHEN business_partner_name LIKE "%BETTERFLY CHILE%" OR business_partner_name like "%CONECTEN%" THEN 1
ELSE 0
END AS relacionadas
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE legal_entity_name = "Conecten" and management_account_name = "Revenue" AND subversion = 'REAL'

),

revenue_conecten_2024 AS (
SELECT 
"0" AS document_number, --1
business_partner_id AS tax_id_client,--2
business_partner_name AS client_legal_name, --3
transaction_detail AS invoice_description, --4
"-" AS document_type, --5
"FS" AS revenue_stream, --6
"Real" AS type, --7
1 AS quantity_charged, --8
value_lc, --9
currency AS local_currency,--10
full_date, --11
"Real" AS version, --12
0 AS ARPU, --13
"-" AS client_segment, --14
"-" AS holding_id, --15
business_partner_name AS holding_name, --16
"-" AS sponsor, --17
DATETIME(DATE "2021-01-01") AS holding_cohort, --18
service_country, --19
legal_entity_country, --20
legal_entity_name, --21
"-" AS sales_channel, --22
"GC-XX-XX" AS product, --23
value_fx_bdg AS fx_value_bdg, --24
value_usd_bdg, --25
source, --26
month, --27
year, --28
"Facturado" AS document_status --29
FROM conecten
WHERE relacionadas = 0

),

revenues_historicos_non_subs_ps as (
  SELECT
  document_number as document_number, --1
  legal_client_id as tax_id_client, --2
  legal_client_name as client_legal_name, --3
  invoice_description as invoice_description, --4
  document_type as document_type, --5
  revenue_stream as revenue_stream, --6
  "Real" as type, --7
  quantity_charged as quantity_charged, --8
  value_lc as value_lc, --9
  local_currency as local_currency, --10
  service_date as full_date, --11
  'Real' as version, --12
  0 as arpu, --13 
  '' as client_segment, --14
  '' as holding_id, --15
  legal_client_name as holding_name, --16
  '' as sponsor, --17
  CAST('1899-12-01' as DATETIME) as holding_cohort, --18
  legal_entity_country as service_country, --19
  legal_entity_country as legal_entity_country, --20
  CASE
  WHEN legal_entity_country = 'CL' THEN 'Betterfly Chile'
  WHEN legal_entity_country = 'BR' then 'Betterfly Brazil'
  END as legal_entity_name, --21
  'Direct' AS sales_channel, --22
  '' as product,
  tc.Value as fx_value_bdg,
  safe_divide(historicos_non_subs_ps.value_lc,tc.Value) as value_usd_bdg,
  'Historicos_non_subs' as source,
  EXTRACT(MONTH FROM historicos_non_subs_ps.service_date) as month,
  EXTRACT(YEAR FROM historicos_non_subs_ps.service_date) as year,
  'Facturado' AS document_status

  FROM `btf-finance-sandbox.Revenues_historicos.ps_historicos` historicos_non_subs_ps
  LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` tc
  ON CAST(EXTRACT(YEAR FROM historicos_non_subs_ps.service_date) as string) = tc.Year
  AND historicos_non_subs_ps.local_currency = tc.Currency
),

revenue_rewards_otros AS (
  SELECT * from `btf-finance-sandbox.Revenue.revenue_rewards_mx_co`
),

revenue_cubo_consolidado AS (
  SELECT * FROM revenue_cubo
  UNION ALL
  SELECT *, '' as contract_plan_id FROM revenue_cubo_spain
  UNION ALL
  SELECT *, '' as contract_plan_id FROM revenue_historico_non_subs
  UNION ALL
  SELECT *, '' as contract_plan_id FROM revenue_conecten_2024
  UNION ALL
  SELECT *, '' as contract_plan_id FROM revenues_historicos_non_subs_ps
  UNION ALL
  SELECT *, '' as contract_plan_id FROM revenue_rewards_otros
)


SELECT 
* from
revenue_cubo_consolidado 
WHERE revenue_stream IS NOT NULL
ORDER BY year,month,revenue_stream,tax_id_client