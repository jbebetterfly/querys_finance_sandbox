---------------- QUERY COHORTS --------------
----- INTENTO DE MANTENER ORDENADA ----------

WITH fx as (
  SELECT year, currency, value FROM `btf-finance-sandbox.Budget.Currency_conversion_bdg`
  WHERE CAST(year AS INT64) >= 2024
),
cruce_ingresos_pati as (
  SELECT DISTINCT
  service_date,
  legal_client_id,
  legal_client_name,
  holding_name,
  sum(quantity_charged) as quantity_charged,
  sum(value_lc) as value_lc,
  product,
  'sc' as seguro_check FROM `btf-source-of-truth.cubo.ingresos_operaciones` ingresos_operaciones
  LEFT JOIN `btf-finance-sandbox.Holdings.holdings_new` holdings
  ON REPLACE(REPLACE(UPPER(ingresos_operaciones.legal_client_id), '.', ''), '-', '') = REPLACE(REPLACE(UPPER(holdings.tax_id_client), '.', ''), '-', '')
  AND ingresos_operaciones.revenue_stream = holdings.revenue_stream
  AND ingresos_operaciones.legal_entity_country = holdings.service_country
  WHERE product in ('S.C', 'S.C cargas')
  AND ingresos_operaciones.revenue_stream = 'EB'
  
  GROUP BY
  service_date,
  legal_client_id,
  legal_client_name,
  holding_name,
  product
),
filtro_buildups as (
  SELECT full_date,
  bups.year,
  month,
  service_country,
  revenue_stream,
  bups.holding_name,
  product,
  holding_cohort as past_cohort,
  client_segment,
  local_currency,
  cip.value_lc,
  members,
  invoice_number,
  members_bop,
  members_new,
  members_retention,
  members_churn,
  CASE
  WHEN product = 'S.C' THEN cip.quantity_charged
  WHEN product = 'S.C cargas' THEN 0
  END as members_eop,
  cip.value_lc/fx.value AS value_usd,
  bups.logo_bop,
  bups.logo_new,
  bups.logo_churn,
  CASE
    WHEN LEAD(logo_eop) OVER (PARTITION BY bups.holding_name, bups.year, bups.month ORDER BY bups.holding_name, bups.year, bups.month) >= 1 THEN 0
    ELSE logo_eop
    END as logo_eop,
  cip.seguro_check


   FROM `btf-finance-sandbox.Revenue.Buildups_EB_CB_Spain` bups
  LEFT JOIN cruce_ingresos_pati cip ON
  bups.month = EXTRACT(MONTH from cip.service_date)
  AND bups.year = EXTRACT(YEAR from cip.service_date)
  AND bups.holding_name = cip.holding_name

  LEFT JOIN fx
  ON EXTRACT(YEAR FROM cip.service_date) = CAST(fx.Year AS INT64)
  AND bups.local_currency = fx.Currency

  WHERE seguro_check IS NOT NULL
),

cohort_check as (
  SELECT *,
  MIN(full_date) OVER (PARTITION BY holding_name) as holding_cohort from filtro_buildups
)

SELECT 
  year,
  month,
  client_segment,
  holding_name,
  DATE(holding_cohort) as holding_cohort,
  service_country,
  local_currency,
  product,
  SUM(members_eop) AS sum_members_eop,
  SUM(value_lc) AS sum_value_lc_eop,
  SUM(logo_bop) AS sum_logo_bop,
  SUM(logo_new) AS sum_logo_new,
  SUM(logo_churn) AS sum_logo_churn,
  SUM(logo_eop) AS sum_logo_eop,
  SUM(value_usd) AS sum_value_usd_eop,
  DATE_DIFF(full_date, holding_cohort, MONTH) as period
FROM
  cohort_check
WHERE revenue_stream = "EB"
AND upper(holding_name) NOT LIKE '%COLSUBSIDIO%'
AND service_country in ('CL', 'MX')
GROUP BY
  year, month, client_segment, product, holding_name, holding_cohort, period, service_country, local_currency

ORDER BY
  year, month, client_segment, holding_name, holding_cohort, service_country
