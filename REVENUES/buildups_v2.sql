--- DEV: JOAQUIM (optimizado)
--- Query para buildups con campo Product incluido en resultado final
--- Campos no utilizados downstream fueron removidos

WITH 
merge_operaciones AS (
  SELECT 
    legal_client_id,
    legal_client_name,
    document_type,
    revenue_stream,
    quantity_charged,
    value_lc,
    local_currency,
    service_date,
    legal_entity_country,
    sponsor,
    product
  FROM `btf-source-of-truth.cubo.ingresos_operaciones`

  UNION ALL 
  SELECT 
    legal_client_id,
    legal_client_name,
    document_type,
    revenue_stream,
    quantity_charged,
    value_lc,
    local_currency,
    service_date,
    legal_entity_country,
    sponsor,
    product
  FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos` 
  WHERE (legal_client_id IS NOT NULL OR legal_client_id != '')

  UNION ALL 
  SELECT 
    legal_client_id,
    legal_client_name,
    document_type,
    revenue_stream,
    quantity_charged,
    value_lc,
    local_currency,
    service_date,
    legal_entity_country,
    sponsor,
    product
  FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos_pre_2022` 
  WHERE (legal_client_id IS NOT NULL OR legal_client_id != '')

  UNION ALL 
  SELECT
    legal_client_id,
    legal_client_name,
    document_type,
    revenue_stream,
    quantity_charged,
    value_lc,
    local_currency,
    service_date,
    legal_entity_country,
    sponsor,
    product 
  FROM `btf-finance-sandbox.Revenue.temp-fix_ingresos-ops`
),

holdings_new AS (
  SELECT DISTINCT
    tax_id_client,
    holding_name,
    revenue_stream,
    service_country,
    client_segment,
    holding_cohort
  FROM `btf-finance-sandbox.Holdings.holdings_new`
),

--- Simplificado: solo campos que se usan downstream + document_type/document_binary para filtro
revenue_document_check AS (
  SELECT
    ingresos_operaciones.document_type,
    ingresos_operaciones.revenue_stream,
    ingresos_operaciones.quantity_charged,
    CASE
      WHEN ingresos_operaciones.local_currency = 'BRL' THEN ingresos_operaciones.value_lc * 0.88
      ELSE ingresos_operaciones.value_lc
    END AS value_lc,
    ingresos_operaciones.local_currency,
    ingresos_operaciones.service_date AS full_date,
    holdings.client_segment,
    holdings.holding_name,
    ingresos_operaciones.sponsor,
    holdings.holding_cohort,
    ingresos_operaciones.legal_entity_country AS service_country,
    CASE
      WHEN ingresos_operaciones.revenue_stream = 'EB' AND product = 'S.C' THEN ingresos_operaciones.product
      WHEN ingresos_operaciones.revenue_stream = 'EB' AND product = 'S.C cargas' THEN ingresos_operaciones.product
      WHEN ingresos_operaciones.revenue_stream = 'EB' THEN 'EB-XX-1'
      WHEN ingresos_operaciones.revenue_stream = 'CB' THEN ingresos_operaciones.product
    END AS product,
    CASE
      WHEN local_currency = 'BRL' THEN safe_divide(ingresos_operaciones.value_lc * 0.88, fx.Value)
      ELSE safe_divide(ingresos_operaciones.value_lc, fx.Value)
    END AS value_usd_bdg,
    MAX(
      CASE
        WHEN document_type IN ('Provision', 'Provision write-off') THEN 0
        WHEN document_type IN ('Invoice', 'Credit Note', 'Bill') THEN 1
      END
    ) OVER (
      PARTITION BY holdings.holding_name, ingresos_operaciones.service_date, ingresos_operaciones.revenue_stream, ingresos_operaciones.legal_entity_country
    ) AS document_binary

  FROM merge_operaciones AS ingresos_operaciones

  LEFT JOIN holdings_new holdings
    ON UPPER(REPLACE(REPLACE(ingresos_operaciones.legal_client_id, '.', ''), '-', '')) = UPPER(REPLACE(REPLACE(holdings.tax_id_client, '.', ''), '-', ''))
    AND ingresos_operaciones.revenue_stream = holdings.revenue_stream
    AND ingresos_operaciones.legal_entity_country = holdings.service_country

  LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` fx
    ON CAST(EXTRACT(YEAR FROM ingresos_operaciones.service_date) AS STRING) = fx.Year
    AND ingresos_operaciones.local_currency = fx.Currency

  WHERE 
    ingresos_operaciones.revenue_stream != 'Betterflyer' 
    AND document_type != 'Free'
),

revenue_cubo AS (
  SELECT 
    full_date,
    service_country,
    holding_name,
    holding_cohort,
    client_segment,
    local_currency,
    sponsor,
    revenue_stream,
    product,
    value_lc,
    value_usd_bdg,
    quantity_charged
  FROM revenue_document_check 
  WHERE 
    (document_binary = 1 AND document_type IN ('Invoice', 'Bill', 'Credit Note')) 
    OR (document_binary = 0 AND document_type IN ('Provision', 'Provision write-off'))
),

--- Revenue_Cubo_Spain: seleccionamos solo las columnas necesarias para no depender de SELECT *
revenue_cubo_spain AS (
  SELECT 
    full_date,
    service_country,
    holding_name,
    holding_cohort,
    client_segment,
    local_currency,
    sponsor,
    revenue_stream,
    product,
    value_lc,
    value_usd_bdg,
    quantity_charged
  FROM `btf-finance-sandbox.Revenue.Revenue_Cubo_Spain`
),

revenue_cubo_consolidado AS (
  SELECT * FROM revenue_cubo
  UNION ALL
  SELECT * FROM revenue_cubo_spain
),

revenue_cubo_jb AS (
  SELECT 
    full_date,
    EXTRACT(YEAR FROM full_date) AS year, 
    EXTRACT(MONTH FROM full_date) AS month, 
    service_country, 
    holding_name, 
    holding_cohort, 
    client_segment, 
    local_currency,
    sponsor,
    revenue_stream,
    product,
    SUM(value_lc) AS value_lc,
    SUM(value_usd_bdg) AS value_usd_bdg,
    SUM(quantity_charged) AS quantity_charged
  FROM revenue_cubo_consolidado
  GROUP BY 
    full_date,
    year, 
    month, 
    service_country, 
    holding_name, 
    holding_cohort, 
    client_segment, 
    local_currency,
    sponsor,
    revenue_stream,
    product
),

revenue_cubo_jb_2 AS (
  SELECT * FROM revenue_cubo_jb WHERE value_lc != 0
),

revenue_cubo_no_gaps AS (
  SELECT * FROM gap_fill(
    TABLE revenue_cubo_jb_2,
    ts_column => ('full_date'),
    bucket_width => INTERVAL 1 MONTH,
    partitioning_columns => ['revenue_stream', 'service_country', 'holding_name', 'holding_cohort', 'client_segment', 'local_currency', 'sponsor', 'product'],
    value_columns => [
      ('value_lc', 'null'),
      ('quantity_charged', 'null'),
      ('value_usd_bdg', 'null')
    ]
  )
),

--- PASO 2: calcular variacion mensual
revenue AS (
  SELECT  
    full_date,
    EXTRACT(YEAR FROM full_date) AS year, 
    EXTRACT(MONTH FROM full_date) AS month,
    revenue_stream, 
    service_country, 
    holding_name, 
    holding_cohort, 
    client_segment, 
    local_currency,
    sponsor,
    product,
    COALESCE(SUM(value_lc), 0) AS value_lc,
    COALESCE(SUM(quantity_charged), 0) AS members,
    COALESCE(SUM(value_usd_bdg), 0) AS value_usd_bdg
  FROM revenue_cubo_no_gaps
  WHERE revenue_stream IN ('EB', 'CB', 'Otro negocio') AND holding_name != ''
  GROUP BY 
    full_date, year, month, revenue_stream, service_country, 
    holding_name, holding_cohort, client_segment, sponsor, local_currency, product
),

revenue_base AS (
  SELECT
    full_date,
    service_country,
    revenue_stream, 
    holding_name,
    sponsor, 
    holding_cohort, 
    client_segment, 
    local_currency,
    product,
    value_lc,
    value_usd_bdg,
    members,
    DATE_DIFF(full_date, holding_cohort, MONTH) AS invoice_number,
    COALESCE(LAG(members) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS members_bop,
    LEAD(members) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)) AS next_period_members,
    COALESCE(LAG(value_lc) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS value_lc_bop,
    LEAD(value_lc) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)) AS next_period_value_lc,
    COALESCE(LAG(value_usd_bdg) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS value_usd_bop,
    LEAD(value_usd_bdg) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)) AS next_period_value_usd,
    COALESCE(LAG(members, 2) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS members_bop2,
    COALESCE(LAG(value_lc, 2) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS value_lc_bop2,
    COALESCE(LAG(value_usd_bdg, 2) OVER (PARTITION BY holding_name, revenue_stream, service_country, sponsor, product ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)), 0) AS value_usd_bop2
  FROM revenue
),

calculated_revenue AS (
  SELECT *,
    --- members
    CASE
      WHEN invoice_number = 0 THEN members
      WHEN invoice_number = 1 AND members_bop < members THEN members - members_bop
      WHEN invoice_number = 2 AND members_bop > 0 AND members_bop < members THEN members - members_bop
      WHEN invoice_number = 2 AND members_bop = 0 AND members_bop2 > 0 AND members_bop2 < members THEN members - members_bop2
      WHEN invoice_number = 2 AND members_bop = 0 AND members_bop2 = 0 AND members > members_bop THEN members
      ELSE 0
    END AS members_new,
    CASE
      WHEN invoice_number = 0 THEN 0
      WHEN invoice_number = 1 AND members_bop > members THEN members - members_bop
      WHEN invoice_number = 2 AND members_bop > members THEN members - members_bop
      WHEN invoice_number > 2 THEN members - members_bop
      ELSE 0
    END AS members_retention,
    CASE
      WHEN next_period_members IS NULL THEN members
      ELSE 0
    END AS members_churn,
    --- revenues lc
    CASE
      WHEN invoice_number = 0 THEN value_lc
      WHEN invoice_number = 1 AND value_lc_bop < value_lc THEN value_lc - value_lc_bop
      WHEN invoice_number = 2 AND value_lc_bop > 0 AND value_lc_bop < value_lc THEN value_lc - value_lc_bop
      WHEN invoice_number = 2 AND value_lc_bop = 0 AND value_lc_bop2 > 0 AND value_lc_bop2 < value_lc THEN value_lc - value_lc_bop2
      ELSE 0
    END AS value_lc_new,
    CASE
      WHEN invoice_number = 0 THEN 0
      WHEN invoice_number = 1 AND value_lc_bop > value_lc THEN value_lc - value_lc_bop
      WHEN invoice_number = 2 AND value_lc_bop > value_lc THEN value_lc - value_lc_bop
      WHEN invoice_number > 2 THEN value_lc - value_lc_bop
      ELSE 0
    END AS value_lc_retention,
    CASE
      WHEN next_period_value_lc IS NULL THEN value_lc
      ELSE 0
    END AS value_lc_churn,
    --- revenues usd
    CASE
      WHEN invoice_number = 0 THEN value_usd_bdg
      WHEN invoice_number = 1 AND value_usd_bop < value_usd_bdg THEN value_usd_bdg - value_usd_bop
      WHEN invoice_number = 2 AND value_usd_bop > 0 AND value_usd_bop < value_usd_bdg THEN value_usd_bdg - value_usd_bop
      WHEN invoice_number = 2 AND value_usd_bop = 0 AND value_usd_bop2 > 0 AND value_usd_bop2 < value_usd_bdg THEN value_usd_bdg - value_usd_bop2
      ELSE 0
    END AS value_usd_new,
    CASE
      WHEN invoice_number = 0 THEN 0
      WHEN invoice_number = 1 AND value_usd_bop > value_usd_bdg THEN value_usd_bdg - value_usd_bop
      WHEN invoice_number = 2 AND value_usd_bop > value_usd_bdg THEN value_usd_bdg - value_usd_bop
      WHEN invoice_number > 2 THEN value_usd_bdg - value_usd_bop
      ELSE 0
    END AS value_usd_retention,
    CASE
      WHEN value_usd_bdg < 0 AND next_period_value_usd IS NULL THEN 0 
      WHEN next_period_value_usd IS NULL THEN value_usd_bdg
      ELSE 0
    END AS value_usd_churn,
    --- logos
    CASE
      WHEN value_lc_bop IS NULL THEN 0
      WHEN invoice_number < 0 THEN 0
      ELSE 1
    END AS logo_bop,
    CASE
      WHEN invoice_number = 0 THEN 1
      ELSE 0
    END AS logo_new,
    CASE
      WHEN next_period_value_lc IS NULL THEN 1
      ELSE 0
    END AS logo_churn
  FROM revenue_base
)

SELECT 
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  EXTRACT(MONTH FROM full_date) AS month,
  service_country,
  revenue_stream,
  holding_name,
  sponsor,
  holding_cohort,
  client_segment, 
  local_currency,
  product,
  value_lc,
  members,
  invoice_number,
  members_bop,
  members_new,
  members_retention,
  members_churn,
  members AS members_eop,
  value_lc_bop,
  value_lc_new,
  value_lc_retention,
  value_lc_churn,
  value_lc AS value_lc_eop,
  value_usd_bop,
  value_usd_new,
  value_usd_retention,
  value_usd_churn,
  value_usd_bdg AS value_usd_eop,
  logo_bop,
  logo_new,
  logo_churn,
  CASE 
    WHEN full_date < holding_cohort THEN 0
    ELSE CAST(1 AS INT64) 
  END AS logo_eop
FROM calculated_revenue