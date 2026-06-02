---
--- REVENUE BUILDUPS
--- Construye la tabla de revenue con descomposición new / retention / churn
--- para value_lc, value_usd, members y logos.
---
--- Flujo: facturas + provisiones → join holdings → conciliación factura/provisión 
---        → gap_fill mensual → window functions → descomposición
---
--- Owner: Joaquim
--- ---------------------------> RECORDATORIO: Aquí no hay una partición por producto
---                              Esto significa que no analizo el producto para identificar si el logo es nuevo o churn.
---                              Por lo tanto: en tablas de buildups separadas por producto, habrá 1 new en sc y 1 churn en legacy (+1-1=0). Acá sólo habra 0 y 0.

-- ============================================================
-- 1. FUENTES DE INGRESO: unión de todas las tablas de facturación
-- ============================================================
WITH facturas_raw AS (
  SELECT legal_client_id, legal_client_name, document_type, revenue_stream,
         quantity_charged, value_lc, local_currency, service_date,
         legal_entity_country, sponsor, product
  FROM `btf-source-of-truth.cubo.ingresos_operaciones`

  UNION ALL
  SELECT legal_client_id, legal_client_name, document_type, revenue_stream,
         quantity_charged, value_lc, local_currency, service_date,
         legal_entity_country, sponsor, product
  FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos`
  WHERE legal_client_id IS NOT NULL OR legal_client_id != ''

  UNION ALL
  SELECT legal_client_id, legal_client_name, document_type, revenue_stream,
         quantity_charged, value_lc, local_currency, service_date,
         legal_entity_country, sponsor, product
  FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos_pre_2022`
  WHERE legal_client_id IS NOT NULL OR legal_client_id != ''

  UNION ALL
  SELECT legal_client_id, legal_client_name, document_type, revenue_stream,
         quantity_charged, value_lc, local_currency, service_date,
         legal_entity_country, sponsor, product
  FROM `btf-finance-sandbox.Revenue.temp-fix_ingresos-ops`  -- parches manuales
),

holdings AS (
  SELECT DISTINCT
    tax_id_client, holding_name, revenue_stream,
    service_country, client_segment, holding_cohort
  FROM `btf-finance-sandbox.Holdings.holdings_new`
),

-- ============================================================
-- 2. ENRIQUECIMIENTO: join con holdings + conversión USD + ajuste BRL
--    También marca si un holding/mes/stream tiene factura real (document_binary=1)
--    para resolver la prioridad factura > provisión en el paso siguiente.
-- ============================================================
facturas_enriquecidas AS (
  SELECT
    f.document_type,
    f.revenue_stream,
    f.quantity_charged,
    -- Brasil: se descuenta ~12% de tax del valor local
    CASE WHEN f.local_currency = 'BRL' THEN f.value_lc * 0.88
         ELSE f.value_lc
    END AS value_lc,
    f.local_currency,
    f.service_date                        AS full_date,
    f.legal_entity_country                AS service_country,
    f.sponsor,
    h.holding_name,
    h.holding_cohort,
    h.client_segment,
    CASE
      WHEN f.revenue_stream = 'EB' AND f.product IN ('S.C', 'S.C cargas') THEN f.product
      WHEN f.revenue_stream = 'EB'  THEN 'EB-XX-1'
      WHEN f.revenue_stream = 'CB'  THEN f.product
    END AS product,
    -- Conversión a USD con tipo de cambio presupuestario
    CASE WHEN f.local_currency = 'BRL' THEN SAFE_DIVIDE(f.value_lc * 0.88, fx.Value)
         ELSE SAFE_DIVIDE(f.value_lc, fx.Value)
    END AS value_usd_bdg,
    -- 1 si existe al menos una factura/NC para este holding+mes+stream+país
    MAX(CASE WHEN f.document_type IN ('Invoice', 'Credit Note', 'Bill') THEN 1
             WHEN f.document_type IN ('Provision', 'Provision write-off')  THEN 0
        END)
      OVER (PARTITION BY h.holding_name, f.service_date, f.revenue_stream, f.legal_entity_country)
      AS tiene_factura

  FROM facturas_raw f

  LEFT JOIN holdings h
    ON  UPPER(REPLACE(REPLACE(f.legal_client_id, '.', ''), '-', ''))
      = UPPER(REPLACE(REPLACE(h.tax_id_client,   '.', ''), '-', ''))
    AND f.revenue_stream        = h.revenue_stream
    AND f.legal_entity_country  = h.service_country

  LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` fx
    ON  CAST(EXTRACT(YEAR FROM f.service_date) AS STRING) = fx.Year
    AND f.local_currency = fx.Currency

  WHERE f.revenue_stream != 'Betterflyer'
    AND f.document_type  != 'Free'
),

-- ============================================================
-- 3. CONCILIACIÓN: si hay factura, usa factura; si no, usa provisión
-- ============================================================
revenue_conciliado AS (
  SELECT full_date, service_country, holding_name, holding_cohort,
         client_segment, local_currency, sponsor, revenue_stream,
         product, value_lc, value_usd_bdg, quantity_charged
  FROM facturas_enriquecidas
  WHERE (tiene_factura = 1 AND document_type IN ('Invoice', 'Bill', 'Credit Note'))
     OR (tiene_factura = 0 AND document_type IN ('Provision', 'Provision write-off'))
),

-- España viene de una tabla separada
revenue_spain AS (
  SELECT full_date, service_country, holding_name, holding_cohort,
         client_segment, local_currency, sponsor, revenue_stream,
         product, value_lc, value_usd_bdg, quantity_charged
  FROM `btf-finance-sandbox.Revenue.Revenue_Cubo_Spain`
),

-- ============================================================
-- 4. CONSOLIDACIÓN + AGREGACIÓN mensual por holding/producto
-- ============================================================
revenue_mensual AS (
  SELECT
    full_date,
    service_country, holding_name, holding_cohort, client_segment,
    local_currency, sponsor, revenue_stream, product,
    SUM(value_lc)         AS value_lc,
    SUM(value_usd_bdg)    AS value_usd_bdg,
    SUM(quantity_charged)  AS quantity_charged
  FROM (
    SELECT * FROM revenue_conciliado
    UNION ALL
    SELECT * FROM revenue_spain
  )
  GROUP BY full_date, service_country, holding_name, holding_cohort,
           client_segment, local_currency, sponsor, revenue_stream, product
  HAVING value_lc != 0
),

-- ============================================================
-- 5. GAP FILL: rellenar meses sin actividad para que los LAG funcionen
-- ============================================================
revenue_sin_gaps AS (
  SELECT * FROM gap_fill(
    TABLE revenue_mensual,
    ts_column            => ('full_date'),
    bucket_width         => INTERVAL 1 MONTH,
    partitioning_columns => [
      'revenue_stream', 'service_country', 'holding_name', 'holding_cohort',
      'client_segment', 'local_currency', 'sponsor', 'product'
    ],
    value_columns => [
      ('value_lc',         'null'),
      ('quantity_charged',  'null'),
      ('value_usd_bdg',    'null')
    ]
  )
),

-- ============================================================
-- 6. REVENUE BASE: agregar + calcular períodos anterior/siguiente con LAG/LEAD
-- ============================================================
revenue_base AS (
  SELECT
    full_date, service_country, revenue_stream, holding_name, sponsor,
    holding_cohort, client_segment, local_currency, product,

    COALESCE(SUM(value_lc), 0)         AS value_lc,
    COALESCE(SUM(value_usd_bdg), 0)    AS value_usd_bdg,
    COALESCE(SUM(quantity_charged), 0)  AS members,
    DATE_DIFF(full_date, holding_cohort, MONTH) AS invoice_number

  FROM revenue_sin_gaps
  WHERE revenue_stream IN ('EB', 'CB', 'Otro negocio')
    AND holding_name != ''
  GROUP BY full_date, service_country, revenue_stream, holding_name, sponsor,
           holding_cohort, client_segment, local_currency, product
),

con_periodos AS (
  SELECT
    *,
    -- Período anterior (BOP = beginning of period)
    COALESCE(LAG(members)      OVER w, 0) AS members_bop,
    COALESCE(LAG(value_lc)     OVER w, 0) AS value_lc_bop,
    COALESCE(LAG(value_usd_bdg) OVER w, 0) AS value_usd_bop,
    -- Período anterior -2 (para holdings con invoice_number=2 y gap en BOP)
    COALESCE(LAG(members, 2)      OVER w, 0) AS members_bop2,
    COALESCE(LAG(value_lc, 2)     OVER w, 0) AS value_lc_bop2,
    COALESCE(LAG(value_usd_bdg, 2) OVER w, 0) AS value_usd_bop2,
    -- Período siguiente (para detectar churn)
    LEAD(members)      OVER w AS next_members,
    LEAD(value_lc)     OVER w AS next_value_lc,
    LEAD(value_usd_bdg) OVER w AS next_value_usd

  FROM revenue_base
  WINDOW w AS (
    PARTITION BY holding_name, revenue_stream, service_country, sponsor, product
    ORDER BY EXTRACT(YEAR FROM full_date), EXTRACT(MONTH FROM full_date)
  )
),

-- ============================================================
-- 7. DESCOMPOSICIÓN: new / retention / churn
-- ============================================================
descomposicion AS (
  SELECT
    *,

    -- MEMBERS -----------------------------------------------
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

    CASE WHEN next_members IS NULL THEN members ELSE 0
    END AS members_churn,

    -- VALUE LC ----------------------------------------------
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

    CASE WHEN next_value_lc IS NULL THEN value_lc ELSE 0
    END AS value_lc_churn,

    -- VALUE USD ----------------------------------------------
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
      WHEN value_usd_bdg < 0 AND next_value_usd IS NULL THEN 0
      WHEN next_value_usd IS NULL THEN value_usd_bdg
      ELSE 0
    END AS value_usd_churn,

    -- LOGOS --------------------------------------------------
    CASE WHEN value_lc_bop IS NULL OR invoice_number < 0 THEN 0 ELSE 1
    END AS logo_bop,

    CASE WHEN invoice_number = 0 THEN 1 ELSE 0
    END AS logo_new,

    CASE WHEN next_value_lc IS NULL THEN 1 ELSE 0
    END AS logo_churn

  FROM con_periodos
)

-- ============================================================
-- 8. OUTPUT                      
-- ============================================================
SELECT
  full_date,
  EXTRACT(YEAR FROM full_date)   AS year,
  EXTRACT(MONTH FROM full_date)  AS month,
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
  -- members buildup
  members_bop,
  members_new,
  members_retention,
  members_churn,
  members          AS members_eop,
  -- value lc buildup
  value_lc_bop,
  value_lc_new,
  value_lc_retention,
  value_lc_churn,
  value_lc         AS value_lc_eop,
  -- value usd buildup
  value_usd_bop,
  value_usd_new,
  value_usd_retention,
  value_usd_churn,
  value_usd_bdg    AS value_usd_eop,
  -- logos buildup
  logo_bop,
  logo_new,
  logo_churn,
  CASE WHEN full_date < holding_cohort THEN 0
       ELSE CAST(1 AS INT64)
  END AS logo_eop

FROM descomposicion