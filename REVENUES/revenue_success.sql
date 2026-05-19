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
        legal_entity_country
    FROM `btf-source-of-truth.cubo.ingresos_operaciones`

    UNION ALL
    SELECT
        legal_client_id, legal_client_name, document_type, revenue_stream,
        quantity_charged, value_lc, local_currency, service_date, legal_entity_country
    FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos`
    WHERE legal_client_id IS NOT NULL OR legal_client_id != ''

    UNION ALL
    SELECT
        legal_client_id, legal_client_name, document_type, revenue_stream,
        quantity_charged, value_lc, local_currency, service_date, legal_entity_country
    FROM `btf-finance-sandbox.Revenues_historicos.revenues_historicos_pre_2022`
    WHERE legal_client_id IS NOT NULL OR legal_client_id != ''

    UNION ALL
    SELECT
        legal_client_id, legal_client_name, document_type, revenue_stream,
        quantity_charged, value_lc, local_currency, service_date, legal_entity_country
    FROM `btf-finance-sandbox.Revenue.temp-fix_ingresos-ops`
)

SELECT
    ops.service_date,
    h.holding_name,
    h.client_segment                                             AS holding_size,
    REPLACE(REPLACE(ops.legal_client_id, '.', ''), '-', '')      AS tax_id,
    ops.legal_client_name                                        AS company_name,
    ops.legal_entity_country                                     AS country,
    ops.local_currency                                           AS currency,
    SUM(ops.quantity_charged)                                     AS quantity,
    SUM(CASE
        WHEN ops.local_currency = 'BRL' THEN ops.value_lc * 0.88
        ELSE ops.value_lc
    END)                                                         AS revenue,
    SAFE_DIVIDE(
        SUM(CASE WHEN ops.local_currency = 'BRL' THEN ops.value_lc * 0.88 ELSE ops.value_lc END),
        SUM(ops.quantity_charged)
    )                                                            AS arpu,
    SUM(CASE
        WHEN ops.local_currency = 'BRL' THEN SAFE_DIVIDE(ops.value_lc * 0.88, fx.Value)
        ELSE SAFE_DIVIDE(ops.value_lc, fx.Value)
    END)                                                         AS revenue_usd,
    SAFE_DIVIDE(
        SUM(CASE WHEN ops.local_currency = 'BRL' THEN SAFE_DIVIDE(ops.value_lc * 0.88, fx.Value) ELSE SAFE_DIVIDE(ops.value_lc, fx.Value) END),
        SUM(ops.quantity_charged)
    )                                                            AS arpu_usd

FROM merge_operaciones ops

LEFT JOIN `btf-finance-sandbox.Holdings.holdings_new` h
       ON UPPER(REPLACE(REPLACE(ops.legal_client_id, '.', ''), '-', ''))
        = UPPER(REPLACE(REPLACE(h.tax_id_client, '.', ''), '-', ''))
      AND ops.revenue_stream      = h.revenue_stream
      AND ops.legal_entity_country = h.service_country

LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` fx
       ON CAST(EXTRACT(YEAR FROM ops.service_date) AS STRING) = fx.Year
      AND ops.local_currency = fx.Currency

WHERE ops.revenue_stream != 'Betterflyer'
  AND ops.document_type  != 'Free'
  AND service_country IN ('CL', 'MX')
  AND service_date >= '2025-01-01'

GROUP BY
    ops.service_date,
    h.holding_name,
    h.client_segment,
    ops.legal_client_id,
    ops.legal_client_name,
    ops.legal_entity_country,
    ops.local_currency