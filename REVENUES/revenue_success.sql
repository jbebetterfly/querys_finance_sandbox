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
    WHERE revenue_stream = 'EB'

    UNION ALL
    SELECT
        legal_client_id, legal_client_name, document_type, revenue_stream,
        quantity_charged, value_lc, local_currency, service_date, legal_entity_country
    FROM `btf-finance-sandbox.Revenue.temp-fix_ingresos-ops`
    WHERE revenue_stream = 'EB'
),

revenue_with_doc_check AS (
    SELECT
        ops.service_date,
        ops.legal_client_id,
        ops.legal_client_name,
        ops.document_type,
        ops.revenue_stream,
        ops.quantity_charged,
        ops.value_lc,
        ops.local_currency,
        ops.legal_entity_country,
        h.holding_name,
        h.client_segment,
        fx.Value                                                         AS fx_value,

        MAX(CASE
            WHEN ops.document_type IN ('Provision', 'Provision write-off') THEN 0
            WHEN ops.document_type IN ('Invoice', 'Credit Note', 'Bill')   THEN 1
        END) OVER (
            PARTITION BY h.holding_name, ops.service_date,
                         ops.revenue_stream, ops.legal_entity_country
        )                                                                AS document_binary

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
      AND ops.legal_entity_country IN ('CL', 'MX')
)

SELECT
    service_date,
    holding_name,
    client_segment                                               AS holding_size,
    REPLACE(REPLACE(legal_client_id, '.', ''), '-', '')          AS tax_id,
    legal_client_name                                            AS company_name,
    legal_entity_country                                         AS country,
    local_currency                                               AS currency,
    SUM(quantity_charged)                                         AS quantity,
    SUM(CASE
        WHEN local_currency = 'BRL' THEN value_lc * 0.88
        ELSE value_lc
    END)                                                         AS revenue,
    SAFE_DIVIDE(
        SUM(CASE WHEN local_currency = 'BRL' THEN value_lc * 0.88 ELSE value_lc END),
        SUM(quantity_charged)
    )                                                            AS arpu,
    SUM(CASE
        WHEN local_currency = 'BRL' THEN SAFE_DIVIDE(value_lc * 0.88, fx_value)
        ELSE SAFE_DIVIDE(value_lc, fx_value)
    END)                                                         AS revenue_usd,
    SAFE_DIVIDE(
        SUM(CASE WHEN local_currency = 'BRL' THEN SAFE_DIVIDE(value_lc * 0.88, fx_value) ELSE SAFE_DIVIDE(value_lc, fx_value) END),
        SUM(quantity_charged)
    )                                                            AS arpu_usd,
    SAFE_DIVIDE(
        SUM(CASE WHEN local_currency = 'BRL' THEN value_lc * 0.88 ELSE value_lc END),
        SUM(SUM(CASE WHEN local_currency = 'BRL' THEN value_lc * 0.88 ELSE value_lc END))
            OVER (PARTITION BY service_date, legal_entity_country)
    )                                                            AS revenue_weight

FROM revenue_with_doc_check
WHERE ((document_binary = 1 AND document_type IN ('Invoice', 'Bill', 'Credit Note'))
   OR  (document_binary = 0 AND document_type IN ('Provision', 'Provision write-off')))
  AND service_date >= '2025-01-01'

GROUP BY
    service_date,
    holding_name,
    client_segment,
    legal_client_id,
    legal_client_name,
    legal_entity_country,
    local_currency

ORDER BY holding_name, service_date