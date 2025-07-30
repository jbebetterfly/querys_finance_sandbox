-- Description: Orders con facturación, limpieza y clasificación mensual
-- Última actualización: 2025-05-08

WITH facturas AS (
 SELECT
   bt.depositorderid AS orden_facturada,
   CAST(JSON_VALUE(bt.withdraworders_member, '$.orderId') AS STRING) AS orden_ingresada,
   o.label AS label_order_facturada,
   o1.label AS label_order_ingresada,
   o.totalamount AS totalamount_orderdesaldo,
   CAST(CAST(JSON_VALUE(bt.withdraworders_member, '$.amount') AS FLOAT64) AS INT64) AS withdrawn_amount
 FROM `btf-wallet-backoffice-prod.fs_transaction_data.balance_transactions_schema_balance_transactions_latest` AS bt
 INNER JOIN `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS o
   ON bt.depositorderid = o.document_id
 INNER JOIN `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS o1
   ON CAST(JSON_VALUE(bt.withdraworders_member, '$.orderId') AS STRING) = o1.document_id
),

primer_mes_factura AS (
 SELECT
   CAST(
     COALESCE(
       REGEXP_EXTRACT(COALESCE(f.label_order_facturada, o.label), r'\d{13}'),
       REGEXP_EXTRACT(COALESCE(f.label_order_facturada, o.label), r'\d{6}')
     ) AS INT64
   ) AS factura_suprema_limpia,
   MIN(DATE(o.createdAt)) AS primera_fecha
 FROM
   `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS o
   LEFT JOIN facturas AS f ON f.label_order_ingresada = o.label
 WHERE
   DATE(o.createdAt) >= '2023-06-01'
   AND o.countryId = 'CL'
   AND o.status = 'done'
 GROUP BY factura_suprema_limpia
)

SELECT DISTINCT
 ord.createdAt AS orderCreatedDate,
 CAST(FORMAT_DATE('%Y%m', DATE(ord.createdAt)) AS INT64) AS `AnoMes Order`,
 ord.label AS orderId,
 CAST(
   COALESCE(
     REGEXP_EXTRACT(ord.label, r'\d{13}'),
     REGEXP_EXTRACT(ord.label, r'\d{6}')
   ) AS INT64
 ) AS `orderId Limpio`,
 ord.paymentMethod AS orderPaymentMethod,
 ord.status AS orderStatus,
 CASE
   WHEN com.nationalId IS NULL THEN c.document_value
   ELSE com.nationalId
 END AS companyNationalId,
 CASE
   WHEN com.legalName IS NULL THEN c.company_name
   ELSE com.legalName
 END AS companyLegalName,
 inv.invoiceNumber AS invoiceNumber,
 inv.amount AS invoiceAmount,
 COALESCE(f.label_order_facturada, ord.label) AS invoiced_orden_factura_suprema,
 CAST(
   COALESCE(
     REGEXP_EXTRACT(COALESCE(f.label_order_facturada, ord.label), r'\d{13}'),
     REGEXP_EXTRACT(COALESCE(f.label_order_facturada, ord.label), r'\d{6}')
   ) AS INT64
 ) AS `invoiced_orden_factura_suprema Limpio`,
 CAST(FORMAT_DATE('%Y%m', pmf.primera_fecha) AS INT64) AS `AnoMes Order Supremo`,
 withdrawn_amount AS withdrawn_amount_saldo,
 CAST(CAST(JSON_VALUE(ord.products_member, '$.quantity') AS FLOAT64) AS INT64) AS productAmount,
 CASE
   WHEN LOWER(ord.channel) = 'b2b-wallet-platform' THEN 'Portal'
   ELSE 'BO'
 END AS channel
FROM
 `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS ord
 LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.companies_schema_companies_latest` AS com
   ON com.document_id = ord.companyId
 LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.invoices_schema_invoices_latest` AS inv
   ON inv.orderId = ord.document_id
 LEFT JOIN facturas AS f
   ON f.label_order_ingresada = ord.label
 LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.products_schema_products_latest` AS prod
   ON prod.document_id = CAST(JSON_VALUE(ord.products_member, '$.productId') AS STRING)
 LEFT JOIN `btf-unified-data-platform.sdr_clients.client` AS c
   ON ord.companyId = CAST(c.id AS STRING)
 LEFT JOIN primer_mes_factura AS pmf
   ON pmf.factura_suprema_limpia = CAST(
     COALESCE(
       REGEXP_EXTRACT(COALESCE(f.label_order_facturada, ord.label), r'\d{13}'),
       REGEXP_EXTRACT(COALESCE(f.label_order_facturada, ord.label), r'\d{6}')
     ) AS INT64
   )
WHERE
 DATE(ord.createdAt) >= '2023-06-01'
 AND ord.countryId = 'CL'
 AND ord.status = 'done'
ORDER BY
 ord.createdAt ASC