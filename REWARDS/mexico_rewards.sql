WITH revenue_rewards_mexico AS (
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
CAST(full_date as DATE) as full_date, --11
"Real" AS version, --12
0 AS ARPU, --13
"-" AS client_segment, --14
"-" AS holding_id, --15
business_partner_name AS holding_name, --16
"-" AS sponsor, --17
CAST ("2021-01-01" as DATE) AS holding_cohort, --18
service_country, --19
legal_entity_country, --20
legal_entity_name, --21
"-" AS sales_channel, --22
"GC-XX-01" AS product, --23
value_fx_bdg AS value_fx_bdg, --24
value_usd_bdg as value_usd_bdg, --25
source as source, --26
month, --27
year, --28
"Facturado" AS document_status --29
FROM `btf-finance-sandbox.Expenses.Cubo_Financiero`
WHERE source = 'Cierre Mexico X-2024'
AND accounting_account_id = '41010203'

)

select * from revenue_rewards_mexico