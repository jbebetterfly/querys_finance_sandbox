-- Pendiente: Agrupar producto con nueva tabla
SELECT 
gastos_base.expense_id,
gastos_base.transaction_id,
gastos_base.document_id,
CAST(gastos_base.full_date as DATETIME) as full_date,
gastos_base.accounting_account_id,
gastos_base.accounting_account_name,
CAST(null as string) as management_account_id,
relacion_cuentas.management_account_name,
relacion_cuentas.management_account_classification as mgmt_account_classification,
relacion_cuentas.management_account_subclassification as mgmt_account_subclasification,
gastos_base.accounting_department_id as accounting_cost_center_dim1_id,
gastos_base.accounting_department_name as accounting_cost_center_dim1_name,
gastos_base.accounting_country_id as accounting_cost_center_dim2_id,
gastos_base.accounting_country_name as accounting_cost_center_dim2_name,
relacion_cecos.management_department,
classification_department.department_classification,
gastos_base.product_id as project_id,
gastos_base.product_name as project_name,
gastos_base.transaction_detail,
relacion_cecos_dim2.service_country,
gastos_base.legal_entity_country,
gastos_base.legal_entity_name,
CASE
WHEN gastos_base.segment_name IS NULL OR gastos_base.segment_name = '' THEN 'Sin holding'
ELSE gastos_base.segment_name
END AS holding_segment,
relacion_cecos.sales_channel,
relacion_cecos.revenue_stream,
gastos_base.business_partner_id,
gastos_base.business_partner_name,
gastos_base.currency,
gastos_base.value_lc,
gastos_base.value_fx_accounting,
gastos_base.value_usd_accounting,
fx.Value as value_fx_bdg,
safe_divide(gastos_base.value_lc, fx.Value) as value_usd_bdg,
gastos_base.version,
gastos_base.subversion,
CAST(gastos_base.payment_date AS DATETIME) as payment_date,
gastos_base.source,
CASE
WHEN gastos_base.segment_name = 'ENTERPRISE' THEN 'Enterprise'
WHEN gastos_base.segment_name = 'SME' THEN 'SMB'
ELSE null END AS business_unit,
EXTRACT(MONTH FROM gastos_base.full_date) as month,
EXTRACT(YEAR FROM gastos_base.full_date) as year

FROM `btf-finance-sandbox.Expenses.expenses_base_netsuite` gastos_base
LEFT JOIN `btf-finance-sandbox.relaciones_netsuite.relaciones_cuenta` relacion_cuentas
ON gastos_base.accounting_account_id = relacion_cuentas.accounting_account_id

--AND UPPER(gastos_base.legal_entity_name) = UPPER(relacion_cuentas.legal_entity_name) #### Ya no es necesario

LEFT JOIN `btf-finance-sandbox.relaciones_netsuite.relaciones_area` relacion_cecos
ON UPPER(gastos_base.accounting_department_name) = UPPER(relacion_cecos.accounting_department_name)

--AND UPPER(gastos_base.legal_entity_name) = UPPER(relacion_cecos.legal_entity_name) #### Ya no es necesario

LEFT JOIN `btf-finance-sandbox.Relacion_Motores.relacion_ceco_dim2` relacion_cecos_dim2
ON UPPER(gastos_base.accounting_country_name) = UPPER(relacion_cecos_dim2.accounting_cost_center_dim2_name)

LEFT JOIN `btf-finance-sandbox.Relacion_Motores.department_classification` classification_department
ON UPPER(relacion_cecos.management_department) = UPPER(classification_department.management_department)

LEFT JOIN `btf-finance-sandbox.Budget.Currency_conversion_bdg` fx
ON CAST(EXTRACT(YEAR FROM gastos_base.full_date) as string) = fx.Year
AND UPPER(gastos_base.currency) = UPPER(fx.Currency)

order by management_department