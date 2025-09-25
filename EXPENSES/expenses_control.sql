WITH base as (SELECT DISTINCT 
  year,
  month,
  service_country,
  department_classification,
  CASE
    WHEN upper(source) LIKE '%CONECTEN%'
      THEN 'Rewards'
    ELSE management_department
    END
    AS management_department,
  CASE
    WHEN management_account_name IN ('Payroll', 'Outsourcing', 'Professional Services', 'Severance', 'Benefits')
    THEN 'Payroll'
    WHEN management_account_name in ('Travels', 'Digital Tools', 'Events', 'Paid Media', 'Merchandising', 'Rent', 'Office Supplies', 'Onboarding', 'Retention', 'Engagement', 'Others', 'Memberships', 'Free Month')
    THEN 'Non-Payroll'
    ELSE 'check'
    END AS agrupacion_franpoblete,
  CASE
   WHEN business_partner_name like '%ALLHANDS%'
     THEN 'All Hands'
   ELSE management_account_name
   end as management_account_name,
CASE
    WHEN management_account_name = 'Professional Services'
    AND business_partner_name in (
'LYON CONSULTORIA E ADMINISTRACAO',
'PELOSI & SCHIMMELPFENG',
'SERVICIOS ESPECIALES CONTABLES SAC',
'AAB ASOCIADOS',
'BHR ENW MEXICO GROUP',
'DCC LIMITADA',
'DESARROLLO DE CONTABILIDADES Y CONSULTORIAS',
'EEGUIA',
'FBISPO',
'FOXTROT INNOVACIONES SPA',
'IMPULSO FISCAL Y CONTABLE SAS',
'INTERIA CARTERA EMPRESARIAL S.A.S',
'KPMG',
'MIRA MEGIAS CONSULTING, SLU',
'MONTOYA TROYA MIREYA',
'PWC',
'REVICOM SAS',
'VPS PERU SAC',
'ZUKALO SERVICIO SISTEMA CONTABLE',
'Albagli Zaliasnik',
'DEVESA SERVICIOS JURÍDICOS Y FISCALES, S.L.P.',
'ECIJA',
'FELSBERG E PEDRETTI ADVOGADOS',
'GESCON CONTABIL E TRIBUTARIA LTDA',
'Gunderson Dettmer',
'HOLLAND & KNIGHT COLOMBIA S.A.S',
'JARRY IP SPA',
'PINO ELIZALDE ABOGADOS',
'PYMELEGAL, SL',
'REBAZA & ALCAZAR ABOGADOS SCRL',
'BENDITA IMAGEM',
'MIRLO CLUB SPA',
'TUANY DE PAULA CHAGAS',
'V3RTICE MARQUETING',
'VMICA CONSULTORES',
'WINCLAP',
'WOWFACTOR',
'CANAL DE COMPRAS SAS',
'Catalina Moreno Consultores EIRL',
'EUROPREVEN SERV.PRL SL',
'FIRST JOB',
'SOS GROUP',
'TMF',
'ADOK CERTIFICACION, SL',
'INSTITUTO NORMATIVO, SL',
'DANRESA SECURITY',
'FIRMA BRAND',
'RUNA HR',
'DEEL',
'COMISION PAYCHEX',
'IBAÑEZ, FERNANDEZ DEL CASTILLO, MALAGON'
)
THEN 'Outsourcing'
    WHEN management_account_name = 'Professional Services'
    AND business_partner_name in (
'ADECCO',
'Carolina Ramirez',
'MANPOWER',
'ELIANA IÑIGUEZ',
'Fernando Cea Montenegro',
'Jorge Vera',
'RODRIGO GABRIEL TORRES',
'Carolina Carril Rubio',
'STK ATENCION DE LLAMADAS, SL',
'BETWEEN TECHNOLOGY SL',
'Cosmico Find your Space SL',
'GEEK CASTLE',
'APIUX TECNOLOGÍA ESPAÑA SL',
'Pepiln Spa',
'WORKANA',
'SOHO',
'María Jesús Ariztia Moreno',
'Tailor Hub',
'Joaquin Espinosa EIRL',
'María Jose Saul Drapela',
'DICA POWER OÜ'
      )
    THEN 'Payroll Externo'
   WHEN business_partner_name like '%ALLHANDS%'
     THEN 'All Hands'
   ELSE management_account_name
  END as subagrupacion_analisis,
  business_partner_name,
  holding_segment,
  transaction_detail,
  sum(value_usd_bdg) as value_usd,
  source,
  revenue_stream

from `btf-finance-sandbox.Expenses.Cubo_Financiero`


where year >= 2025
and mgmt_account_classification = 'P&L'
and mgmt_account_subclasification = 'SG&A'
AND version = 'REAL' AND subversion = 'REAL'
and management_account_name NOT IN ('Non-Operational','Uncollectible Accounts')
AND transaction_detail NOT IN (
  'Anulacion asiento volteado 175812'
  )




GROUP BY 
  year,
  month,
  service_country,
  department_classification,
  management_department,
  agrupacion_franpoblete,
  management_account_name,
  subagrupacion_analisis,
  business_partner_name,
  holding_segment,
  transaction_detail,
  source,
  revenue_stream


ORDER BY 
year,
month,
management_department,
business_partner_name,
value_usd ASC)

SELECT * from base where agrupacion_franpoblete != 'check'