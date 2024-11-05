with 
global_pre_check as (
  SELECT DISTINCT
  EXTRACT(MONTH FROM hs_closed_date) AS closed_date_month,
  EXTRACT(YEAR FROM hs_closed_date) AS closed_date_year,
  hs_betterfly_country,
  CASE
  WHEN lower(hs_company_size) = 'micro' THEN 'SMB'
  WHEN lower(hs_company_size) = 'sme' THEN 'SMB'
  WHEN lower(hs_company_size) = 'corporate' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'enterprise' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'big enterprise' THEN 'Enterprise'
  END AS bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency,
  hs_deal_id
  
  FROM `btf-source-of-truth.acquisition.deals`

  WHERE hs_closed_date >= '2023-11-01'
  AND hs_deal_Stage = 'Cierres ganados'
  --AND UPPER(hs_company_name) NOT LIKE '%COLSUBSIDIO%'
  AND (lower(hs_product_modules) not in (
       'gift cards',
       'gift cards 🇨🇱',
       'flexben',
       'flexoh',
       'seu vale',
       'flexoh - poliza de seguro',
       'poliza de seguro'
      )
    OR hs_product_modules IS null)
    AND lower(hs_deal_name) NOT LIKE '%test%'
    AND lower(hs_deal_name) NOT LIKE '%prueba%'
    AND lower(hs_deal_name) NOT LIKE '%wallet%'
    AND (lower(hs_development_type) NOT LIKE '%piloto%' or hs_development_type is null)
    AND (lower(hs_development_type) NOT LIKE '%reactivate%' or hs_development_type is null)
    AND (lower(hs_development_type) NOT LIKE '%retention%' or hs_development_type is null)
    AND (lower(hs_value_proposition) NOT LIKE '%customer_engagement%' or hs_value_proposition is null)
    AND (lower(hs_partnership_demand_name) NOT LIKE '%colsubsidio%' or hs_partnership_demand_name is null)
    and lower(hs_deal_pipeline) in ('pipeline de ventas','pipeline enterprise')
    and lower(hs_value_proposition) not in ('affinity') 
    and lower(hs_betterfly_country) not in ('spain')
  
  ORDER BY closed_date_month,
  closed_date_year,
  hs_betterfly_country,
  bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency
),

global_distinct as 
(SELECT 
  closed_date_month,
  closed_date_year,
  hs_betterfly_country,
  bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency
  
  FROM global_pre_check

  ORDER BY closed_date_month,
  closed_date_year,
  hs_betterfly_country,
  bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency),

    last_date_esp as (
        select
            hs_deal_id,
            max(hs_last_modified_date) as last_date
        from
            `btf-source-of-truth.acquisition.deals`
        group by
            hs_deal_id
    ),
    espana as (
    SELECT
  EXTRACT(MONTH FROM hs_closed_date) AS closed_date_month,
  EXTRACT(YEAR FROM hs_closed_date) AS closed_date_year,
  hs_betterfly_country,
  CASE
  WHEN lower(hs_company_size) = 'micro' THEN 'SMB'
  WHEN lower(hs_company_size) = 'sme' THEN 'SMB'
  WHEN lower(hs_company_size) = 'corporate' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'enterprise' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'big enterprise' THEN 'Enterprise'
  END AS bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency
        FROM
            `btf-source-of-truth.acquisition.deals` as ad
            inner join last_date_esp as ld on ad.hs_deal_id = ld.hs_deal_id
            AND ad.hs_last_modified_date = ld.last_date
        WHERE
            hs_closed_date >= '2023-11-01'
            and hs_created_date <= current_date() -1
            and LOWER(hs_deal_pipeline) LIKE '%flexoh%'
            and (lower(hs_value_proposition) not in ('affinity') or lower(hs_value_proposition) is null)
            and lower(hs_betterfly_country) in ('spain')
            and lower(hs_deal_name) not like '%test%'
            and lower(hs_deal_name) not like '%prueba%'
            and lower(hs_deal_name) not like '%wallet%'
            and (lower(hs_development_type) not like '%piloto%' or hs_development_type is null)
            AND (lower(hs_development_type) NOT LIKE '%ong%' OR hs_development_type IS NULL)
            and (lower(hs_development_type) not like '%reactivate%'or hs_development_type is null)
            and (lower(hs_development_type) not like '%retention%'or hs_development_type is null)
            and (lower(hs_value_proposition) not like '%customer_engagement%'or hs_value_proposition is null)
            and (lower(hs_partnership_demand_name) not like '%colsubsidio%' or hs_partnership_demand_name is null)
            AND UPPER(hs_deal_stage) like '%CLOSED WON%'

            ORDER by hs_closed_date),
    espana_1 AS (
            SELECT DISTINCT
            closed_date_month,
            closed_date_year,
            hs_betterfly_country,
            bu,
            hs_number_of_employees,
            hs_closed_amount_in_company_currency
            from espana where bu is not null

            ORDER BY 
            closed_date_year, closed_date_month),
          
          
          final_final as (
          
SELECT * from global_distinct
UNION ALL
SELECT * from espana_1

ORDER BY 
closed_date_year,
closed_date_month,
hs_betterfly_country)

SELECT * FROM final_final WHERE hs_closed_amount_in_company_currency > 0 AND hs_number_of_employees > 0