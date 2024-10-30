with
    last_date as (
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
  WHEN lower(hs_company_size) = 'micro' THEN 'SME'
  WHEN lower(hs_company_size) = 'sme' THEN 'SME'
  WHEN lower(hs_company_size) = 'corporate' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'enterprise' THEN 'Enterprise'
  WHEN lower(hs_company_size) = 'big enterprise' THEN 'Enterprise'
  END AS bu,
  hs_number_of_employees,
  hs_closed_amount_in_company_currency,
  hs_deal_stage
        FROM
            `btf-source-of-truth.acquisition.deals` as ad
            inner join last_date as ld on ad.hs_deal_id = ld.hs_deal_id
            and ad.hs_last_modified_date = ld.last_date
        WHERE
            hs_closed_date >= '2024-01-01'
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
            AND upper(hs_deal_stage) like '%CLOSED WON%'

            ORDER by hs_closed_date),
            espana_1 AS (
            SELECT 
            closed_date_month,
            closed_date_year,
            hs_betterfly_country,
            bu,
            hs_number_of_employees,
            hs_closed_amount_in_company_currency
            from espana where bu is not null

            ORDER BY 
            closed_date_year, closed_date_month)
            SELECT * from espana_1