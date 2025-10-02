
SELECT DISTINCT
DATE_TRUNC(meeting_date, MONTH) as meeting_date_,
deal.betterfly_country,
COUNT(acq.deal_id),


FROM `btf-unified-data-platform.pdr_acquisition.meetings` acq
LEFT JOIN `btf-unified-data-platform.pdr_acquisition.deals` deal
ON acq.deal_id = deal.deal_id
WHERE
meeting_outcome = 'COMPLETED'
AND activity_type = 'First meeting (AE)'
AND betterfly_country in ('Chile', 'Mexico', 'Spain', 'China')


group by meeting_date_,
deal.betterfly_country



ORDER by meeting_date_ DESC