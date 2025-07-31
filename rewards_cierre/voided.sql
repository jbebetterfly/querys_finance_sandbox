Voided
-- Voided points, con campos ordenados, bonusLoadTransactionId Limpio agregado y lógica de país aplicada
WITH
    first_day_calendar AS (
        SELECT
            yearmonth
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY(
                    DATE('2023-06-01'),
                    CURRENT_DATE(),
                    INTERVAL 1 MONTH
                )
            ) AS yearmonth
    ),
    bonuses_info AS (
        SELECT
            bon.document_id,
            bon.giftCardsLoadId,
            bon.amount,
            bon.createdAt,
            bon.nationality,
            bon.nationalId,
            bon.userId,
            bon.voidedat,
            bon.email
        FROM
            first_day_calendar fdc
            LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_bonuses_schema_btf_wallet_bonuses_latest` AS bon 
            ON DATE(DATE_TRUNC(bon.createdAt, MONTH)) = fdc.yearmonth
        WHERE
            bon.giftCardsLoadId IS NOT NULL
            AND bon.allowedGiftCardIds = false
            AND bon.status = 'voided'
    ),
    giftcards_loads_info AS (
        SELECT DISTINCT
            gcl.document_id,
            gcl.transactionId,
            gcl.companyId
        FROM
            `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_giftcards_loads_schema_btf_wallet_giftcards_loads_latest` AS gcl
    ),
    temp AS (
        SELECT
            bi.createdAt AS bonusLoadCreatedAt,
            bi.voidedat AS voidedAt,
            CAST(FORMAT_DATE('%Y%m', DATE(bi.voidedat)) AS INT64) AS `AnoMes Voided`,
            CAST(FORMAT_DATE('%Y%m', DATE(bi.createdAt)) AS INT64) AS `AnoMes Loaded`,
            bi.amount,
            bi.email,
            bi.nationalId AS userNationalId,
            com.name AS companyNameApp,
            com_bo.nationalId AS companyNationalIdBO,
            com_bo.countryId AS companyNationalityBO,
            gcl.transactionId AS bonusLoadTransactionId,
            ord.paymentmethod
        FROM
            bonuses_info AS bi
            INNER JOIN giftcards_loads_info AS gcl ON gcl.document_id = bi.giftCardsLoadId
            LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_companies_schema_btf_wallet_companies_latest` AS com 
                ON com.document_id = gcl.companyId
            LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS ord 
                ON ord.label = SPLIT(gcl.transactionId, '.')[OFFSET(0)]
            LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.companies_schema_companies_latest` AS com_bo 
                ON com_bo.document_id = ord.companyid
    )
SELECT
    temp.voidedAt,
    temp.`AnoMes Voided`,
    temp.bonusLoadCreatedAt,
    temp.`AnoMes Loaded`,
    temp.amount,
    temp.email,
    temp.userNationalId,
    temp.companyNameApp,
    temp.companyNationalIdBO,
    -- Lógica para nacionalidad corregida
    CASE
        WHEN temp.companyNationalityBO = 'CL' THEN 'CL'
        WHEN (temp.companyNationalityBO IS NULL OR temp.companyNationalityBO = '')
             AND (STRPOS(temp.bonusLoadTransactionId, 'OP2') > 0 OR STRPOS(temp.bonusLoadTransactionId, 'CL-') > 0) THEN 'CL'
        ELSE 'OTRO'
    END AS companyNationalityBO_corregido,
    temp.bonusLoadTransactionId,
    CAST(
        COALESCE(
            REGEXP_EXTRACT(temp.bonusLoadTransactionId, r'\d{13}'),
            REGEXP_EXTRACT(temp.bonusLoadTransactionId, r'\d{6}')
        ) AS INT64
    ) AS `bonusLoadTransactionId Limpio`,
    temp.paymentmethod,
    CASE
        WHEN STARTS_WITH(LOWER(temp.companyNameApp), 'betterfly') 
             OR LOWER(temp.companyNameApp) = 'conecten' THEN 'SI'
        ELSE 'NO'
    END AS empresa_btf
FROM
    temp
WHERE
    CASE
        WHEN temp.companyNationalityBO = 'CL' THEN 'CL'
        WHEN (temp.companyNationalityBO IS NULL OR temp.companyNationalityBO = '')
             AND (STRPOS(temp.bonusLoadTransactionId, 'OP2') > 0 OR STRPOS(temp.bonusLoadTransactionId, 'CL-') > 0) THEN 'CL'
        ELSE 'OTRO'
    END = 'CL'
ORDER BY
    `AnoMes Voided` ASC