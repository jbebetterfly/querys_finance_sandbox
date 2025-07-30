-- Points Loaded, con campos corregidos y lógica aplicada a nacionalidad BO
WITH
   first_day_calendar AS (
       SELECT
           yearmonth
       FROM
           UNNEST(
               GENERATE_DATE_ARRAY(DATE('2023-01-01'), CURRENT_DATE(), INTERVAL 1 MONTH)
           ) AS yearmonth
   ),
   orders_companies AS (
       SELECT DISTINCT
           ord.document_id,
           ord.label,
           ord.companyid
       FROM
           `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS ord
   ),
   temp AS (
       SELECT
           gcl.createdAt,
           CAST(FORMAT_DATE('%Y%m', DATE_TRUNC(gcl.createdAt, MONTH)) AS INT64) AS `AnoMes Loaded`,
           CAST(JSON_VALUE(gcl.giftCards_member, '$.amount') AS INT64) AS amount,
           com.name AS companyNameApp,
           com_bo.nationalId AS companyNationalIdBO,
           com_bo.countryId AS companyNationalityBO,
           JSON_VALUE(gcl.giftCards_member, '$.nationalId') AS `user nationalId`,
           JSON_VALUE(gcl.giftCards_member, '$.email') AS user_email,
           bon.userId AS user_id,
           gcl.transactionId,
           CAST(
               COALESCE(
                   REGEXP_EXTRACT(gcl.transactionId, r'\d{13}'),
                   REGEXP_EXTRACT(gcl.transactionId, r'\d{6}')
               ) AS INT64
           ) AS `transactionId Limpio`,
           'points' AS type
       FROM
           first_day_calendar fdc
           LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_giftcards_loads_schema_btf_wallet_giftcards_loads_latest` AS gcl
               ON DATE(DATE_TRUNC(gcl.createdAt, MONTH)) = fdc.yearmonth
           LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_companies_schema_btf_wallet_companies_latest` AS com
               ON com.document_id = gcl.companyId
           LEFT JOIN orders_companies AS oc ON oc.label = gcl.transactionId
           LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.companies_schema_companies_latest` AS com_bo
               ON com_bo.document_id = oc.companyid
           LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_bonuses_schema_btf_wallet_bonuses_latest` AS bon 
               ON bon.document_id = JSON_VALUE(gcl.giftCards_member, '$.id')
       WHERE
           DATE(gcl.createdAt) >= DATE('2025-01-01')
           AND (
               com_bo.countryId = 'CL'
               OR (
                   (com_bo.countryId IS NULL OR com_bo.countryId = '')
                   AND (
                       REGEXP_CONTAINS(gcl.transactionId, r'OP')
                       OR REGEXP_CONTAINS(gcl.transactionId, r'CL-')
                   )
               )
           )
           AND JSON_VALUE(gcl.giftCards_member, '$.allowedGiftCardIds') IS NULL
           AND (
               REGEXP_CONTAINS(gcl.transactionId, r'OP\d+') = TRUE
               OR REGEXP_CONTAINS(gcl.transactionId, r'[MX|CO|CL|COP|PE]-\d+') = TRUE
           )
   )
SELECT
   temp.createdAt,
   temp.`AnoMes Loaded`,
   temp.amount,
   temp.companyNameApp,
   temp.companyNationalIdBO,
   CASE
       WHEN temp.companyNationalityBO = 'CL' THEN 'CL'
       WHEN (temp.companyNationalityBO IS NULL OR temp.companyNationalityBO = '')
            AND (STRPOS(temp.transactionId, 'OP2') > 0 OR STRPOS(temp.transactionId, 'CL-') > 0) THEN 'CL'
       ELSE 'OTRO'
   END AS companyNationalityBO_corregido,
   temp.`user nationalId`,
   temp.user_email,
   temp.user_id,
   temp.transactionId,
   temp.`transactionId Limpio`,
   temp.type,
   ord.paymentmethod,
   CASE
   WHEN STARTS_WITH(LOWER(temp.companyNameApp), 'betterfly')
        OR LOWER(temp.companyNameApp) = 'conecten' THEN 'SI'
   ELSE 'NO'
END AS empresa_btf,
CONCAT(temp.transactionId, temp.`user nationalId`) AS llave_voided_loadedid_userid,
CASE
   WHEN STRPOS(LOWER(temp.transactionId), '.') > 0
        OR STRPOS(LOWER(temp.transactionId), 'bonus') > 0 THEN 'SI'
   ELSE 'NO'
END AS es_movimiento_reloaded
FROM
   temp
   LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS ord
       ON ord.label = temp.transactionId
WHERE
   -- 🧹 Filtramos solo las filas con nacionalidad CL según lógica corregida
   CASE
       WHEN temp.companyNationalityBO = 'CL' THEN 'CL'
       WHEN (temp.companyNationalityBO IS NULL OR temp.companyNationalityBO = '')
            AND (STRPOS(temp.transactionId, 'OP2') > 0 OR STRPOS(temp.transactionId, 'CL-') > 0) THEN 'CL'
       ELSE 'OTRO'
   END = 'CL'
ORDER BY
   `AnoMes Loaded` ASC
09:46
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