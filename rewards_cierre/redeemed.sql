-- Redeemed points report - Solo datos desde 2025
-- Última actualización: 2025-05-08

WITH
    first_day_calendar AS (
        SELECT yearmonth
        FROM UNNEST(
            GENERATE_DATE_ARRAY(DATE('2025-01-01'), CURRENT_DATE(), INTERVAL 1 MONTH)
        ) AS yearmonth
    ),

    redemptions_by_user AS (
        SELECT
            ugc.createdAt,
            ugc.document_id,
            ugc.code,
            ugc.giftCardId,
            ugc.userId,
            CAST(JSON_VALUE(ugc.contributions_member, '$.totalTakenAmount') AS FLOAT64) AS redeemed_amount,
            JSON_VALUE(ugc.contributions_member, '$.bonusId') AS bonus_id,
            ugc.amount AS total_amount
        FROM
            first_day_calendar fdc
        LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_users_giftcards_schema_btf_wallet_users_giftcards_latest` AS ugc 
            ON DATE(DATE_TRUNC(ugc.createdAt, MONTH)) = fdc.yearmonth
        WHERE
            ugc.voidedAt IS NULL
            AND ugc.status NOT IN ('voided', 'reversed')
            AND ugc.userGiftCardRedeemRequestId IS NOT NULL
            AND ugc.contributions_index IS NOT NULL
    ),

    giftcards_loads_info AS (
        SELECT DISTINCT
            gcl.document_id,
            gcl.transactionId,
            gcl.createdAt,
            gcl.companyId,
            ord.paymentmethod,
            com.name AS companyNameApp,
            com_bo.nationalId AS companyNationalIdBO,
            com_bo.countryId AS companyNationalityBO
        FROM
            `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_giftcards_loads_schema_btf_wallet_giftcards_loads_latest` AS gcl
        LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.orders_schema_orders_latest` AS ord 
            ON ord.label = SPLIT(gcl.transactionId, '.')[OFFSET(0)]
        LEFT JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_companies_schema_btf_wallet_companies_latest` AS com 
            ON com.document_id = gcl.companyId
        LEFT JOIN `btf-wallet-backoffice-prod.fs_transaction_data.companies_schema_companies_latest` AS com_bo 
            ON com_bo.document_id = ord.companyid
    ),

    bonuses_info AS (
        SELECT DISTINCT
            bon.document_id,
            bon.giftCardsLoadId,
            bon.userId
        FROM
            `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_bonuses_schema_btf_wallet_bonuses_latest` AS bon
        WHERE
            bon.giftCardsLoadId IS NOT NULL
    )

SELECT
    DATETIME(rgcu.createdAt, 'America/Santiago') AS `Redeemed createdAt`,
    CAST(SUBSTR(CAST(DATETIME(rgcu.createdAt, 'America/Santiago') AS STRING), 1, 4) ||
         SUBSTR(CAST(DATETIME(rgcu.createdAt, 'America/Santiago') AS STRING), 6, 2) AS INT64) AS `AnoMes Redeemed`,
    rgcu.total_amount AS userGiftCardAmount,
    CAST(rgcu.redeemed_amount AS INT64) AS bonusLoadRedeemedContributionAmount,
    rgcu.giftCardId,
    rgcu.code,
    us.document_id AS user_id,
    us.nationalId AS user_nationalId,
    CASE 
        WHEN REGEXP_CONTAINS(gcl.transactionId, r'OP\d+') = TRUE 
            THEN SPLIT(SPLIT(gcl.transactionId, '.')[OFFSET(0)], ',')[OFFSET(0)]
        ELSE gcl.transactionId 
    END AS bonusLoadTransactionId,
    CAST(
        COALESCE(
            REGEXP_EXTRACT(gcl.transactionId, r'\d{13}'),
            REGEXP_EXTRACT(gcl.transactionId, r'\d{6}')
        ) AS INT64
    ) AS `bonusLoadTransactionId Limpio`,
    CAST(SUBSTR(CAST(DATETIME(gcl.createdAt, 'America/Santiago') AS STRING), 1, 4) ||
         SUBSTR(CAST(DATETIME(gcl.createdAt, 'America/Santiago') AS STRING), 6, 2) AS INT64) AS `AnoMes Loaded`,
    gcl.companyNameApp,
    gcl.companyNationalIdBO,
    CASE
        WHEN STARTS_WITH(LOWER(gcl.companyNameApp), 'betterfly') 
             OR LOWER(gcl.companyNameApp) = 'conecten' THEN 'SI'
        ELSE 'NO'
    END AS empresa_btf,
    CONCAT(
        CASE 
            WHEN REGEXP_CONTAINS(gcl.transactionId, r'OP\d+') = TRUE 
                THEN SPLIT(SPLIT(gcl.transactionId, '.')[OFFSET(0)], ',')[OFFSET(0)]
            ELSE gcl.transactionId 
        END,
        us.document_id
    ) AS llave_bonus_user

FROM
    redemptions_by_user AS rgcu
INNER JOIN bonuses_info AS boni 
    ON boni.document_id = rgcu.bonus_id AND rgcu.userId = boni.userId
INNER JOIN giftcards_loads_info AS gcl 
    ON gcl.document_id = boni.giftCardsLoadId
INNER JOIN `btf-wallet-prod.btf_wallet_prod_transaction_data.btf_wallet_users_schema_btf_wallet_users_latest` AS us 
    ON us.document_id = rgcu.userId
WHERE
    (REGEXP_CONTAINS(gcl.transactionId, r'OP\d+') = TRUE 
     OR REGEXP_CONTAINS(gcl.transactionId, r'[MX|COP|CL|PE]-\d+') = TRUE)
    AND us.nationality = 'CL'
    AND DATE(DATETIME(rgcu.createdAt, 'America/Santiago')) >= DATE('2025-01-01')
    AND (
      CASE
        WHEN gcl.companyNationalityBO = 'CL' THEN 'CL'
        WHEN (gcl.companyNationalityBO IS NULL OR gcl.companyNationalityBO = '')
             AND (STRPOS(gcl.transactionId, 'OP2') > 0 OR STRPOS(gcl.transactionId, 'CL-') > 0) THEN 'CL'
        ELSE 'OTRO'
      END
    ) = 'CL'

ORDER BY
    rgcu.createdAt,
    rgcu.document_id