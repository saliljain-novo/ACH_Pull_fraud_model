
WITH
PULL_FUND_REQUEST AS (
    SELECT *
    FROM "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."PULL_FUNDS_REQUESTS"
)

,PULL_FUND_REQUEST_EVENTS AS (
    SELECT *
    FROM "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."PULL_FUNDS_REQUESTS_EVENTS"
)

,EXTERNAL_ACCOUNT AS (
    SELECT *
    FROM "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."EXTERNAL_ACCOUNTS"
)

,PFR_TAGGING AS (
    SELECT
        A.ID,
        a.STATUS,
        EA.BANK_NAME,
        CASE
        WHEN A.STATUS = 'returned' THEN 1
        WHEN b.event = 'rejected' AND (b.description ilike ('%Doubtful Collectibility%')
                                       or b.description ilike ('%Account under review%')) THEN 1
        WHEN A.STATUS = 'completed' THEN 0
        END AS TARGET,
        description
    FROM PULL_FUND_REQUEST AS A
    LEFT JOIN PULL_FUND_REQUEST_EVENTS AS B ON A.ID = B.PULL_FUNDS_REQUEST_ID AND b.event = 'rejected'
    LEFT JOIN EXTERNAL_ACCOUNT AS EA ON A.EXTERNAL_ACCOUNT_ID = EA.ID
    WHERE A.CREATED_AT >= DATEADD('MONTH',-3, CURRENT_DATE)
    AND A.STATUS IN ('completed','returned','rejected')
)


SELECT
    BANK_NAME,
    SUM(TARGET)/COUNT(TARGET) AS RETURN_RATE,
    CASE
        WHEN return_rate >= 0.08 THEN 3
        WHEN return_rate >= 0.03 THEN 2
        ELSE 1
    END AS BANK_RISK
    
FROM
    (SELECT
        ID,
        BANK_NAME,
        MAX(TARGET) AS TARGET
    FROM PFR_TAGGING
     where target is not null
    GROUP BY 1,2)
GROUP BY 1