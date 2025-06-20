{{
    config(
        materialized='table',
        tags=['dc_model_input']
    )
}}
    
WITH
BUSINESSES AS (
    SELECT *
    FROM {{ ref('businesses') }}
)

,APPLICATIONS AS (
    SELECT *
    FROM {{ ref('applications') }}
)

,DAILY_BALANCE AS (
    SELECT *
    FROM {{ ref('balances_daily') }}
)

,PULL_FUND_REQUEST AS (
    SELECT *
    FROM {{ ref('stg_api__pull_funds_requests') }}
)

,TRANSACTIONS AS (
    SELECT *
    FROM {{ ref('transactions') }}
)

,BALANCE_METRICS AS (
    SELECT
        BUSINESS_ID,
        AVG(DAY_END_BALANCE) AS avg_running_balance_past30d,
        MAX_BY(DAY_END_BALANCE,DATE) AS rb_at_deposit,
        STDDEV(DAY_END_BALANCE) AS stddev_running_balance_past30d,
        COUNT_IF(DAY_END_BALANCE < 0) AS od_count_past30d,
        COUNT_IF(DAY_END_BALANCE = 0) AS zero_balance_count_past30d
    FROM DAILY_BALANCE
    WHERE DATE >= DATEADD('DAY',-30,CURRENT_DATE)
    GROUP BY 1
)

,BUSINESS_METRICS AS (
    SELECT
        BUSINESSES.BUSINESS_ID,
        DATEDIFF('MONTH',BUSINESSES.BUSINESS_CREATED_AT,CURRENT_TIMESTAMP) AS MOB,
        APPLICATIONS.EIN_SSN
    FROM BUSINESSES
    LEFT JOIN APPLICATIONS ON BUSINESSES.APPLICATION_ID = APPLICATIONS.APPLICATION_ID
)

,HISTORIC_PFR AS (
    SELECT
        ID,
        TRANSACTION_ID,
        BUSINESS_ID,
        AMOUNT,
        CREATED_AT,
        UPDATED_AT,
        STATUS
        
    FROM PULL_FUND_REQUEST
    WHERE CREATED_AT >= DATEADD('DAY',-30,CURRENT_DATE)
)

,PAST_30_DAY_AVG_ACH AS (
    SELECT
        BUSINESS_ID,
        AVG(AMOUNT) AS past30d_avg_ach_amount,
        COUNT(ID) as past30d_ach_count,
        AVG(IFF(STATUS = 'rejected',AMOUNT,NULL)) AS rejected_past30d_avg_ach_amount,
        COUNT(IFF(STATUS = 'rejected',ID,NULL)) AS past30d_rejected_ach,
        AVG(IFF(STATUS = 'return',AMOUNT,NULL)) AS returned_past30d_avg_ach_amount,
        COUNT(IFF(STATUS = 'return',ID,NULL)) AS past30d_returned_ach
    FROM HISTORIC_PFR
    GROUP BY 1
)

,PFR_HISTORIC_TRANSACTION AS (
    SELECT
        T.BUSINESS_ID,
        T.CATEGORY_ID,
        T.AMOUNT,
        T.DESCRIPTION,
        T.TYPE,
        T.MEDIUM,
        T.DERIVED_MEDIUM,
        T.VVC_MEDIUM,
        T.STATUS,
        T.CREATED_DATE,
        T.TRANSACTION_DATE,
        T.TIMESTAMP,
        T.RUNNING_BALANCE,
        T.MERCHANT_CATEGORY_CODE,
        DATEDIFF(DAY,T.TRANSACTION_DATE,CURRENT_DATE) AS DAYS_SINCE,
        CASE WHEN T.DERIVED_MEDIUM = 'ACH' AND T.TYPE = 'credit' THEN 1 ELSE 0 END AS IS_ACH_C,
        CASE WHEN T.DERIVED_MEDIUM = 'ACH' AND T.TYPE = 'debit' THEN 1 ELSE 0 END AS IS_ACH_D,
        CASE WHEN T.TYPE = 'credit' then 1 else 0 end as IS_CREDIT,
        CASE WHEN T.TYPE = 'debit' then 1 else 0 end as IS_DEBIT,
        CASE WHEN T.MEDIUM = 'POS Withdrawal' then 1 else 0 end as IS_CARD_TXN
    
    
    FROM TRANSACTIONS AS T
    WHERE T.TRANSACTION_DATE >= DATEADD('DAY',-30,CURRENT_DATE)
)

,pfr_past_txn_agg_past30d AS (
    SELECT
        BUSINESS_ID,
    
        -- Card transactions
        SUM(is_card_txn) AS card_txn_count_past30d,
        MEDIAN(CASE WHEN is_card_txn = 1 THEN ABS(amount) ELSE NULL END) AS card_txn_median_past30d,
        AVG(is_card_txn * ABS(amount)) AS card_txn_avg_past30d,
    
        -- ACH credits
        SUM(is_ach_c) AS ach_c_count_past30d,
        MEDIAN(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_median_past30d,
        AVG(is_ach_c * ABS(amount)) AS ach_c_avg_past30d,
        STDDEV(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_std_past30d,
    
        -- Debit vs Credit ratio
        DIV0NULL(SUM(is_debit), SUM(is_credit)) AS debit_by_credit_past_30d,
        SUM(is_debit) AS debit_txn_count_past30d,
    
        -- ACH debits
        MEDIAN(CASE WHEN is_ach_d = 1 THEN ABS(amount) ELSE NULL END) AS ach_d_median_past30d,
        AVG(is_ach_d * ABS(amount)) AS ach_d_avg_past30d
    
    FROM PFR_HISTORIC_TRANSACTION
    GROUP BY 1
)

,pfr_past_txn_agg_past10d AS (
    SELECT
        BUSINESS_ID,
    
        -- Card transactions
        SUM(is_card_txn) AS card_txn_count_past10d,
        MEDIAN(CASE WHEN is_card_txn = 1 THEN ABS(amount) ELSE NULL END) AS card_txn_median_past10d,
        AVG(is_card_txn * ABS(amount)) AS card_txn_avg_past10d,
    
        -- ACH credits
        SUM(is_ach_c) AS ach_c_count_past10d,
        MEDIAN(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_median_past10d,
        AVG(is_ach_c * ABS(amount)) AS ach_c_avg_past10d,
        STDDEV(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_std_past10d,
    
        -- Debit vs Credit ratio
        DIV0NULL(SUM(is_debit), SUM(is_credit)) AS debit_by_credit_past_10d,
        SUM(is_debit) AS debit_txn_count_past10d,
    
        -- ACH debits
        MEDIAN(CASE WHEN is_ach_d = 1 THEN ABS(amount) ELSE NULL END) AS ach_d_median_past10d,
        AVG(is_ach_d * ABS(amount)) AS ach_d_avg_past10d
    
    FROM PFR_HISTORIC_TRANSACTION
    WHERE DAYS_SINCE <= 10
    GROUP BY 1
)

,pfr_past_txn_agg_past2d AS (
    SELECT
        BUSINESS_ID,
    
        -- Card transactions
        SUM(is_card_txn) AS card_txn_count_past2d,
        MEDIAN(CASE WHEN is_card_txn = 1 THEN ABS(amount) ELSE NULL END) AS card_txn_median_past2d,
        AVG(is_card_txn * ABS(amount)) AS card_txn_avg_past2d,
    
        -- ACH credits
        SUM(is_ach_c) AS ach_c_count_past2d,
        MEDIAN(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_median_past2d,
        AVG(is_ach_c * ABS(amount)) AS ach_c_avg_past2d,
        STDDEV(CASE WHEN is_ach_c = 1 THEN ABS(amount) ELSE NULL END) AS ach_c_std_past2d,
    
        -- Debit vs Credit ratio
        DIV0NULL(SUM(is_debit), SUM(is_credit)) AS debit_by_credit_past_2d,
        SUM(is_debit) AS debit_txn_count_past2d,
    
        -- ACH debits
        MEDIAN(CASE WHEN is_ach_d = 1 THEN ABS(amount) ELSE NULL END) AS ach_d_median_past2d,
        AVG(is_ach_d * ABS(amount)) AS ach_d_avg_past2d
    
    FROM PFR_HISTORIC_TRANSACTION
    WHERE DAYS_SINCE <= 2
    GROUP BY 1
)

,BUSINESS_TRANSACTION_METRICS AS (
    SELECT
        BUM.BUSINESS_ID,
        BUM.MOB,
        BUM.EIN_SSN,

        COALESCE(PACH.PAST30D_AVG_ACH_AMOUNT, 0) AS PAST30D_AVG_ACH_AMOUNT,
        COALESCE(PACH.REJECTED_PAST30D_AVG_ACH_AMOUNT, 0) AS REJECTED_PAST30D_AVG_ACH_AMOUNT,
        COALESCE(PACH.RETURNED_PAST30D_AVG_ACH_AMOUNT, 0) AS RETURNED_PAST30D_AVG_ACH_AMOUNT,
        COALESCE(PACH.PAST30D_ACH_COUNT, 0) AS PAST30D_ACH_COUNT,
        COALESCE(PACH.PAST30D_RETURNED_ACH, 0) AS PAST30D_RETURNED_ACH,
        COALESCE(PACH.PAST30D_REJECTED_ACH, 0) AS PAST30D_REJECTED_ACH,
        
        COALESCE(BAM.AVG_RUNNING_BALANCE_PAST30D,0) AS AVG_RUNNING_BALANCE_PAST30D,
        COALESCE(BAM.RB_AT_DEPOSIT,0) AS RB_AT_DEPOSIT,
        COALESCE(BAM.STDDEV_RUNNING_BALANCE_PAST30D,0) AS STDDEV_RUNNING_BALANCE_PAST30D,
        COALESCE(BAM.OD_COUNT_PAST30D,0) AS OD_COUNT_PAST30D,
        COALESCE(BAM.ZERO_BALANCE_COUNT_PAST30D,0) AS ZERO_BALANCE_COUNT_PAST30D,
    
        COALESCE(PFR_2D.card_txn_count_past2d,0) AS card_txn_count_past2d,
        COALESCE(PFR_2D.card_txn_median_past2d,0) AS card_txn_median_past2d,
        COALESCE(PFR_2D.card_txn_avg_past2d,0) AS card_txn_avg_past2d,
        COALESCE(PFR_2D.ach_c_count_past2d,0) AS ach_c_count_past2d,
        COALESCE(PFR_2D.ach_c_median_past2d,0) AS ach_c_median_past2d,
        COALESCE(PFR_2D.ach_c_avg_past2d,0) AS ach_c_avg_past2d,
        COALESCE(PFR_2D.ach_c_std_past2d,0) AS ach_c_std_past2d,
        COALESCE(PFR_2D.debit_by_credit_past_2d,0) AS debit_by_credit_past_2d,
        COALESCE(PFR_2D.debit_txn_count_past2d,0) AS debit_txn_count_past2d,
        COALESCE(PFR_2D.ach_d_median_past2d,0) AS ach_d_median_past2d,
        COALESCE(PFR_2D.ach_d_avg_past2d,0) AS ach_d_avg_past2d,
        
        COALESCE(PFR_10D.card_txn_count_past10d,0) AS card_txn_count_past10d,
        COALESCE(PFR_10D.card_txn_median_past10d,0) AS card_txn_median_past10d,
        COALESCE(PFR_10D.card_txn_avg_past10d,0) AS card_txn_avg_past10d,
        COALESCE(PFR_10D.ach_c_count_past10d,0) AS ach_c_count_past10d,
        COALESCE(PFR_10D.ach_c_median_past10d,0) AS ach_c_median_past10d,
        COALESCE(PFR_10D.ach_c_avg_past10d,0) AS ach_c_avg_past10d,
        COALESCE(PFR_10D.ach_c_std_past10d,0) AS ach_c_std_past10d,
        COALESCE(PFR_10D.debit_by_credit_past_10d,0) AS debit_by_credit_past_10d,
        COALESCE(PFR_10D.debit_txn_count_past10d,0) AS debit_txn_count_past10d,
        COALESCE(PFR_10D.ach_d_median_past10d,0) AS ach_d_median_past10d,
        COALESCE(PFR_10D.ach_d_avg_past10d,0) AS ach_d_avg_past10d,
        
        COALESCE(PFR_30D.card_txn_count_past30d,0) AS card_txn_count_past30d,
        COALESCE(PFR_30D.card_txn_median_past30d,0) AS card_txn_median_past30d,
        COALESCE(PFR_30D.card_txn_avg_past30d,0) AS card_txn_avg_past30d,
        COALESCE(PFR_30D.ach_c_count_past30d,0) AS ach_c_count_past30d,
        COALESCE(PFR_30D.ach_c_median_past30d,0) AS ach_c_median_past30d,
        COALESCE(PFR_30D.ach_c_avg_past30d,0) AS ach_c_avg_past30d,
        COALESCE(PFR_30D.ach_c_std_past30d,0) AS ach_c_std_past30d,
        COALESCE(PFR_30D.debit_by_credit_past_30d,0) AS debit_by_credit_past_30d,
        COALESCE(PFR_30D.debit_txn_count_past30d,0) AS debit_txn_count_past30d,
        COALESCE(PFR_30D.ach_d_median_past30d,0) AS ach_d_median_past30d,
        COALESCE(PFR_30D.ach_d_avg_past30d,0) AS ach_d_avg_past30d,
    
        DIV0NULL(PFR_10D.ach_c_count_past10d,PFR_30D.ach_c_count_past30d) AS ach_c_count_past10by30d,
        DIV0NULL(PFR_10D.debit_txn_count_past10d,PFR_30D.debit_txn_count_past30d) AS debit_txn_count_past10by30d,
        DIV0NULL(PFR_10D.ach_c_median_past10d,PFR_30D.ach_c_median_past30d) AS ach_c_median_past10by30d,
        DIV0NULL(PFR_10D.card_txn_median_past10d,PFR_30D.card_txn_median_past30d) AS card_txn_median_past10by30d,
        DIV0NULL(PFR_10D.ach_c_avg_past10d,PFR_30D.ach_c_avg_past30d) AS ach_c_avg_past10by30d
        
    
    FROM BUSINESS_METRICS AS BUM
    LEFT JOIN BALANCE_METRICS AS BAM on BUM.BUSINESS_ID = BAM.BUSINESS_ID
    LEFT JOIN PFR_PAST_TXN_AGG_PAST30D AS PFR_30D ON BUM.BUSINESS_ID = PFR_30D.BUSINESS_ID
    LEFT JOIN PFR_PAST_TXN_AGG_PAST10D AS PFR_10D ON BUM.BUSINESS_ID = PFR_10D.BUSINESS_ID
    LEFT JOIN PFR_PAST_TXN_AGG_PAST2D AS PFR_2D ON BUM.BUSINESS_ID = PFR_2D.BUSINESS_ID
    LEFT JOIN PAST_30_DAY_AVG_ACH AS PACH ON BUM.BUSINESS_ID = PACH.BUSINESS_ID
)

,LTV_INPUT AS (
SELECT
    BUSINESS_ID,
    CONTRIBUTION_LTV_M1,
    CONTRIBUTION_LTV_M3
FROM prod_db.models.customer_lifetime_value
)

,EXPERIAN_INPUT AS (
SELECT
    BUSINESS_ID,
    ROUND(FICO_RISK_V8) AS MOST_RECENT_FICO,
    ROUND(TOTAL_INQUIRIES_L12M) AS TOTAL_INQUIRIES_L12M,
    (IFF(TOTAL_AVAILABLE_CREDIT_LIMIT_BANKCARD_L3M > 999999995, NULL, TOTAL_AVAILABLE_CREDIT_LIMIT_BANKCARD_L3M))
    *
    (IFF(UTILIZATION_RATIO_BANKCARDS_L3M > 995, NULL, UTILIZATION_RATIO_BANKCARDS_L3M) / 100) AS BANKCARD_BALANCE,
    IFF(AVG_MONTHS_SINCE_RECENT_60PLUS_DQ > 9995, NULL, ROUND(AVG_MONTHS_SINCE_RECENT_60PLUS_DQ)) AS AVG_MONTHS_SINCE_RECENT_60PLUS_DQ,
    IFF(TOTAL_DQ_INSTANCES_60DPD_L6M > 990, NULL, TOTAL_DQ_INSTANCES_60DPD_L6M) AS TOTAL_DQ_INSTANCES_60DPD_L6M,
    TOTAL_REVOLVING_BALANCE
FROM PROD_DB.DATA.EXPERIAN_CREDIT_REPORT
WHERE CREATED_AT = (SELECT MAX(CREATED_AT) FROM PROD_DB.DATA.EXPERIAN_CREDIT_REPORT)
)

,TRANSACTION_COUNT AS (
SELECT
    BUSINESS_ID,
    COUNT(*) AS TOTAL_TXN_COUNT_PAST30D
FROM TRANSACTIONS
WHERE TRANSACTION_DATE >= DATEADD('MONTH',-1,TRANSACTION_DATE)
AND VVC_MEDIUM NOT IN ('Other Deposits','Others','TW','Fees')
GROUP BY 1
)

SELECT
    BTM.*,
    LI.CONTRIBUTION_LTV_M1,
    LI.CONTRIBUTION_LTV_M3,
    EI.MOST_RECENT_FICO,
    EI.TOTAL_INQUIRIES_L12M,
    EI.BANKCARD_BALANCE,
    EI.AVG_MONTHS_SINCE_RECENT_60PLUS_DQ,
    EI.TOTAL_DQ_INSTANCES_60DPD_L6M,
    EI.TOTAL_REVOLVING_BALANCE,
    TC.TOTAL_TXN_COUNT_PAST30D
    
FROM BUSINESS_TRANSACTION_METRICS AS BTM
LEFT JOIN LTV_INPUT AS LI ON BTM.BUSINESS_ID = LI.BUSINESS_ID
LEFT JOIN EXPERIAN_INPUT AS EI ON BTM.BUSINESS_ID = EI.BUSINESS_ID
LEFT JOIN TRANSACTION_COUNT AS TC ON BTM.BUSINESS_ID = TC.BUSINESS_ID
