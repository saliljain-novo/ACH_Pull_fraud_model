
WITH ach as (
select a.id, a.business_id, a.created_at, b.bank_name, a.amount
from "public"."pull_funds_requests" a
inner join "public"."external_accounts" b on a.EXTERNAL_ACCOUNT_ID = b.ID
-- order by created_at desc
where a.id ='03ae2273-b02f-42fc-9d9e-3be3fedf23fb'
and a.BUSINESS_ID = 'b3a25437-54a1-43b5-ad74-6563cb6246dc'
)

----------- Bank Risk ------------

,external_bank as (
select distinct a.id, b.bank_name, date(a.CREATED_AT) as external_bank_date,
CASE WHEN a.status = 'returned' THEN 1 ELSE 0 END as is_returned
from "public"."pull_funds_requests" a
inner join "public"."external_accounts" b on a.EXTERNAL_ACCOUNT_ID = b.ID
WHERE date(a.CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '90 day') AND date(a.CREATED_AT) <= date(CURRENT_TIMESTAMP)
and a.status in ('returned','completed')
)


,bank_return_rate as (
select bank_name ,avg(is_returned) as return_rate
from external_bank
group by 1
)

,bank_risk as (
select bank_name, 
case when return_rate > 0.08 then 3
    when return_rate < 0.08 and return_rate >0.03 then 2
    when return_rate < 0.03 then 1 end as bank_risk 
from bank_return_rate
)

-----------------All 30 day transactions-------------------------

,all_pfr as (
select ID, TRANSACTION_ID, USER_ID, BUSINESS_ID, EXTERNAL_ACCOUNT_ID, AMOUNT, CREATED_AT, UPDATED_AT, STATUS, created_at AS pfr_created_at
,DATE(CREATED_AT) AS ach_created_at,

---- STATUS FEATURES
CASE WHEN status = 'completed' THEN 1 ELSE 0 END as is_completed,
CASE WHEN status = 'returned' THEN 1 ELSE 0 END as is_returned,
CASE WHEN status = 'rejected' THEN 1 ELSE 0 END as is_rejected

from "public"."pull_funds_requests"
WHERE date(CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '30 day') AND date(CREATED_AT) <= date(CURRENT_TIMESTAMP)
and business_id in (select business_id from ach )
)

----------------- EIN SSN -------------------------

,applications AS (
    SELECT id :: text FROM "public"."onboarding_applications"
)

,owners AS (
    SELECT * FROM "public"."onboarding_owners"
)

,businesses AS (
            SELECT id
                  ,meta->'application'->> 'id' as application_id
                  ,meta->'business'->> 'ein_hash' as ein_hash 
            FROM "public"."businesses" where id in (select business_id from ach)
)


,ein_ssn AS (
    SELECT
        businesses.application_id AS onbs_appid,
        owners.application_id :: text AS own_appid
    FROM businesses
    INNER JOIN owners
        ON businesses.ein_hash = owners.ssn_hash
)

,ein_ssn_next AS (
    SELECT onbs_appid FROM ein_ssn
    UNION ALL
    SELECT own_appid AS onbs_appid FROM ein_ssn
)

, tempq AS 
(
SELECT
    a.id as application_id,
    (
        CASE
            WHEN
                a.id = e.onbs_appid THEN 1
            ELSE 0
        END
    )::TEXT AS ein_ssn
FROM applications a
LEFT JOIN ein_ssn_next e 
ON
a.id = e.onbs_appid
)

, dda_query as (
select distinct b.id as business_id,
          t.ein_ssn      
from businesses b
left join tempq t
    on b.application_id = t.application_id
where b.id in (select business_id from ach)
)


--------Running balance 30 days------------

, cal_dates_1 AS (
		select generate_series(
           (date (current_date - INTERVAL '31 day'))::timestamp,
           (date (current_date ))::timestamp,
           interval '1 day'
         )  AS CAL_DATE)
         
, biz_dates_1 AS (
			SELECT a.BUSINESS_ID, MIN(a.transaction_date) AS FIRST_TXN_DATE
			from "public"."transactions"  a INNER JOIN  all_pfr b using(business_id)
			GROUP BY a.BUSINESS_ID
			)
			

, PILOT_BIZ_DATES_TEMP AS (
		SELECT *
		FROM  cal_dates_1 A LEFT JOIN
		biz_dates_1 B ON
		date(A.CAL_DATE) >= date(B.FIRST_TXN_DATE)
		ORDER BY CAL_DATE ASC)

,PILOT_BIZ_TXN_TEMP AS (
		SELECT   Business_id ,TRANSACTION_DATE,running_balance
		FROM
		         (SELECT a.*, RANK () OVER (PARTITION BY a.BUSINESS_ID,a.TRANSACTION_DATE ORDER BY a.timestamp DESC) RANKS
		         FROM "public"."transactions" a  INNER JOIN  all_pfr t using(business_id)
                 where 1=1
                 and a.status='active'
		         ) b
		WHERE RANKS=1
		order by Business_id, TRANSACTION_DATE)
		

,PILOT_DAILY_BALANCES_1  AS (
		SELECT BUSINESS_ID,FIRST_TXN_DATE, CAL_DATE,TRANSACTION_DATE,RUNNING_BALANCE,
		first_value(RUNNING_BALANCE) OVER (PARTITION BY BUSINESS_ID,GROUPER ORDER BY cal_date asc, TRANSACTION_DATE desc, RUNNING_BALANCE asc nulls last) as RUNNING_BALANCE_2
		FROM (
		SELECT A.BUSINESS_ID,A.CAL_DATE,A.FIRST_TXN_DATE,
		        B.TRANSACTION_DATE,B.RUNNING_BALANCE,
		         COUNT(running_balance) OVER (PARTITION BY a.BUSINESS_ID ORDER BY cal_date asc) as grouper
		        FROM PILOT_BIZ_DATES_TEMP A LEFT JOIN
		        PILOT_BIZ_TXN_TEMP B ON
		        A.BUSINESS_ID=B.BUSINESS_ID AND
		        date(A.CAL_DATE)= date(B.TRANSACTION_DATE)
		        ORDER BY A.CAL_DATE ASC) c
		ORDER BY BUSINESS_ID,CAL_DATE)
		

,final_daily_running_balance AS (
		 with temp as (
		    SELECT distinct a.business_id, b.id as pfr_id, CREATED_AT, (extract(day from b.CREATED_AT::timestamp -  a.cal_date::timestamp)) as daySincepfr
		                        , cal_date, running_balance_2 AS running_balance_daily
		           FROM  PILOT_DAILY_BALANCES_1  a   INNER JOIN all_pfr b using(business_id) ) 
		      select * from temp 
		          WHERE daySincepfr > 0 AND daySincepfr <= 30
		         ) 

, rb_30d AS (	
				SELECT business_id, pfr_id,
				               sum(CASE WHEN running_balance_daily < 0 THEN  1 ELSE 0 end) AS od_count_past30d, 
				               sum(CASE WHEN running_balance_daily = 0 THEN  1 ELSE 0 end) AS zero_balance_count_past30d,
				               AVG(running_balance_daily) as Avg_running_balance_past30d,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_past30d
				FROM final_daily_running_balance
				GROUP BY business_id, pfr_id)


, all_prev_txns as 
(select b.business_id, b.id as pfr_id, a.running_balance, 
    RANK () OVER (PARTITION BY b.id, CREATED_AT ORDER BY timestamp DESC) ranks
from "public"."transactions" a inner join all_pfr b
on a.business_id = b.business_id
    and a.transaction_date < b.CREATED_AT
    AND a.STATUS ='active'
)

, latest_running_balance as 
(
select business_id, pfr_id, running_balance as rb_at_deposit
from all_prev_txns
where ranks = 1
)

, final_query_rb as 
(
select * 
from latest_running_balance left join rb_30d
using (business_id, pfr_id)
)


,running_balance_30d as(
select a.business_id, a.id, a.CREATED_AT, b.RB_AT_DEPOSIT, b.OD_COUNT_PAST30D, b.ZERO_BALANCE_COUNT_PAST30D, b.AVG_RUNNING_BALANCE_PAST30D, b.STDDEV_RUNNING_BALANCE_PAST30D
from all_pfr a left outer join final_query_rb b on a.id = b.pfr_id
order by a.created_at
)



--------Transactions 30 days------------

-- with all_pfr_transaction AS (
-- select
-- a.id, a.TRANSACTION_ID, a.USER_ID, a.BUSINESS_ID, a.EXTERNAL_ACCOUNT_ID,  a.CREATED_AT AS pfr_created_at, a.created_at, a.STATUS, a.AMOUNT
-- FROM "public"."pull_funds_requests"  a
-- WHERE date(CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '30 day') AND date(CREATED_AT) <= date(CURRENT_TIMESTAMP)
-- )


,pfr_past_txn_past30d as (
select b.transaction_id as pfr_transaction_id, b.id as pfr_id, a.business_id, a.category_id, a.amount, a.description, a.type,
a.medium, a.status, a.created_date, a.transaction_date,
a.timestamp, a.running_balance, a.merchant_category_code,
b.pfr_created_at, b.status as pfr_status,
extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp) as days_since_pfr,
-- case when a.derived_medium = 'ACH' and a.type='credit' then 1 else 0 end as is_ach_c,
-- case when a.derived_medium = 'ACH' and a.type='debit' then 1 else 0 end as is_ach_d,
CASE WHEN medium IN ('External Deposit', 'IAT Deposit') OR a.meta::json->'transfer_id' IS NOT NULL 
            and a.type='credit' then 1 else 0 end as is_ach_c,
CASE WHEN medium IN ('External Withdrawal', 'IAT Withdrawal') OR a.meta::json->'transfer_id' IS NOT NULL 
            and a.type='debit' then 1 else 0 end as is_ach_d,
case when a.description = 'Mobile Check Deposit' and a.type='credit' then 1 else 0 end as is_mrdc_c,
case when a.description = 'Mobile Check Deposit' and a.type='debit' then 1 else 0 end as is_mrdc_d,
case when a.type = 'credit' then 1 else 0 end as is_credit,
case when a.type = 'debit' then 1 else 0 end as is_debit,
case when a.medium = 'POS Withdrawal' then 1 else 0 end as is_card_txn,
(CASE WHEN
type = 'credit' AND medium = 'External Deposit' AND a.description ILIKE ANY(array[
'%AMAZON%'
, '%AMZN%'
, '%STRIPE%'
, '%SQUARE INC%'
, '%SHOPIFY%'
, '%SHOPPAY%']
)
AND NOT(
a.description ILIKE ANY(array[
'%LYFT'
, '%OfferUp%'
, '%Gumroad%'
, '%FB%%Fundrai%'
, '%Verify%'
, '%CASH%'
, '%PAYROLL%'
, '%VRFY%'
, '%CAPITAL%'
, '%REFUND%']
)
) THEN 1
ELSE 0
END) AS is_pd_txn
from "public"."transactions" a right join all_pfr b
on a.business_id = b.business_id
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) > 0 -- a.transaction_date < b.mrdc_created_at
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) <= 30
and a.status='active'
)

,pfr_past30d_aggregate_temp as (
select pfr_id, business_id, pfr_status,
sum(is_card_txn) as card_txn_count_past30d,
sum(is_credit) as credit_txn_count_past30d,
median(case when is_ach_c=1 then abs(amount) else null end) as ach_c_median_past30d,
median(case when is_ach_d=1 then abs(amount) else null end) as ach_d_median_past30d,
avg(is_ach_c*abs(amount)) as ach_c_avg_past30d,
median(case when is_card_txn=1 then abs(amount) else null end) as card_txn_median_past30d,
sum(is_ach_c) as ach_c_count_past30d,
avg(is_ach_d*abs(amount)) as ach_d_avg_past30d,
avg(is_card_txn*abs(amount)) as card_txn_avg_past30d,
sum(is_debit) as debit_txn_count_past30d,
stddev(case when is_ach_c=1 then abs(amount) else null end) as ach_c_std_past30d
from pfr_past_txn_past30d
group by pfr_id, business_id, pfr_status
)

,pfr_past30d_aggregate as (
select *, debit_txn_count_past30d/NULLIF(COALESCE(CAST(credit_txn_count_past30d as float),0),0) as debit_by_credit_past_30d
from pfr_past30d_aggregate_temp 
)

--------Transactions 2 days------------

-- with all_pfr_transaction AS (
-- select
-- a.id, a.TRANSACTION_ID, a.USER_ID, a.BUSINESS_ID, a.EXTERNAL_ACCOUNT_ID,  a.CREATED_AT AS pfr_created_at, a.created_at, a.STATUS, a.AMOUNT
-- FROM "public"."pull_funds_requests"  a
-- WHERE date(CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '30 day') AND date(CREATED_AT) <= date(CURRENT_TIMESTAMP)
-- )


,pfr_past_txn_past2d as (
select b.transaction_id as pfr_transaction_id, b.id as pfr_id, a.business_id, a.category_id, a.amount, a.description, a.type,
a.medium, a.status, a.created_date, a.transaction_date,
a.timestamp, a.running_balance, a.merchant_category_code,
b.pfr_created_at, b.status as pfr_status,

case when a.medium = 'POS Withdrawal' then 1 else 0 end as is_card_txn
from "public"."transactions" a right join all_pfr b
on a.business_id = b.business_id
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) > 0 -- a.transaction_date < b.mrdc_created_at
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) <= 2
and a.status='active'
)

,pfr_past2d_aggregate as (
select pfr_id, business_id, pfr_status,
median(case when is_card_txn=1 then abs(amount) else null end) as card_txn_median_past2d
from pfr_past_txn_past2d
group by pfr_id, business_id, pfr_status
)


--------Transactions 10 days------------

-- with all_pfr_transaction AS (
-- select
-- a.id, a.TRANSACTION_ID, a.USER_ID, a.BUSINESS_ID, a.EXTERNAL_ACCOUNT_ID,  a.CREATED_AT AS pfr_created_at, a.created_at, a.STATUS, a.AMOUNT
-- FROM "public"."pull_funds_requests"  a
-- WHERE date(CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '30 day') AND date(CREATED_AT) <= date(CURRENT_TIMESTAMP)
-- )


,pfr_past_txn_past10d as (
select b.transaction_id as pfr_transaction_id, b.id as pfr_id, a.business_id, a.category_id, a.amount, a.description, a.type,
a.medium, a.status, a.created_date, a.transaction_date,
a.timestamp, a.running_balance, a.merchant_category_code,
b.pfr_created_at, b.status as pfr_status,
extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp) as days_since_pfr,

CASE WHEN medium IN ('External Deposit', 'IAT Deposit') OR a.meta::json->'transfer_id' IS NOT NULL 
            and a.type='credit' then 1 else 0 end as is_ach_c,
CASE WHEN medium IN ('External Withdrawal', 'IAT Withdrawal') OR a.meta::json->'transfer_id' IS NOT NULL 
            and a.type='debit' then 1 else 0 end as is_ach_d,
case when a.description = 'Mobile Check Deposit' and a.type='credit' then 1 else 0 end as is_mrdc_c,
case when a.description = 'Mobile Check Deposit' and a.type='debit' then 1 else 0 end as is_mrdc_d,
case when a.type = 'credit' then 1 else 0 end as is_credit,
case when a.type = 'debit' then 1 else 0 end as is_debit,
case when a.medium = 'POS Withdrawal' then 1 else 0 end as is_card_txn,
(CASE WHEN
type = 'credit' AND medium = 'External Deposit' AND a.description ILIKE ANY(array[
'%AMAZON%'
, '%AMZN%'
, '%STRIPE%'
, '%SQUARE INC%'
, '%SHOPIFY%'
, '%SHOPPAY%']
)
AND NOT(
a.description ILIKE ANY(array[
'%LYFT'
, '%OfferUp%'
, '%Gumroad%'
, '%FB%%Fundrai%'
, '%Verify%'
, '%CASH%'
, '%PAYROLL%'
, '%VRFY%'
, '%CAPITAL%'
, '%REFUND%']
)
) THEN 1
ELSE 0
END) AS is_pd_txn
from "public"."transactions" a right join all_pfr b
on a.business_id = b.business_id
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) > 0 -- a.transaction_date < b.mrdc_created_at
and (extract(day from pfr_created_at::timestamp - a.transaction_date::timestamp)) <= 10
and a.status='active'
)

,pfr_past10d_aggregate_temp as ( 
select pfr_id, business_id, pfr_status,
sum(is_credit) as credit_txn_count_past10d, 
median(case when is_card_txn=1 then abs(amount) else null end) as card_txn_median_past10d, 
sum(is_debit) as debit_txn_count_past10d, 
avg(is_card_txn*abs(amount)) as card_txn_avg_past10d, 
sum(is_ach_c) as ach_c_count_past10d, 
avg(is_ach_c*abs(amount)) as ach_c_avg_past10d, 
median(case when is_ach_c=1 then abs(amount) else null end) as ach_c_median_past10d
from pfr_past_txn_past10d
group by pfr_id, business_id, pfr_status
)

,pfr_past10d_aggregate as (
select *, debit_txn_count_past10d/NULLIF(COALESCE(CAST(credit_txn_count_past10d as float),0),0) as debit_by_credit_past_10d
from pfr_past10d_aggregate_temp 
)

,transaction_query_temp as (
select a.*, b.card_txn_median_past2d, c.card_txn_avg_past10d, c.debit_by_credit_past_10d,
card_txn_median_past10d/NULLIF(COALESCE(CAST(card_txn_median_past30d as float),0),0) as card_txn_median_past10by30d,
ach_c_count_past10d/NULLIF(COALESCE(CAST(ach_c_count_past30d as float),0),0) as ach_c_count_past10by30d,
ach_c_avg_past10d/NULLIF(COALESCE(CAST(ach_c_avg_past30d as float),0),0) as ach_c_avg_past10by30d,
debit_txn_count_past10d/NULLIF(COALESCE(CAST(debit_txn_count_past30d as float),0),0) as debit_txn_count_past10by30d,
ach_c_median_past10d/NULLIF(COALESCE(CAST(ach_c_median_past30d as float),0),0) as ach_c_median_past10by30d
from pfr_past30d_aggregate a left outer join pfr_past2d_aggregate b on a.pfr_id = b.pfr_id and a.business_id= b.business_id
left outer join pfr_past10d_aggregate c on a.pfr_id = c.pfr_id and a.business_id= c.business_id
)

,transaction_query as ( select transaction_query_temp.* from transaction_query_temp inner join ach on transaction_query_temp.pfr_id = ach.id )




-----------------PAST 30 DAY CALCULATIONS-------------------


-- with all_ach AS (
-- select
-- a.id, a.TRANSACTION_ID, a.USER_ID, a.BUSINESS_ID, a.EXTERNAL_ACCOUNT_ID,  DATE(a.CREATED_AT) AS ach_created_at,
-- a.created_at, a.STATUS, a.AMOUNT,

-- ---- STATUS FEATURES
-- CASE WHEN a.status = 'completed' THEN 1 ELSE 0 END as is_completed,
-- CASE WHEN a.status = 'returned' THEN 1 ELSE 0 END as is_returned,
-- CASE WHEN a.status = 'rejected' THEN 1 ELSE 0 END as is_rejected

-- from "public"."pull_funds_requests" a
-- WHERE date(CREATED_AT) >= date( CURRENT_TIMESTAMP - INTERVAL '30 day') AND date(CREATED_AT) <= date(CURRENT_TIMESTAMP)
-- )


, past30d_ach as (
select a.*,
past.id as past30d_id,
past.TRANSACTION_ID as past30d_TRANSACTION_ID,
past.USER_ID as past30d_USER_ID,
past.BUSINESS_ID AS past30d_BUSINESS_ID,
past.EXTERNAL_ACCOUNT_ID as past30d_EXTERNAL_ACCOUNT_ID,
past.amount as past30d_amount,
past.status as past30d_status,
past.is_completed as past30d_is_completed,
past.is_returned as past30d_is_returned,
past.is_rejected as past30d_is_rejected

from all_pfr a inner join all_pfr past
on a.business_id = past.business_id
and past.ach_created_at < a.ach_created_at
and past.ach_created_at >=  a.ach_created_at - INTERVAL '30 day'
)


, past30d_total_info as (
select BUSINESS_ID, id,
count(past30d_id) as past30d_ach_count, 
avg(past30d_amount) as past30d_avg_ach_amount, 
sum(past30d_is_completed) as past30d_completed_ach,
sum(past30d_is_returned)  as past30d_returned_ach,
sum(past30d_is_rejected)  as past30d_rejected_ach
from past30d_ach
group by 1,2
)


, past30d_returned_info as (
select business_id, id,
count(past30d_id) as returned_past30d_ach_count,
avg(past30d_amount) as returned_past30d_avg_ach_amount
from past30d_ach
where past30d_status = 'returned'
group by 1,2
)


, past30d_rejected_info as (
select business_id, id,
avg(past30d_amount) as rejected_past30d_avg_ach_amount
from past30d_ach
where past30d_status in ('rejected')
group by 1,2
)


, past30d_completed_info as (
select business_id, id,
count(past30d_id) as completed_past30d_ach_count,
avg(past30d_amount) as completed_past30d_avg_ach_amount, 
stddev(past30d_amount) as completed_past30d_std_ach_amount
from past30d_ach
where past30d_status = 'completed'
group by 1,2
)

,past_30d_features as (
select a.id, a.business_id, b.past30d_ach_count, b.past30d_avg_ach_amount, b.past30d_completed_ach, b.past30d_returned_ach, b.past30d_rejected_ach,
c.returned_past30d_ach_count, c.returned_past30d_avg_ach_amount, d.rejected_past30d_avg_ach_amount, e.completed_past30d_ach_count,
e.completed_past30d_avg_ach_amount, e.completed_past30d_std_ach_amount, a.amount/(e.completed_past30d_avg_ach_amount + 1) as  completed_past30d_vs_current_amount_score
from all_pfr a left outer join past30d_total_info b on a.id = b.id and a. business_id = b.business_id
left outer join past30d_returned_info c on a.id = c.id and a. business_id = c.business_id
left outer join past30d_rejected_info d on a.id = d.id and a. business_id = d.business_id
left outer join past30d_completed_info e on a.id = e.id and a. business_id = e.business_id
)

select a.*, 

COALESCE(b.bank_risk,0) as bank_risk, c.ein_ssn,

d.RB_AT_DEPOSIT, d.OD_COUNT_PAST30D, d.ZERO_BALANCE_COUNT_PAST30D, d.AVG_RUNNING_BALANCE_PAST30D, d.STDDEV_RUNNING_BALANCE_PAST30D,

e.card_txn_median_past2d, e.card_txn_avg_past10d, e.debit_by_credit_past_10d, e.card_txn_median_past10by30d,
e.ach_c_count_past10by30d, e.ach_c_avg_past10by30d, e.debit_txn_count_past10by30d, e.ach_c_median_past10by30d,

-- f.past30d_ach_count, f.past30d_avg_ach_amount, f.past30d_completed_ach, f.past30d_returned_ach, f.past30d_rejected_ach,
-- f.returned_past30d_ach_count, f.returned_past30d_avg_ach_amount, f.rejected_past30d_avg_ach_amount, f.completed_past30d_ach_count,
-- f.completed_past30d_avg_ach_amount, f.completed_past30d_std_ach_amount, 
f.completed_past30d_vs_current_amount_score,

COALESCE(f.past30d_ach_count,0) as past30d_ach_count,
COALESCE(f.past30d_avg_ach_amount,0) as past30d_avg_ach_amount,
COALESCE(f.past30d_completed_ach,0) as past30d_completed_ach,
COALESCE(f.past30d_returned_ach,0) as past30d_returned_ach,
COALESCE(f.past30d_rejected_ach,0) as past30d_rejected_ach,

COALESCE(f.returned_past30d_ach_count,0) as  returned_past30d_ach_count,
COALESCE(f.returned_past30d_avg_ach_amount,0) as  returned_past30d_avg_ach_amount,
COALESCE(f.rejected_past30d_avg_ach_amount,0) as  rejected_past30d_avg_ach_amount,
COALESCE(f.completed_past30d_ach_count,0) as completed_past30d_ach_count,
COALESCE(f.completed_past30d_avg_ach_amount,0) as completed_past30d_avg_ach_amount,
COALESCE(f.completed_past30d_std_ach_amount,0) as completed_past30d_std_ach_amount

from ach a left outer join bank_risk b on a.bank_name = b.bank_name
left outer join dda_query c on a.business_id= c.business_id
left outer join running_balance_30d d on a.business_id = d.business_id and a.id = d.id
left outer join transaction_query e on a.business_id = e.business_id and a.id = e.pfr_id
left outer join past_30d_features f on a.business_id = f.business_id and a.id = f.id


