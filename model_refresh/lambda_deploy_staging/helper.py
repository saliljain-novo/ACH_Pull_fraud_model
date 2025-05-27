import snowflake.connector
import pandas as pd
from catboost import CatBoostClassifier


############## Snowflake Connection ##############
def create_snowflake_connection():
    return snowflake.connector.connect(
    user= 'general_access',
    password= 'Xylo-data-bi-844tag',
    account= 'hba10191.us-east-1',
    warehouse= 'COMPUTE_WH',
    database='PROD_DB',
    schema='ADHOC',
    role= 'BI_ROLE'
    )


def get_data(conn, query, params=None):
    return pd.read_sql(query, conn, params=params)


############## DS Model ##############
# Variables Selection
def get_model_variables(business_id, amount, external_bank_name):
    conn = create_snowflake_connection()

    query = """
        with data1 as (
        SELECT a.*
        FROM "PROD_DB"."DATA"."ACH_DS_ENGINE_PROD" a
        WHERE a.business_id = %s
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.business_id ORDER BY a.run_time DESC) = 1
        )

        ,data2 as(
        select bank_risk from "PROD_DB"."DATA"."ACH_DS_ENGINE_BANK_RISK"
        where trim(LOWER(bank_name))=TRIM(LOWER(%s))
        )

        select a.*, COALESCE(b.bank_risk,0) as bank_risk 
        from data1 a left join data2 b ON TRUE limit 1
    """

    data = get_data(conn, query, params=(business_id, external_bank_name))
    data['amount'] = float(amount)
    data.columns = data.columns.str.lower()

    data = data.drop(columns=['run_time'])
    months_on_books = data["mob"][0]

    if months_on_books < 6:
        model_variables = data[['avg_running_balance_past30d','bank_risk','rb_at_deposit','past30d_returned_ach',
                     'card_txn_count_past30d','returned_past30d_avg_ach_amount',
                     'stddev_running_balance_past30d','ach_c_avg_past30d','od_count_past30d',
                     'ach_c_count_past10by30d','amount','ach_c_median_past30d','card_txn_avg_past30d',
                     'past30d_avg_ach_amount','debit_txn_count_past10by30d','ach_c_std_past30d',
                     'card_txn_median_past10by30d','card_txn_median_past30d','ach_c_median_past10by30d',
                     'debit_by_credit_past_30d','rejected_past30d_avg_ach_amount','ein_ssn']]
        
    elif months_on_books >= 6:
        model_variables = data[['bank_risk','avg_running_balance_past30d','rb_at_deposit','card_txn_count_past30d',
                     'debit_by_credit_past_30d','ach_c_median_past30d','ach_c_avg_past30d','amount',
                     'od_count_past30d','returned_past30d_avg_ach_amount','ach_c_avg_past10by30d',
                     'debit_by_credit_past_10d','past30d_ach_count','card_txn_median_past2d',
                     'ach_d_median_past30d','card_txn_median_past30d','stddev_running_balance_past30d',
                     'ach_c_count_past30d','card_txn_avg_past10d','card_txn_median_past10by30d',
                     'ach_d_avg_past30d','past30d_avg_ach_amount','ach_c_count_past10by30d',
                     'zero_balance_count_past30d']] 

    return data, model_variables, months_on_books

# Score Model
def score_model(model_variables, months_on_books):
    model_name =''
    if months_on_books < 6:
        model_file_name = 'model_less_6_months.cbm'
        model = CatBoostClassifier()
        model.load_model(model_file_name)
        model_name = 'less_6_months'
    elif months_on_books >= 6:
        model_file_name = 'model_greater_6_months.cbm'
        model = CatBoostClassifier()
        model.load_model(model_file_name)
        model_name = 'greater_6_months'
    
    return model.predict_proba(model_variables)[0][1], model_name # round(model.predict_proba(model_variables)[0][1],3)


############## Name Matching ##############
def clean_value(value):
    if isinstance(value, list) and not value:
        return 'Empty'
    if isinstance(value, str) and value.strip() == "":
        return 'Empty'
    return value

def process_result(result):
    decision_map = {
        'External_Account_Name_Null': '0',
        'Name_Matched': '0',
        'Name_Mismatch': '-1'
    }
    decision = decision_map.get(result, '0') 
    return result, decision


if __name__ == "__main__":
    test1, test2, test3 = get_model_variables('f3c14100-8453-4381-a411-999796c96b99',470,'test')
    # print(test1)
    print(test2)
    print(test3)