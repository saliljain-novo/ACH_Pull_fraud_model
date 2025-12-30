import json
from name_match import name_matching
from helper import (get_model_variables,
                    score_model,
                    clean_value,
                    process_result)

from ast import literal_eval
import re

############## DS Model ##############
def ds_model(data_dict):
    business_id = data_dict['business_id']
    amount = data_dict['amount']
    external_bank_name = data_dict['external_bank_name']

    raw_variables, model_variables, months_on_books = get_model_variables(business_id, amount, external_bank_name)
    score, model_name = score_model(model_variables, months_on_books)

    return score, model_name, raw_variables.iloc[0].to_dict()


############## Name Matching ##############
def name_match(data_dict):

    users = clean_value(data_dict.get('users', 'Empty'))
    
    if users == 'Empty':
        user_name_full = 'Empty'
        user_name_mid = 'Empty'
    else:
        user_name_full = [f"{user['first_name']} {user['middle_name']} {user['last_name']}" for user in users]
        user_name_mid = [f"{user['first_name']} {user['last_name']}" for user in users]

    company_name = clean_value(data_dict.get('company_name', 'Empty'))
    dba = clean_value(data_dict.get('dba', 'Empty'))
    external_account_name = clean_value(data_dict.get('external_account_name', 'Empty'))

    company_name = re.sub(r'\s+\bLLC\b$', '', company_name, flags=re.IGNORECASE)
    external_account_name = [re.sub(r'\s+\bLLC\b$', '', name, flags=re.IGNORECASE).strip() for name in external_account_name]
    # print(company_name)
    # print(external_account_name)

    result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()
    print(result)
    result, decision = process_result(result)
    return result, decision



def lambda_handler(event, context):
    data = json.loads(event['body'])

    needed_keys = ['pfr_id', 'business_id','amount', 'external_bank_name',
                   'users', 'company_name', 'dba', 'external_account_name']

    for key in needed_keys:
        if key not in data.keys():
            print(f"Key Missing {key}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'All parameters needed for scoring not present',
                    'message': f"please include {needed_keys}; missing {key}"
                })
            }

    try:
        score, model_name, raw_variables = ds_model(data)
        name_status, decision_status = name_match(data)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': "Error parsing request",
                'message': f"{e}"
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({'pfr_id': data['pfr_id'], 
                            'ds_model_score': score, 
                            'model_used' : model_name,
                            'variables': raw_variables,
                            'name_match_result': name_status, 
                            'name_match_decision': decision_status
                            })
    }


if __name__ == '__main__':
    event_dict = {"pfr_id": "91cfac97-c769-4776-bcfa-ef27ad016b05"}
    event_dict["business_id"] = '55c27bc4-6e52-4b3b-9370-ddb122eb3a91'
    event_dict['amount'] = '300'
    event_dict['external_bank_name'] = 'Chase'
    event_dict['users'] = [
                            {
                                "first_name": "Danielle",
                                "middle_name": "",
                                "last_name": "Huppler",
                                },
                            #     {
                            #     "first_name": "Jane",
                            #     "middle_name": "Bar",
                            #     "last_name": "Doe",
                            # }
                          ]
    # event_dict['user_name_mid'] = ""
    event_dict['company_name'] = 'StayCo LLC'
    event_dict['dba'] = ''
    event_dict['external_account_name'] = ["KIDS PLAY LLC"]

    event = {
        'body': json.dumps(event_dict)
    }
    print(event)
    lambda_return = lambda_handler(event, {})
    print(lambda_return)