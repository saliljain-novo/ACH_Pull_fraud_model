import json
from name_match import name_matching
from helper import (get_model_variables,
                    score_model,
                    clean_value,
                    process_result)

from ast import literal_eval

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

    result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()
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
    event_dict = {"pfr_id": "ec1a475b-ede9-4d5b-8f23-834916d7c6aa"}
    event_dict["business_id"] = 'f3c14100-8453-4381-a411-999796c96b99'
    event_dict['amount'] = '470'
    event_dict['external_bank_name'] = 'John'
    event_dict['users'] = [
                            {
                                "first_name": "John",
                                "middle_name": "Bar",
                                "last_name": "Doe",
                                },
                                {
                                "first_name": "Jane",
                                "middle_name": "Bar",
                                "last_name": "Doe",
                            }
                          ]
    # event_dict['user_name_mid'] = ""
    event_dict['company_name'] = ''
    event_dict['dba'] = ''
    event_dict['external_account_name'] = []

    event = {
        'body': json.dumps(event_dict)
    }
    print(event)
    lambda_return = lambda_handler(event, {})
    print(lambda_return)
