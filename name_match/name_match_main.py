import json
from name_matching_script import name_matching
import pandas as pd
from ast import literal_eval


# Inputs from Engineering
# user_name_full = ['Sadia Rattani', 'Edison Calvopina']
# user_name_mid = ['Sadia Rattani', 'Edison Calvopina']
# company_name = 'NNLA Inc.'
# dba = ''
# external_account_name = [ "NNLA INC"]


def run_name_match(data_dict):

    user_name_full = data_dict['user_name_full']
    user_name_mid = data_dict['user_name_mid']
    company_name = data_dict['company_name']
    dba = data_dict['dba']
    external_account_name = data_dict['external_account_name']

    result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()
    return result



def lambda_handler(event, context):
    data = json.loads(event['body'])

    needed_keys = ['user_name_full', 'user_name_mid', 'company_name', 'dba', 'external_account_name','pfr_id']

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
        name_status = run_name_match(data)
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
        'body': json.dumps({'pfr_id': data['pfr_id'], 'name_match_status': name_status})
    }


if __name__ == '__main__':
    event_dict = {"pfr_id": "ec1a475b-ede9-4d5b-8f23-834916d7c6aa"}
    event_dict['user_name_full'] = ['Marcus A Herzog', 'Zena Penning']
    event_dict['user_name_mid'] = ['Marcus Herzog', 'Zena Penning']
    event_dict['company_name'] = 'ZNA MUSIC LLC'
    event_dict['dba'] = 'Empty'
    event_dict['external_account_name'] = ["Marcus Herzog"]

    event = {
        'body': json.dumps(event_dict)
    }
    lambda_return = lambda_handler(event, {})
    print(lambda_return)
