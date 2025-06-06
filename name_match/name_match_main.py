import json
from name_matching_script import name_matching
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
    amount = data_dict['amount']
    rb_at_deposit = data_dict['rb_at_deposit']
    tenure = data_dict['accountAge']

    if rb_at_deposit=="":
        rb_at_deposit=0

    rb_at_deposit = float(rb_at_deposit)
    tenure = float(tenure)
    amount = float(amount)

    ratio = rb_at_deposit/amount

    if user_name_full== []:
        user_name_full='Empty'
    
    if user_name_mid== []:
        user_name_mid='Empty'

    if company_name== "":
        company_name="Empty"

    if dba== "":
        dba="Empty"

    if external_account_name == []:
        external_account_name='Empty'


    result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()

    if result =='External_Account_Name_Null':
        decision='0'

    # if (result =='External_Account_Name_Null') & (ratio<0.5) & (amount>500) & (tenure<6):
    #     decision='-1'

    if result =='Name_Matched':
        decision='0'
    
    if result =='Name_Mismatch':
        decision='-1'

    return result, decision



def lambda_handler(event, context):
    data = json.loads(event['body'])

    needed_keys = ['user_name_full', 'user_name_mid', 'company_name', 'dba', 
                   'external_account_name','pfr_id','amount','rb_at_deposit','accountAge']

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
        name_status, decision_status = run_name_match(data)
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
        'body': json.dumps({'pfr_id': data['pfr_id'], 'name_match_status': name_status, 'decision_status': decision_status})
    }


if __name__ == '__main__':
    event_dict = {"pfr_id": "ec1a475b-ede9-4d5b-8f23-834916d7c6aa"}
    event_dict['user_name_full'] = ['Torie De-Laine']
    event_dict['user_name_mid'] = ['Torie De-Laine']
    event_dict['company_name'] = 'A.A.A.T.C LLC'
    event_dict['dba'] = 'Empty'
    event_dict['external_account_name'] = ["CHINENE DELAINE"]
    event_dict['amount'] = '200'
    event_dict['rb_at_deposit'] = '200'
    event_dict['accountAge'] = '1.4'

    event = {
        'body': json.dumps(event_dict)
    }
    lambda_return = lambda_handler(event, {})
    print(lambda_return)
