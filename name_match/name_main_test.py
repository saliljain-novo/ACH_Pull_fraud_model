import json
from name_matching_script import name_matching
import pandas as pd
from ast import literal_eval


# Inputs from Engineering
# user_name_full = {'Salil Jain', 'Edison Calvopina'}
# user_name_mid = {'Salil Jain', 'Edison Calvopina'}
# company_name = 'Salil Jain'
# dba = 'Empty'
# external_account_name = ["Sadia Rattani"]

df1 = pd.read_csv('/Users/saliljain/Downloads/test_name.csv')
final_list = []
def main():

    # result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()
    # print(result)
    # return result

    for index, row in df1.iterrows():
        pfr_id = row['pfr_id']
        user_name_full = row['user_name_1']
        user_name_mid = row['user_name_2']
        company_name = row['company_name']
        dba = row['dba']
        external_account_name = row['external_account_names']

        # print(dba)
        print(pfr_id)

        result = name_matching(user_name_full, user_name_mid, company_name, dba, external_account_name).get_result()

        final_list.append([pfr_id,result])
    
    df_final = pd.DataFrame(final_list,columns=['pfr_id','result'])
    df_final.to_csv('/Users/saliljain/Downloads/test_name_results.csv')
    return result

if __name__ == '__main__':
    main()