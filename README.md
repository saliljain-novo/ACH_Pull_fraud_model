Two models are built for ACH Pull based on tenure of business with Novo.

1. Less than 6 months (Train on 1-6 months | Test on less than 6 months of tenure)
     **Hyper Parameters**
       categoricals = ['bank_risk']
       eval_metric = 'AUC',
       depth = 6,
       iterations = 400,
       l2_leaf_reg = 70,
       learning_rate = 0.11,
       random_state = 42,
       use_best_model=True

2. Greater than 6 months (Train on greater than 6 months | Test on greater than 6 months)
    **Hyper Parameters**
       categoricals = ['bank_risk']
       eval_metric = 'AUC',
       depth = 5,
       iterations = 400,
       l2_leaf_reg = 100,
       learning_rate = 0.11,
       random_state = 42,
       use_best_model=True
                       
3. Notebook Instance for deployement: https://us-east-1.console.aws.amazon.com/sagemaker/home?region=us-east-1#/notebook-instances/Ach-fraud-model-test
4. AWS Greater than 6 months model (Deployment Notebook) : ach_pull_fraud_greater_6_months.ipynb

5. AWS Less than 6 months model (Deployment Notebook) : ach_pull_fraud_less_6_months.ipynb

6. Endpoint Greater than 6 months: INFO:sagemaker:Creating model with name: sagemaker-jumpstart-2024-03-04-04-36-18-946
                                   INFO:sagemaker:Creating endpoint-config with name greater-6-months-catboost-classificatio-2024-03-04-04-36-18-946
                                   INFO:sagemaker:Creating endpoint with name greater-6-months-catboost-classificatio-2024-03-04-04-36-18-946
   
7. Endpoint Greater than 6 months: INFO:sagemaker:Creating model with name: sagemaker-jumpstart-2024-03-04-04-24-01-233
                                   INFO:sagemaker:Creating endpoint-config with name less-6-months-catboost-classification-m-2024-03-04-04-24-01-233
                                   INFO:sagemaker:Creating endpoint with name less-6-months-catboost-classification-m-2024-03-04-04-24-01-233   

8. ACH Pull Model Results : https://docs.google.com/spreadsheets/d/15cIM-xeszrWSPnwo_UkvdM6U7_LYK-sf4A784CRMiM8/edit#gid=311510193
                            https://docs.google.com/spreadsheets/d/1mcZ5lRzOZ2RbrC8nZbCggeqZRZD6vIpSV_wVsGvyZ4Q/edit#gid=772396204