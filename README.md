Two models are built for ACH Pull based on tenure of business with Novo.

1. Less than 6 months (Train on 1-6 months | Test on less than 6 months of tenure)
     **Hyper Parameters**
       categoricals = ['bank_risk']
       eval_metric = 'AUC',
       depth = 5,
       iterations = 700,
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
4. Notebook Greater than 6 months model : ach_pull_fraud_greater_6_months.ipynb
5. Notebook Less than 6 months model : ach_pull_fraud_less_6_months.ipynb
   
   