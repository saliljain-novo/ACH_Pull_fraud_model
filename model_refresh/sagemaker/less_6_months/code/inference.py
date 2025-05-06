import os
import json
import logging
import pandas as pd
from catboost import CatBoostClassifier
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO)

def model_fn(model_dir):
    logging.info(f"Loading model from {model_dir}")
    model = CatBoostClassifier()
    try:
        model.load_model(os.path.join(model_dir, "model_less_6_months.cbm"))
        logging.info("Model loaded successfully")
    except Exception as e:
        logging.error(f"Error loading model: {e}")
        raise e
    return model

FEATURE_NAMES = [
    'avg_running_balance_past30d','bank_risk','rb_at_deposit','past30d_returned_ach','card_txn_count_past30d',
    'returned_past30d_avg_ach_amount','stddev_running_balance_past30d','ach_c_avg_past30d','od_count_past30d',
    'ach_c_count_past10by30d','amount','ach_c_median_past30d','card_txn_avg_past30d','past30d_avg_ach_amount',
    'debit_txn_count_past10by30d','ach_c_std_past30d','card_txn_median_past10by30d','card_txn_median_past30d',
    'ach_c_median_past10by30d','debit_by_credit_past_30d','rejected_past30d_avg_ach_amount','ein_ssn'
]

def input_fn(request_body, request_content_type):
    logging.info(f"Parsing input data with content type: {request_content_type}")
    try:
        if request_content_type == "text/csv":
            if isinstance(request_body, bytes):
                request_body = request_body.decode("utf-8")
            df = pd.read_csv(StringIO(request_body), header=None)
            df.columns = FEATURE_NAMES
            logging.info(f"[INPUT_FN] Parsed DataFrame:\n{df.head()}")
            return df
        else:
            logging.error(f"Unsupported content type: {request_content_type}")
            raise ValueError(f"Unsupported content type: {request_content_type}")
    except Exception as e:
        logging.error(f"Error in input_fn: {e}")
        raise e

def predict_fn(input_data, model):
    logging.info("Making predictions")
    try:
        pred_probs = model.predict_proba(input_data)
        logging.info("Prediction successful")
        return {"probabilities": pred_probs.tolist()}
    except Exception as e:
        logging.error(f"Prediction error: {e}")
        raise e

def output_fn(prediction, content_type):
    logging.info("Formatting output")
    try:
        return json.dumps(prediction)
    except Exception as e:
        logging.error(f"Error in output_fn: {e}")
        raise e




