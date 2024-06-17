import json
import pandas as pd


def read_parameters(param_file):
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    return parameters


def extract_features(log_df,params, features):
    time_window = params['time_window']
    grouped = log_df.groupby([pd.Grouper(key='datetime', freq=time_window), 'cip'])
    for (time, ip), group in grouped:
        num_requests = group.shape[0]
        error_counts = {
            error_code: sum(group['status'] == params['error_codes'][error_code]['status']) for error_code in params['error_codes']}
        is_anomaly_errors = [error_counts[error_code] >= params['error_codes'][error_code]['threshold'] for error_code in params['error_codes']]
        is_anomaly_requests = num_requests >= params['high_request_threshold']
        is_anomaly = any(is_anomaly_errors) or is_anomaly_requests
        features = features._append({
            'cip': ip,
            'datetime': time,
            'num_requests': num_requests,
            **error_counts,
            'is_anomaly': is_anomaly
        }, ignore_index=True)
    return features
