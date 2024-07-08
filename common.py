import yaml
import os.path
import datetime
import pandas as pd


def read_yaml(yaml_file):
    with open(yaml_file, 'r') as cfg:
        config = yaml.safe_load(cfg)
    return config


def extract_features(log_df, params, features):
    time_window = params['time_window']
    grouped = log_df.groupby([pd.Grouper(key='datetime', freq=time_window), 'cip'])
    for (time, ip), group in grouped:
        num_requests = group.shape[0]
        error_counts = {
            str(error_code): sum(group['status'] == params['error_codes'][error_code]['status']) for error_code in params['error_codes']}
        is_anomaly_errors = [error_counts[str(error_code)] >= params['error_codes'][error_code]['threshold'] for error_code in params['error_codes']]
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


def save_results(result, output_file):
    if os.path.exists(output_file):
        current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        directory, filename = os.path.split(output_file)
        filename,extension = os.path.splitext(filename)
        last_output_file_name = f"{filename}_{current_time}{extension}"
        last_output_file_path = os.path.join(directory,last_output_file_name)
        os.rename(output_file,last_output_file_path)
    result_df = pd.DataFrame(result)
    result_df.to_excel(output_file, index=False)