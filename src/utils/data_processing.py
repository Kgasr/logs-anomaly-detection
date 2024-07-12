import pandas as pd

from .logger import CustomLogger

from src.config.config import read_config
from src.core.get_parser import get_parser

print("2")
config_file = 'config/config.yaml'
custom_logger = CustomLogger(config_file).get_logger()


def data_processing(patterns_file, log_type, log_file):
    patterns, features_df = read_anomaly_patterns_n_setup_features(patterns_file)
    log_df = parse_logs(log_type, log_file)
    features_dataframe = extract_features(log_df, patterns, features_df)
    return features_dataframe


def read_anomaly_patterns_n_setup_features(pattern_file):
    patterns = read_config(pattern_file)
    df = pd.DataFrame(
        columns=['cip', 'datetime', 'num_requests'] + list(patterns['error_codes'].keys()) + ['is_anomaly'])
    df.columns = df.columns.astype(str)
    return patterns, df


def parse_logs(log_type, log_file):
    parser = get_parser(log_type, log_file)
    print(parser)
    df = parser.parse_logs()
    custom_logger.info("22")
    return df


def extract_features(log_df, params, features):
    time_window = params['time_window']
    grouped = log_df.groupby([pd.Grouper(key='datetime', freq=time_window), 'cip'])

    for (time, ip), group in grouped:
        num_requests = group.shape[0]
        error_counts = {
            str(error_code): sum(group['status'] == params['error_codes'][error_code]['status']) for error_code in
            params['error_codes']}
        is_anomaly_errors = [error_counts[str(error_code)] >= params['error_codes'][error_code]['threshold'] for
                             error_code in params['error_codes']]
        is_anomaly_requests = num_requests >= params['high_request_threshold']
        is_anomaly = any(is_anomaly_errors) or is_anomaly_requests
        df_to_concat = {
            'cip': ip,
            'datetime': time,
            'num_requests': num_requests,
            **error_counts,
            'is_anomaly': is_anomaly
        }

        features = pd.concat([features, pd.DataFrame([df_to_concat])], ignore_index=True)
        """
        features = features._append({
            'cip': ip,
            'datetime': time,
            'num_requests': num_requests,
            **error_counts,
            'is_anomaly': is_anomaly
        }, ignore_index=True)
        """

    return features



