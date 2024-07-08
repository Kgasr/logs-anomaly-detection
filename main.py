import sys
import pandas as pd

from train_and_predict import training, prediction
from common import read_yaml, extract_features, save_results
from parsers import parse_app_logs, parse_squid_logs, parse_iis_logs, parse_apache_logs


def initialize():
    config = read_yaml('config.yaml')

    mode = config['model']['mode']
    model_name = config['model']['model_name']

    log_type = config['log']['log_type']
    log_file = config['log']['log_file']

    output_file = config['output']['output_file']
    return mode, model_name, log_type, log_file, output_file


if __name__ == "__main__":
    mode, model_name, log_type, log_file, output_file = initialize()
    parameters = read_yaml('patterns.yaml')
    features_df = pd.DataFrame(columns=['cip', 'datetime', 'num_requests'] + list(parameters['error_codes'].keys()) + ['is_anomaly'])
    features_df.columns = features_df.columns.astype(str)
    if log_type == 'iis':
        log_df = parse_iis_logs.read_log(log_file)
    elif log_type == 'apache':
        log_df = parse_apache_logs.read_log(log_file)
    elif log_type == 'squid':
        log_df = parse_squid_logs.read_log(log_file)
    elif log_type == 'custom_app':
        log_df = parse_app_logs.read_log(log_file)
    else:
        print('Log Type not Supported')
        sys.exit()

    features_dataframe = extract_features(log_df, parameters, features_df)
    if mode == "Predict":
        result = prediction(features_dataframe, model_name)
        save_results(result, output_file)
    elif mode == "Train":
        result = training(features_dataframe, model_name)

    print(result)
