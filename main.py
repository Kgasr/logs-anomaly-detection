import sys
import pandas as pd

from train_and_predict import training, prediction
from common import read_parameters, extract_features
from parsers import parse_app_logs, parse_squid_logs, parse_iis_logs, parse_apache_logs


def initialize():
    mode = 'Predict'
    """
    # IIS Setup
    model_name = 'Models/iis_model.pkl'
    log_type = 'iis'
    log_file = 'TestData/IIS Logs/iis_sample.log'
    """

    # Apache Setup
    model_name = 'Models/apache_model.pkl'
    log_type = 'apache'
    log_file = 'TestData/Apache Logs/access.logs'

    """
    # Squid Setup
    model_name = 'Models/squid_model.pkl'
    log_type = 'squid'
    log_file = 'TestData/Squid Logs/sample.log'
    """
    """
    # Custom App Setup
    model_name = 'Models/app_model.pkl'
    log_type = 'custom_app'
    log_file = 'TestData/Sample Application Logs/app_logs.log'
    """

    return mode, model_name, log_type, log_file


if __name__ == "__main__":
    mode, model_name, log_type, log_file = initialize()
    parameters = read_parameters('pattern.json')
    features_df = pd.DataFrame(columns=['cip', 'datetime', 'num_requests'] + list(parameters['error_codes'].keys()) + ['is_anomaly'])

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
    elif mode == "Train":
        result = training(features_dataframe, model_name)

    print(result)
