import pandas as pd
from src.config.config import read_config
from src.core.get_parser import get_parser


class DataProcessing:
    def __init__(self, logger):
        self.__logger = logger

    # Main method for data processing
    def data_processing(self,patterns_file, log_type, log_file):
        self.__logger.info('Data Processing started')
        patterns, features_df = self.__read_anomaly_patterns_n_setup_features(patterns_file)
        log_df = self.__parse_logs(log_type, log_file)
        features_dataframe = self.__extract_features(log_df, patterns, features_df)
        self.__logger.info('Data Processing ended')
        return features_dataframe

    # Reads anomaly patterns from pattern file and set up an empty dataframe with columns names
    def __read_anomaly_patterns_n_setup_features(self, pattern_file):
        patterns = read_config(pattern_file)
        df = pd.DataFrame(
            columns=['cip', 'datetime', 'num_requests'] + list(patterns['error_codes'].keys()) + ['is_anomaly'])
        df.columns = df.columns.astype(str)
        self.__logger.info(f'Anomaly patterns successfully read from "{pattern_file}"')
        return patterns, df

    # Gets the required log parser based on log type and parse logs into a dataframe.
    def __parse_logs(self, log_type, log_file):
        try:
            parser = get_parser(log_type.upper(), log_file)
            df = parser.parse_logs()
            self.__logger.info(f'"{log_type.upper()}" logs at location "{log_file}" successfully parsed')
            return df
        except Exception as ex:
            raise Exception(f'Error while parsing logs for "{log_type}" from "{log_file}"')

    # Extracts the features based on anomaly patterns/params from the logs dataframe
    # and append it to empty features dataframe
    def __extract_features(self,log_df, params, features):
        self.__logger.info("Features extraction process started")
        error_codes = params['error_codes']
        high_request_threshold = params['high_request_threshold']
        time_window = params['time_window']

        # Ensure datetime is sorted
        log_df = log_df.sort_values(by='datetime')

        datetime_list = log_df['datetime'].tolist()
        window_size = pd.Timedelta(time_window)

        features_list = []
        start_time = datetime_list[0]

        while start_time is not None:
            window_end = start_time + window_size
            window_data = log_df[(log_df['datetime'] >= start_time) & (log_df['datetime'] < window_end)]
            if not window_data.empty:
                grouped = window_data.groupby('cip')

                for ip, group in grouped:
                    num_requests = group.shape[0]
                    error_counts = {str(code): sum(group['status'] == error_codes[code]['status']) for code in
                                    error_codes}
                    is_anomaly_errors = [error_counts[str(code)] >= error_codes[code]['threshold'] for code in
                                         error_codes]
                    is_anomaly_requests = num_requests >= high_request_threshold
                    is_anomaly = any(is_anomaly_errors) or is_anomaly_requests
                    df_to_concat = {
                        'cip': ip,
                        'datetime': start_time,
                        'num_requests': num_requests,
                        **error_counts,
                        'is_anomaly': is_anomaly
                    }
                    features_list.append(df_to_concat)
            start_time = next((dt for dt in datetime_list if dt > window_end), None)

        features = pd.concat([features, pd.DataFrame(features_list)], ignore_index=True)
        self.__logger.info("Features extraction process completed successfully")
        return features


"""
config_file = 'D:\Technical\Codebase\Python\logs-anomaly-detection\src\config\config.yaml'
custom_logger = CustomLogger(config_file).get_logger()


# Main method for data processing
def data_processing(patterns_file, log_type, log_file):
    patterns, features_df = read_anomaly_patterns_n_setup_features(patterns_file)
    log_df = parse_logs(log_type, log_file)
    features_dataframe = extract_features(log_df, patterns, features_df)
    return features_dataframe


# Reads anomaly patterns from pattern file and set up an empty dataframe with columns names
def read_anomaly_patterns_n_setup_features(pattern_file):
    patterns = read_config(pattern_file)
    df = pd.DataFrame(
        columns=['cip', 'datetime', 'num_requests'] + list(patterns['error_codes'].keys()) + ['is_anomaly'])
    df.columns = df.columns.astype(str)
    return patterns, df


# Gets the required log parser based on log type and parse logs into a dataframe.
def parse_logs(log_type, log_file):
    parser = get_parser(log_type.upper(), log_file)
    df = parser.parse_logs()
    custom_logger.info("22")
    return df


# Extracts the features based on anomaly patterns/params from the logs dataframe
# and append it to empty features dataframe
def extract_features(log_df, params, features):
    error_codes = params['error_codes']
    high_request_threshold = params['high_request_threshold']
    time_window = params['time_window']

    # Ensure datetime is sorted
    log_df = log_df.sort_values(by='datetime')

    datetime_list = log_df['datetime'].tolist()
    window_size = pd.Timedelta(time_window)

    features_list = []
    start_time = datetime_list[0]

    while start_time is not None:
        window_end = start_time + window_size
        window_data = log_df[(log_df['datetime'] >= start_time) & (log_df['datetime'] < window_end)]
        if not window_data.empty:
            grouped = window_data.groupby('cip')

            for ip, group in grouped:
                num_requests = group.shape[0]
                error_counts = {str(code): sum(group['status'] == error_codes[code]['status']) for code in
                                error_codes}
                is_anomaly_errors = [error_counts[str(code)] >= error_codes[code]['threshold'] for code in
                                     error_codes]
                is_anomaly_requests = num_requests >= high_request_threshold
                is_anomaly = any(is_anomaly_errors) or is_anomaly_requests
                df_to_concat = {
                    'cip': ip,
                    'datetime': start_time,
                    'num_requests': num_requests,
                    **error_counts,
                    'is_anomaly': is_anomaly
                }
                features_list.append(df_to_concat)
        start_time = next((dt for dt in datetime_list if dt > window_end), None)

    features = pd.concat([features, pd.DataFrame(features_list)], ignore_index=True)
    return features


"""
"""
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

        features = features._append({
            'cip': ip,
            'datetime': time,
            'num_requests': num_requests,
            **error_counts,
            'is_anomaly': is_anomaly
        }, ignore_index=True)


    return features
"""
