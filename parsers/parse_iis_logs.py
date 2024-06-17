import pandas as pd


def read_log(file_path):
    log_data = []
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('#'):
                continue  # skip comments
            parts = line.split()
            log_data.append(parts)

    column_names = ['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 's-port', 'cs-username', 'cip',
                    'cs(User-Agent)', 'status', 'sc-substatus', 'sc-win32-status', 'sc-bytes', 'cs-bytes',
                    'time-taken']
    df = pd.DataFrame(log_data, columns=column_names)
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    return df
