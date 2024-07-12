import re
import pandas as pd
from .base_parser import BaseParser


class SquidParser(BaseParser):
    def parse_logs(self):
        log_data = []
        with open(self.file_path, 'r') as file:
            for line in file:
                if line.startswith('#'):
                    continue  # skip comments
                parts = line.split()
                log_data.append(parts)

        column_names = ['timestamp', 'response-time', 'cip', 'status', 'size', 'method', 'url', 'username', 'squid-status',
                        'content-type']
        df = pd.DataFrame(log_data, columns=column_names)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
        df['datetime'] = pd.to_datetime(df['datetime'])

        df['status'] = df['status'].apply(lambda x: re.search(r'\d{3}', x).group())
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        return df
