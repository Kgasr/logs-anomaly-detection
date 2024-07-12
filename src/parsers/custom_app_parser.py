import re
import pandas as pd
from .base_parser import BaseParser


class CustomAppParser(BaseParser):
    def parse_logs(self):
        log_data = []
        with open(self.file_path, 'r') as file:
            for line in file:
                if line.startswith('#'):
                    continue  # skip comments
                log_data.append(line)
        # Define a regular expression to match log lines
        log_pattern = re.compile(r'^(?P<datetime>\S+ \S+) \[(?P<status>\w+)\] \[(?P<cip>\S+)\] (?P<message>.*)$')

        # Parse log lines
        parsed_logs = []
        for line in log_data:
            match = log_pattern.match(line)
            if match:
                parsed_logs.append(match.groupdict())
        column_names = ['datetime', 'status', 'cip', 'message']
        # Create a DataFrame
        df = pd.DataFrame(parsed_logs, columns=column_names)

        # Convert the datetime column to a datetime object and format it
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
        df.drop(columns=['status'])
        df['status'] = df['message'].str.extract(r'(ERR\d)')
        df['status'] = df['status'].fillna('NO_ERR')

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        return df
