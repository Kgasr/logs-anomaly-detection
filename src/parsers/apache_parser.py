import re
import pandas as pd
from datetime import datetime
from .base_parser import BaseParser


# Apache Log format parser class
class ApacheParser(BaseParser):
    def parse_logs(self):
        with open(self.file_path, 'r') as f:
            log_lines = f.readlines()

        log_data = [parse_log_line(line) for line in log_lines]
        log_data = [entry for entry in log_data if entry is not None]

        # Create a DataFrame
        df = pd.DataFrame(log_data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        return df


# Define a function to parse each log entry
def parse_log_line(line):
    pattern = (
        r'(?P<cip>\S+) '  # IP Address
        r'(?P<cidentity>\S+) '  # Identity 
        r'(?P<userid>\S+) '  # User ID
        r'\[(?P<datetime>.+?)\] '  # Timestamp
        r'"(?P<request>.*?)" '  # Request
        r'(?P<status>\d+) '  # Status Code
        r'(?P<bytes>\d+) '  # Bytes
        r'"(?P<referer>.*?)" '  # Referer
        r'"(?P<useragent>.*?)"'  # User Agent
    )
    match = re.match(pattern, line)
    if match:
        data = match.groupdict()
        data['datetime'] = datetime.strptime(data['datetime'], '%d/%b/%Y:%H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')
        return data
    return None
