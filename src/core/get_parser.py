from ..parsers import apache_parser, iis_parser, squid_parser, custom_app_parser


# Based on log file type, initializes the log parser with log file path
def get_parser(log_type, log_path):
    if log_type == 'IIS':
        parser = iis_parser.IISParser(log_path)
    elif log_type == 'APACHE':
        parser = apache_parser.ApacheParser(log_path)
    elif log_type == 'SQUID':
        parser = squid_parser.SquidParser(log_path)
    elif log_type == 'CUSTOM_APP':
        parser = custom_app_parser.CustomAppParser(log_path)
    else:
        raise Exception(f'Unable to load required parser as supplied log type "{log_type}" not supported')
    return parser
