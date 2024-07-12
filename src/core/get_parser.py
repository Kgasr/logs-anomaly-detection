from ..parsers import apache_parser, iis_parser, squid_parser, custom_app_parser


def get_parser(log_type, log_path):
    if log_type == 'iis':
        parser = iis_parser.IISParser(log_path)
    elif log_type == 'apache':
        parser = apache_parser.ApacheParser(log_path)
    elif log_type == 'squid':
        parser = squid_parser.SquidParser(log_path)
    elif log_type == 'custom_app':
        parser = custom_app_parser.CustomAppParser(log_path)
    else:
        raise Exception("Log Type not Supported")
    return parser
