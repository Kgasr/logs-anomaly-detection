import yaml


# Config class to hold the mandatory variables
class Config:
    def __init__(self, processing_mode, log_source_type, log_source_file,patterns_file):
        self.processing_mode = processing_mode
        self.log_source_type = log_source_type
        self.log_source_file = log_source_file
        self.patterns_file = patterns_file


# Reads YAML config file from given path.
# Returns the specific section content if supplied in method call or returns overall file content
def read_config(file_path, target_section=None):
    try:
        with open(file_path, 'r') as file:
            content = yaml.safe_load(file)
            if content is None:
                raise ValueError("YAML file is empty")
            if target_section:
                if target_section in content:
                    return content[target_section]
                else:
                    raise KeyError(f"Section '{target_section}' not found in the YAML file")
            else:
                return content
    except FileNotFoundError:
        raise Exception(f"Error: The file '{file_path}' was not found.")
    except yaml.YAMLError as e:
        raise Exception(f"Error parsing YAML file: {e}")
    return None
