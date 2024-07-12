import os
import pandas as pd
from datetime import datetime

from ..config  import read_config
from ..utils import data_processing, CustomLogger

from .train_and_predict import training, prediction


print("KG", "1")
config_file = 'config/config.yaml'
custom_logger = CustomLogger(config_file).get_logger()
print("GK", "1")


def process(features, processing_mode, model, op_file):
    custom_logger.debug(f"'{processing_mode}' Process Initiated")
    if processing_mode == "Predict":
        prediction_result = prediction(features, model)
        result_msg = save_results(prediction_result, op_file)
    elif processing_mode == "Train":
        result_msg = training(features, model)
    custom_logger.info(result_msg)
    return result_msg


def save_results(result, output_file):
    if os.path.exists(output_file):
        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        directory, filename = os.path.split(output_file)
        filename, extension = os.path.splitext(filename)
        last_output_file_name = f"{filename}_{current_time}{extension}"
        last_output_file_path = os.path.join(directory + '/archive/', last_output_file_name)
        os.rename(output_file, last_output_file_path)
    absolute_path = os.path.abspath(output_file)
    result_df = pd.DataFrame(result)
    result_df.to_excel(output_file, index=False)
    return f"Prediction results saved in '{absolute_path}'"


def logs_anomaly_detection():
    try:
        custom_logger.debug("Logs anomaly detection process started")
        config = read_config(config_file)
        features_dataframe = data_processing(config['patterns_file'], config['log_source'], config['log_source_file'])
        process(features_dataframe, config['processing_mode'], config['model_name'], config['output_file'])
        custom_logger.debug("Logs anomaly detection process ended")
    except Exception as e:
        msg = f"Logs anomaly detection process failed : {e}"
        custom_logger.error(msg)
        raise Exception(e)
