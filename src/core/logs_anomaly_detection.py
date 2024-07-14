import os
import pandas as pd
from datetime import datetime
from ..config import read_config, Config
from .train_and_predict import TrainAndPredict
from ..utils import CustomLogger, DataProcessing

_params_file = os.path.abspath('src/config/params.yaml')



# Saves the Prediction results to the output file. Archives if output file already existing before writing the results
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


class LogAnomalyDetection:

    def __init__(self, processing_mode, log_source_type, log_source_file, patterns_file):
        self.__params = None
        self.__config = Config(processing_mode, log_source_type, log_source_file, patterns_file)
        self.__params_file = _params_file
        self.__custom_logger = CustomLogger(self.__params_file).get_logger()

    # Main method
    def logs_anomaly_detection(self):
        try:
            self.__custom_logger.info("Logs anomaly detection process started")
            self.__params = read_config(self.__params_file)
            self.__custom_logger.debug(f'Config file "{self.__params_file}" successfully read')
            dp = DataProcessing(self.__custom_logger)
            features_dataframe = dp.data_processing(self.__config.patterns_file,
                                                    self.__config.log_source_type,
                                                    self.__config.log_source_file)
            self.__custom_logger.debug("Features dataframe created")
            op = self.__process(features_dataframe)
            self.__custom_logger.info("Logs anomaly detection process ended")
            return op
        except Exception as e:
            msg = f"Logs anomaly detection process failed : {e}"
            self.__custom_logger.error(msg)
            raise Exception(e)

    # Based on supplied processing mode, it performs the training or prediction processing
    def __process(self, features):
        mode = self.__config.processing_mode.upper()
        model_name = f"models/{self.__config.log_source_type.lower()}.pkl"
        self.__custom_logger.debug(f'"{mode}" Process Initiated')
        train_and_predict = TrainAndPredict(features, model_name)
        if mode == "PREDICT":
            prediction_result = train_and_predict.prediction()
            result_msg = save_results(prediction_result, self.__params['output_file'])
        elif mode == "TRAIN":
            result_msg = train_and_predict.training()
        else:
            raise Exception('Invalid processing mode supplied. It should be "TRAIN" or "PREDICT"')
        self.__custom_logger.info(f'"{mode}" Process ended with message :- {result_msg}')
        return result_msg


"""
config_file = 'D:\Technical\Codebase\Python\logs-anomaly-detection\src\config\config.yaml'
custom_logger = CustomLogger(config_file).get_logger()

def process(features, processing_mode, model, op_file):
    custom_logger.debug(f"'{processing_mode}' Process Initiated")
    if processing_mode == "Predict":
        prediction_result = prediction(features, model)
        result_msg = save_results(prediction_result, op_file)
    elif processing_mode == "Train":
        result_msg = training(features, model)
    custom_logger.info(result_msg)
    return result_msg


# Saves the Prediction results to the output file. Archives if output file already existing before writing the results
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


# Main method
def logs_anomaly_detection():
    try:
        custom_logger.debug("Logs anomaly detection process started")
        config = read_config(config_file)
        custom_logger.debug("Config file successfully read")
        features_dataframe = data_processing(config['patterns_file'], config['log_source'], config['log_source_file'])
        custom_logger.debug("Feature")
        process(features_dataframe, config['processing_mode'], config['model_name'], config['output_file'])
        custom_logger.debug("Logs anomaly detection process ended")
    except Exception as e:
        msg = f"Logs anomaly detection process failed : {e}"
        custom_logger.error(msg)
        raise Exception(e)
"""
