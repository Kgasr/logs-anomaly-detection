from src import LogAnomalyDetection

processing_mode = "predict"
log_source_type = "squid"
log_source_file = 'D:\\Technical\\Codebase\\Python\\logs-anomaly-detection\\data\\squid\\sample1.log'
patterns_file = 'D:\\Technical\\Codebase\\Python\\logs-anomaly-detection\\patterns.yaml'


def main():
    try:
        logs_anomaly_detection = LogAnomalyDetection(processing_mode, log_source_type, log_source_file, patterns_file)
        results = logs_anomaly_detection.logs_anomaly_detection()
        print(results)
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main()
