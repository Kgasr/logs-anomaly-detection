import os

import joblib
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import classification_report


class TrainAndPredict:
    def __init__(self, features, model_name):
        self.__features = features
        self.__model_name = model_name

    # Returns features (X) and labels (y)
    def __setup_features(self):
        # print(features)
        X = self.__features.drop(columns=['cip', 'datetime', 'is_anomaly'])
        y = self.__features['is_anomaly'].astype(int)
        return X, y

    # Trains the model and save it to supplied location
    def training(self):
        X, y = self.__setup_features()
        # model = IsolationForest(contamination=0.01, random_state=42)
        model = RandomForestClassifier(random_state=4)
        model.fit(X, y)
        joblib.dump(model, self.__model_name)
        model_path = os.path.abspath(self.__model_name)
        return f'"{model_path}" Model saved successfully.'

    # Loads the model from supplied location and makes prediction to return anomalous records.
    def prediction(self):
        try:
            model = joblib.load(self.__model_name)
            X, y = self.__setup_features()
            predictions = model.predict(X)
            print(X,y)
            # print(classification_report(y, predictions))
            self.__features['predicted_anomaly'] = predictions
            anomalous_records = self.__features[self.__features['predicted_anomaly'] == 1]
            print(anomalous_records)
            result = anomalous_records.drop(columns=['is_anomaly', 'predicted_anomaly'])
            return result
        except FileNotFoundError:
            raise Exception(f'Failed to load the "{self.__model_name}"')
