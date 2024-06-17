import joblib
from sklearn.ensemble import IsolationForest


def setup_features(features):
    X = features.drop(columns=['cip', 'datetime', 'is_anomaly'])
    y = features['is_anomaly']
    return X, y


def training(features, model_name):
    X, y = setup_features(features)
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(X[y == False])
    joblib.dump(model, model_name)
    return "Model saved successfully."


def prediction(features, model_name):
    model = joblib.load(model_name)
    X, y = setup_features(features)
    anomalies = model.predict(X)
    y== (anomalies == -1).astype(bool)
    anomalous_records = features[features['is_anomaly'] == True]
    return anomalous_records
