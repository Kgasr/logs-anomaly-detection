import joblib
from sklearn.ensemble import IsolationForest


def setup_features(features):
    X = features.drop(columns=['cip', 'datetime', 'is_anomaly'])
    y = features['is_anomaly']
    return X, y


def training(features, model_name):
    X, y = setup_features(features)
    print(X)
    print(y)
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(X, y)
    joblib.dump(model, model_name)
    return "Model saved successfully."


def prediction(features, model_name):
    model = joblib.load(model_name)
    X, y = setup_features(features)
    print(X)
    print(y)
    anomalies = model.predict(X)
    print(anomalies)
    print(anomalies == -1)
    print((anomalies == -1).astype(bool))
    print(y == (anomalies == -1).astype(bool))
    features['anomaly'] = (anomalies == -1).astype(bool)
    # anomalous_records = y == (anomalies == -1).astype(bool)
    anomalous_records = features[features['anomaly'] == True]
    print(anomalous_records)
    return anomalous_records
