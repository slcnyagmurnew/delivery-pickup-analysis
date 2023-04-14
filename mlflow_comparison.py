import mlflow.catboost
import mlflow.lightgbm
import pandas as pd
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from catboost import CatBoostRegressor, Pool
import numpy as np
from lightgbm import LGBMRegressor
from sklearn import preprocessing
import json
import itertools
from urllib.parse import urlparse

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def eval_metrics(real, pred) -> dict:
    rmse = np.sqrt(mean_squared_error(y_true=real, y_pred=pred))
    mape = mean_absolute_percentage_error(y_true=real, y_pred=pred)
    r2 = r2_score(y_true=real, y_pred=pred)

    return {"rmse": rmse, "mape": mape, "r2": r2}


def mlflow_with_catboost(X_train, y_train, X_test, y_test, params: dict):
    with mlflow.start_run():
        n_estimators = params["n_estimators"]
        learning_rate = params["learning_rate"]
        cb = CatBoostRegressor(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            depth=3,
            verbose=False,
            random_state=1
        )

        train_pool = Pool(data=X_train, label=y_train, cat_features=["Road_traffic_density", "Type_of_vehicle",
                                                                     "Weatherconditions", "Vehicle_condition",
                                                                     "multiple_deliveries"])
        test_pool = Pool(X_test, cat_features=["Road_traffic_density", "Type_of_vehicle",
                                               "Weatherconditions", "Vehicle_condition", "multiple_deliveries"])
        cb.fit(train_pool)
        predictions = cb.predict(test_pool)
        metrics = eval_metrics(real=y_test, pred=predictions)

        for p in params:
            mlflow.log_param(p, params.get(p))

        for m in metrics:
            mlflow.log_metric(m, metrics[m])

        tracking_url = urlparse(url=mlflow.get_tracking_uri()).scheme

        if tracking_url != "file":
            mlflow.catboost.log_model(cb_model=cb, artifact_path="model",
                                      registered_model_name="CatBoostRegressorModel",
                                      pip_requirements="requirements.txt")
        else:
            mlflow.catboost.log_model(cb_model=cb, artifact_path="model",
                                      pip_requirements="requirements.txt")


def mlflow_with_lightgbm(X_train, y_train, X_test, y_test, params: dict):
    with mlflow.start_run():
        num_leaves = params["num_leaves"]
        boosting_type = params["boosting_type"]
        lgbm = LGBMRegressor(
            num_leaves=num_leaves,
            boosting_type=boosting_type
        )
        lgbm.fit(X=X_train, y=y_train, categorical_feature=["Road_traffic_density", "Type_of_vehicle",
                                                            "Weatherconditions", "Vehicle_condition",
                                                            "multiple_deliveries"])
        predictions = lgbm.predict(X_test)
        metrics = eval_metrics(real=y_test, pred=predictions)

        for p in params:
            mlflow.log_param(p, params.get(p))

        for m in metrics:
            mlflow.log_metric(m, metrics[m])

        tracking_url = urlparse(url=mlflow.get_tracking_uri()).scheme

        if tracking_url != "file":
            mlflow.lightgbm.log_model(lgb_model=lgbm, artifact_path="model",
                                      registered_model_name="LightGBM",
                                      pip_requirements="requirements.txt")
        else:
            mlflow.lightgbm.log_model(lgb_model=lgbm, artifact_path="model",
                                      pip_requirements="requirements.txt")


def encoding_data(df: pd.DataFrame) -> pd.DataFrame:
    label_encoder = preprocessing.LabelEncoder()
    df["Road_traffic_density"] = label_encoder.fit_transform(df["Road_traffic_density"])
    df["Type_of_vehicle"] = label_encoder.fit_transform(df["Type_of_vehicle"])
    df["Weatherconditions"] = label_encoder.fit_transform(df["Weatherconditions"])
    return df


def run_multiple_ml_models(X_train, X_test, y_train, y_test):
    with open("config.json", "r") as f:
        cfg = json.load(f)
    f.close()

    for model in cfg["models"]:
        model_name = model["model_name"]
        params = dict(model["parameters"])

        param_keys = list(params.keys())
        print(param_keys)
        param_values = list(params.values())
        print(param_values)

        for x in list(itertools.product(*param_values)):
            print(x)
            print(model_name.lower())
            if "cat" in model_name.lower():
                mlflow_with_catboost(X_train, y_train, X_test, y_test, params={param_keys[0]: x[0],
                                                                               param_keys[1]: x[1]})
            else:
                mlflow_with_lightgbm(X_train, y_train, X_test, y_test, params={param_keys[0]: x[0],
                                                                               param_keys[1]: x[1]})


if __name__ == '__main__':
    data = pd.read_csv("data/processed_data.csv")
    encoded_data = encoding_data(df=data)

    X = encoded_data[["Road_traffic_density", "Type_of_vehicle", "Distance(m)",
                      "Weatherconditions", "Vehicle_condition", "multiple_deliveries"]]
    y = encoded_data["Time_taken(min)"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=1)
    run_multiple_ml_models(X_train, X_test, y_train, y_test)
