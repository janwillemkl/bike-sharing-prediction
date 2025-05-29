"""Bike sharing demand prediction pipeline."""

import dagster as dg

from bike_sharing_prediction.assets.bike_sharing_data import bike_sharing_data
from bike_sharing_prediction.assets.bike_sharing_features import (
    bike_sharing_features,
)
from bike_sharing_prediction.assets.models import (
    linear_regression_model,
    random_forest_regressor_model,
)
from bike_sharing_prediction.assets.train_data_log_transformed import (
    train_data_log_transformed,
)
from bike_sharing_prediction.assets.train_test import train_test
from bike_sharing_prediction.io_managers.csv_fs_io_manager import (
    CSVFileSystemIOManager,
)
from bike_sharing_prediction.io_managers.pkl_fs_io_manager import (
    PickleFileSystemIOManager,
)
from bike_sharing_prediction.resources.data_loaders import DataLoader

definitions = dg.Definitions(
    assets=[
        bike_sharing_data,
        bike_sharing_features,
        train_test,
        train_data_log_transformed,
        linear_regression_model,
        random_forest_regressor_model,
    ],
    resources={
        "data_loader": DataLoader(),
        "csv_io_manager": CSVFileSystemIOManager(base_dir="dg_data"),
        "pkl_io_manager": PickleFileSystemIOManager(base_dir="dg_models"),
    },
)
