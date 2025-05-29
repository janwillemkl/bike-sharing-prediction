"""Bike sharing demand prediction pipeline."""

import os

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
from bike_sharing_prediction.config import (
    LAKEFS_BRANCH,
)
from bike_sharing_prediction.io_managers.csv_lakefs_io_manager import (
    CSVLakeFSIOManager,
)
from bike_sharing_prediction.io_managers.pkl_fs_io_manager import (
    PickleFileSystemIOManager,
)
from bike_sharing_prediction.resources.data_loaders import (
    LakeFSDataLoader,
)
from bike_sharing_prediction.resources.lakefs_client import LakeFSClient

lakefs_client = LakeFSClient(
    host=os.getenv("LAKEFS_HOST"),
    username=os.getenv("LAKEFS_USERNAME"),
    password=os.getenv("LAKEFS_PASSWORD"),
)

lakefs_data_loader = LakeFSDataLoader(
    lakefs_client=lakefs_client,
    lakefs_repository=os.getenv("LAKEFS_REPOSITORY"),
    lakefs_branch=LAKEFS_BRANCH,
)

csv_lakefs_io_manager = CSVLakeFSIOManager(
    lakefs_client=lakefs_client,
    lakefs_repository=os.getenv("LAKEFS_REPOSITORY"),
)

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
        "data_loader": lakefs_data_loader,
        "lakefs_client": lakefs_client,
        "csv_io_manager": csv_lakefs_io_manager,
        "pkl_io_manager": PickleFileSystemIOManager(base_dir="dg_models"),
    },
)
