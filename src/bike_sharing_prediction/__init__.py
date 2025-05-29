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
    LAKEFS_HOST,
    LAKEFS_PASSWORD,
    LAKEFS_REPOSITORY,
    LAKEFS_USERNAME,
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
from bike_sharing_prediction.sensor import new_data_sensor

lakefs_client = LakeFSClient(
    host=LAKEFS_HOST,
    username=LAKEFS_USERNAME,
    password=LAKEFS_PASSWORD,
)

lakefs_data_loader = LakeFSDataLoader(
    lakefs_client=lakefs_client,
    lakefs_repository=LAKEFS_REPOSITORY,
    lakefs_branch=LAKEFS_BRANCH,
)

csv_lakefs_io_manager = CSVLakeFSIOManager(
    lakefs_client=lakefs_client,
    lakefs_repository=LAKEFS_REPOSITORY,
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
    sensors=[
        new_data_sensor,
    ],
)
