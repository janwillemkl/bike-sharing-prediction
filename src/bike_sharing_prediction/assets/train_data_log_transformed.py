"""Training set asset with the target log transformed."""

import dagster as dg
import numpy as np
import pandas as pd

from bike_sharing_prediction.config import (
    ASSET_GROUP_DATA_PREPROCESSING,
    TARGET,
)


@dg.asset(
    io_manager_key="csv_io_manager",
    group_name=ASSET_GROUP_DATA_PREPROCESSING,
    compute_kind="pandas",
)
def train_data_log_transformed(
    context: dg.AssetExecutionContext, train_data: pd.DataFrame
) -> pd.DataFrame:
    """Apply a log(1 + x) transformation to the target column of the training
    data.

    This is commonly used to normalize right-skewed distributions and improve
    the performance of regression models.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster context used for logging and metadata tracking.
    train_data : pd.DataFrame
        The original training dataset with the skewed target column.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the target column log-transformed.
    """
    train_data_log_transformed = train_data.copy()

    train_data_log_transformed[TARGET] = np.log1p(
        train_data_log_transformed[TARGET]
    )

    context.add_output_metadata({"nr_rows": len(train_data_log_transformed)})

    return train_data_log_transformed
