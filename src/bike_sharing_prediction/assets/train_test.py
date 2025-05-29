"""Training and testing set assets."""

import dagster as dg
import pandas as pd
from sklearn.model_selection import train_test_split

from bike_sharing_prediction.config import (
    ASSET_GROUP_DATA_PREPROCESSING,
    RANDOM_STATE,
    TEST_SIZE,
)


@dg.multi_asset(
    outs={
        "train_data": dg.AssetOut(
            io_manager_key="csv_io_manager", description="Training set."
        ),
        "test_data": dg.AssetOut(
            io_manager_key="csv_io_manager", description="Test set."
        ),
    },
    group_name=ASSET_GROUP_DATA_PREPROCESSING,
    compute_kind="pandas",
)
def train_test(
    context: dg.AssetExecutionContext,
    bike_sharing_features: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split the features dataset into training and testing sets.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster context used for logging and metadata tracking.
    bike_sharing_features : pd.DataFrame
        The dataset containing the features and the target.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        The training and testing subsets of the dataset.
    """

    train_data, test_data = train_test_split(
        bike_sharing_features, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    context.add_output_metadata(
        {"nr_rows": len(train_data)}, output_name="train_data"
    )

    context.add_output_metadata(
        {"nr_rows": len(test_data)}, output_name="test_data"
    )

    return train_data, test_data
