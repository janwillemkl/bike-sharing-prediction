"""Bike sharing features asset."""

import dagster as dg
import pandas as pd

from bike_sharing_prediction.config import (
    ASSET_GROUP_DATA_PREPROCESSING,
    FEATURES,
    TARGET,
)


@dg.asset(
    io_manager_key="csv_io_manager",
    group_name=ASSET_GROUP_DATA_PREPROCESSING,
    compute_kind="pandas",
)
def bike_sharing_features(
    context: dg.AssetExecutionContext, bike_sharing_data: pd.DataFrame
) -> pd.DataFrame:
    """Select feature and target columns from the raw bike sharing dataset.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster context used for logging and metadata tracking.
    bike_sharing_data : pd.DataFrame
        Raw bike sharing data set containing all columns.

    Returns
    -------
    pd.DataFrame
        Subset of the data with only the feature columns and target.
    """

    features = bike_sharing_data[FEATURES + [TARGET]]

    context.add_output_metadata(
        {
            "nr_rows": len(features),
            "columns": features.columns.to_list(),
        }
    )

    return features
