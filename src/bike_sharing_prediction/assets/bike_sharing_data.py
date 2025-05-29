"""Bike sharing data asset.

This module defines a Dagster asset for loading the raw bike sharing data.
"""

import dagster as dg
import pandas as pd

from bike_sharing_prediction.config import ASSET_GROUP_DATA_INGESTION
from bike_sharing_prediction.resources.data_loaders import DataLoader


@dg.asset(
    io_manager_key="csv_io_manager",
    group_name=ASSET_GROUP_DATA_INGESTION,
    compute_kind="pandas",
)
def bike_sharing_data(
    context: dg.AssetExecutionContext, data_loader: DataLoader
) -> pd.DataFrame:
    """Load the bike sharing data using the provided data loader resource.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster context used for logging and metadata tracking.
    data_loader : DataLoader
        A resource that loads the bike sharing data as a DataFrame.

    Returns
    -------
    pd.DataFrame
        Raw bike sharing data.
    """
    raw_data = data_loader.load()

    context.add_output_metadata(
        {
            "nr_rows": len(raw_data),
            "columns": raw_data.columns.to_list(),
        }
    )

    return raw_data
