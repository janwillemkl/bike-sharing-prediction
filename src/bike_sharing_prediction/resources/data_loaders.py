"""Data loaders for the bike sharing prediction project.

This module provides utilities to load monthly CSV data files from a specified
directory and combine them into a single pandas DataFrame.
"""

import glob
import os

import dagster as dg
import pandas as pd


class DataLoader(dg.ConfigurableResource):
    """Data loader that reads monthly CSV files from a directory.

    Attributes
    ----------
    data_directory : str
        Path to the directory containing the monthly CSV files.

    Raises
    ------
    FileNotFoundError
        If no CSV files are found in the specified directory.
    """

    data_directory: str = "./data"

    def load(self) -> pd.DataFrame:
        """Load and combine all CSV files in the data directory.

        Returns
        -------
        pd.DataFrame
            A single DataFrame containing data from all monthly CSV files.
        """
        csv_files = sorted(
            glob.glob(os.path.join(self.data_directory, "*.csv"))
        )

        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in {self.data_directory}"
            )

        data_per_month = [pd.read_csv(file) for file in csv_files]
        return pd.concat(data_per_month, ignore_index=True)
