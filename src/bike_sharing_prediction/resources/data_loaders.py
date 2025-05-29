"""Data loaders for the bike sharing prediction project.

This module provides utilities to load monthly CSV data files from a specified
directory and combine them into a single pandas DataFrame.
"""

import glob
import os

import dagster as dg
import pandas as pd

from bike_sharing_prediction.resources.lakefs_client import LakeFSClient


class DataLoader(dg.ConfigurableResource):
    def load(self) -> pd.DataFrame:
        raise NotImplementedError()


class FileSystemDataLoader(DataLoader):
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


class LakeFSDataLoader(DataLoader):
    """Data loader that reads CSV files from a LakeFS repository.

    Attributes
    ----------
    lakefs_client : LakeFSClient
        Client for interacting with the LakeFS filesystem.
    lakefs_repository : str
        Name of the repository in LakeFS to load the data from.
    lakefs_branch : str
        Branch name in the LakeFS repository.
    data_path : str
        Path within the repository where the CSV files are located.
        Defaults to 'bike_sharing_data'.
    """

    lakefs_client: LakeFSClient
    lakefs_repository: str
    lakefs_branch: str

    data_path: str = "bike_sharing_data"

    def load(self) -> pd.DataFrame:
        """Load and combine all CSV files from the specified LakeFS path.

        Returns
        -------
        pd.DataFrame
            Combined DataFrame of all CSV files found in the specified path.

        Raises
        ------
        FileNotFoundError
            If no CSV files are found in the given LakeFS path.
        """

        # Get a LakeFS file system
        fs = self.lakefs_client.get_filesystem()

        raw_data_uri = f"lakefs://{self.lakefs_repository}/{self.lakefs_branch}/{self.data_path}/"

        # Retrieve a list of all CSV files under the raw data path
        csv_files = []

        for obj in fs.ls(raw_data_uri):
            csv_file_path = f"lakefs://{obj['name']}"
            if csv_file_path.endswith(".csv"):
                csv_files.append(csv_file_path)

        csv_files.sort()

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {raw_data_uri}")

        # Load the CSV files one by one and combine them into one DataFrame
        data_per_month = []
        for csv_file in csv_files:
            with fs.open(csv_file) as f:
                data_per_month.append(pd.read_csv(f))

        return pd.concat(data_per_month, ignore_index=True)
