"""CSV Filesystem IO manager."""

import os

import dagster as dg
import pandas as pd


class CSVFileSystemIOManager(dg.ConfigurableIOManager):
    """IO Manager for reading and writing pandas DataFrames as CSV files on the
    local filesystem.

    Attributes
    ----------
    base_dir : str
        Base directory where the asset files will be stored.
    extension : str
        File extension
    """

    base_dir: str
    extension: str = ".csv"

    def _get_path(self, context: dg.OutputContext | dg.InputContext) -> str:
        """Construct the full file path from the given asset context.

        Parameters
        ----------
        context : dg.OutputContext or dg.InputContext
            Dagster asset context.

        Returns
        -------
        str
            Path to the CSV file.
        """

        return (
            os.path.join(self.base_dir, *context.asset_key.path)
            + self.extension
        )

    def handle_output(
        self, context: dg.OutputContext, obj: pd.DataFrame
    ) -> None:
        """Save a pandas DataFrame to a CSV file on the local filesystem.

        Parameters
        ----------
        context : dg.OutputContext
            Dagster asset context.
        obj : pd.DataFrame
            DataFrame to be written to disk.
        """
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        obj.to_csv(path, index=False)

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        """Load a pandas DataFrame from a CSV file on the local filesystem.

        Parameters
        ----------
        context : dg.InputContext
            Dagster asset context.

        Returns
        -------
        pd.DataFrame
            DataFrame loaded from the CSV file.
        """
        path = self._get_path(context)
        return pd.read_csv(path)
