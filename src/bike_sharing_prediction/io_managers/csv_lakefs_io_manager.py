"""CSV LakeFS IO Manager."""

import dagster as dg
import lakefs
import pandas as pd

from bike_sharing_prediction.config import LAKEFS_BRANCH
from bike_sharing_prediction.resources.lakefs_client import LakeFSClient


class CSVLakeFSIOManager(dg.ConfigurableIOManager):
    """IO Manager for reading and writing CSV files to LakeFS.

    Each run writes its outputs to a uniquely versioned branch based on the
    Dagster run ID. Assets are stored as CSV files in LakeFS using the
    specified base_path.

    Attributes
    ----------
    lakefs_client : LakeFSClient
        Client for interacting with the LakeFS filesystem.
    lakefs_repository : str
        Name of the repository in LakeFS.
    lakefs_branch_prefix : str
        Prefix to use when constructing the branch name. Defaults to "pipeline".
    base_path:
        Base path inside the LakeFS repository where assets will be stored.
        Defaults to "assets".
    extension : str
        File extension for stored data files. Defaults to ".csv".
    """

    lakefs_client: LakeFSClient

    lakefs_repository: str
    lakefs_branch_prefix: str = "pipeline"

    base_path: str = "assets"
    extension: str = ".csv"

    def _get_branch(self, context: dg.OutputContext | dg.InputContext) -> str:
        """Generate a LakeFS branch name based on the Dagster run ID.

        For output, this uses the current run's ID. For input, it uses the
        upstream output's run ID.

        Parameters
        ----------
        context : dg.OutputContext or dg.InputContext
            Dagster asset context.

        Returns
        -------
        str
            A branch name in the format '<prefix>-<run_id>'.
        """

        if isinstance(context, dg.OutputContext):
            run_id = context.run_id
        else:
            run_id = context.upstream_output.run_id

        return f"{self.lakefs_branch_prefix}-{run_id[:8]}"

    def _get_path(self, context: dg.OutputContext | dg.InputContext) -> str:
        """Construct the asset path within LakeFS for the given context.

        Parameters
        ----------
        context : dg.OutputContext or dg.InputContext
            Dagster asset context.

        Returns
        -------
        str
            A path to the asset in a LakeFS repository.
        """
        return f"{self.base_path}/{'/'.join(context.asset_key.path)}.{self.extension}"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame):
        """Write a DataFrame to LakeFS as a versioned CSV file and commit it.

        Creates a new LakeFS branch for the current Dagster run if it does not
        exist.

        Parameters
        ----------
        context : dg.OutputContext
            Dagster asset context.
        obj : pd.DataFrame
            The DataFrame to be written as CSV file to LakeFS.
        """

        branch = self._get_branch(context)
        path = self._get_path(context)

        fs = self.lakefs_client.get_filesystem()

        # Ensure the branch exists
        target_branch = lakefs.Branch(
            self.lakefs_repository,
            branch,
            fs.client,
        )
        target_branch.create(LAKEFS_BRANCH, exist_ok=True)

        # Upload the CSV file to LakeFS
        with fs.open(
            f"lakefs://{self.lakefs_repository}/{branch}/{path}", "wb"
        ) as f:
            obj.to_csv(f)

        # Commit changes
        target_branch.commit(
            message=f"Added asset {'/'.join(context.asset_key.path)}"
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        """Load a pandas DataFrame from a CSV file in LakeFS.

        Parameters
        ----------
        context : dg.InputContext
            Dagster asset context.

        Returns
        -------
        pd.DataFrame
            DataFrame loaded from the CSV file.
        """
        branch = self._get_branch(context)
        path = self._get_path(context)

        fs = self.lakefs_client.get_filesystem()

        with fs.open(f"lakefs://{self.lakefs_repository}/{branch}/{path}") as f:
            return pd.read_csv(f)
