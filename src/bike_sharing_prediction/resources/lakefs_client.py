"""LakeFS client resource."""

import dagster as dg
from lakefs_spec import LakeFSFileSystem


class LakeFSClient(dg.ConfigurableResource):
    """A Dagster resource for configuring access to a LakeFS filesystem.

    Attributes
    ----------
    host : str
        The URL of the LakeFS server.
    username : str
        LakeFS username for authentication.
    password : str
        LakeFS password or access key.
    """

    host: str
    username: str
    password: str

    def get_filesystem(self) -> LakeFSFileSystem:
        """Create a new authenticated LakeFSFileSystem instance.

        Returns
        -------
        LakeFSFileSystem
            A file system object configured to interact with the LakeFS API.
        """
        return LakeFSFileSystem(
            host=self.host, username=self.username, password=self.password
        )
