"""Pickle Filesystem IO manager."""

import os

import dagster as dg
import joblib
from sklearn.base import BaseEstimator


class PickleFileSystemIOManager(dg.ConfigurableIOManager):
    base_dir: str
    extension: str = ".pkl"

    def _get_path(self, context: dg.OutputContext | dg.InputContext) -> str:
        """Construct the full file path from the given asset context.

        Parameters
        ----------
        context : dg.OutputContext or dg.InputContext
            Dagster asset context.

        Returns
        -------
        str
            Path to the pickle file.
        """

        return (
            os.path.join(self.base_dir, *context.asset_key.path)
            + self.extension
        )

    def handle_output(
        self, context: dg.OutputContext, obj: BaseEstimator
    ) -> None:
        """Save a scikit-learn compatible model as pickle file on the local
        filesystem.

        Parameters
        ----------
        context : dg.OutputContext
            Dagster asset context.
        obj : BaseEstimator
            A scikit-learn compatible model to be written to disk.
        """

        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        joblib.dump(obj, path)

    def load_input(self, context: dg.InputContext) -> BaseEstimator:
        """Load a scikit-learn compatible model from a pickle file on the local
        file system.

        Parameters
        ----------
        context : dg.InputContext
            Dagster asset context

        Returns
        -------
        BaseEstimator
            Scikit-learn compatible model loaded from the pickle file.
        """

        path = self._get_path(context)
        return joblib.load(path)
