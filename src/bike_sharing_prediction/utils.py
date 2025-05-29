"""Utility functions."""

import random
import time

import mlflow


def mlflow_get_run_id(run_name: str) -> str | None:
    """Retrieve the run ID of the MLflow run with the given name.

    Introduces a random short delay to reduce race conditions when multiple
    processes are attempting to query or create runs simultaneously.
    """
    # Random short delay to reduce race conditions
    time.sleep(random.uniform(0, 5))

    current_runs = mlflow.search_runs(
        filter_string=f"attributes.`run_name`='{run_name}'",
        output_format="list",
    )

    if current_runs:
        return current_runs[0].info.run_id
    return None
