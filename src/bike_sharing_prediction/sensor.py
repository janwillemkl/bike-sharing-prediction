"""Dagster sensor for detecting new data."""

import dagster as dg
import lakefs

from bike_sharing_prediction.config import LAKEFS_BRANCH, LAKEFS_REPOSITORY
from bike_sharing_prediction.resources.lakefs_client import LakeFSClient

materialize_assets_job = dg.define_asset_job(
    "materialize_assets", dg.AssetSelection.all()
)


@dg.sensor(
    job=materialize_assets_job,
    minimum_interval_seconds=10,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def new_data_sensor(
    context: dg.SensorEvaluationContext,
    lakefs_client: LakeFSClient,
) -> dg.RunRequest | dg.SkipReason:
    """Sensor that triggers a pipeline run when a new commit appears on the
    LakeFS main branch.

    Parameters
    ----------
    context : dg.SensorEvaluationContext
        Dagster context used for updating the cursor state.
    lakefs_client : LakeFSClient
        Client for interacting with LakeFS.

    Returns
    -------
    dg.RunRequest or dg.SkipReason
        A RunRequest if a new commit is found; otherwise a SkipReason
        indicating no new data.
    """
    client = lakefs_client.get_filesystem().client

    target_branch = lakefs.Branch(LAKEFS_REPOSITORY, LAKEFS_BRANCH, client)
    latest_commit = target_branch.head.id

    previous_commit = context.cursor if context.cursor else ""

    if latest_commit != previous_commit:
        context.update_cursor(latest_commit)
        return dg.RunRequest(run_key=latest_commit)

    return dg.SkipReason("No new data detected.")
