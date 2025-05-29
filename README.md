# Bike Sharing Prediction

## Running MLflow

```
$ uv run mlflow server --port 4000
```

## Running LakeFS

From the `deploy` directory

```
$ docker compose up -d
```

## Running Dagster

```
export MLFLOW_TRACKING_URI="http://localhost:4000"
export MLFLOW_EXPERIMENT_NAME=bike_sharing_demand"

export LAKEFS_HOST="http://localhost:8000"
export LAKEFS_USERNAME="lakefs"
export LAKEFS_PASSWORD="appliedai"
export LAKEFS_REPOSITORY="bike-sharing-demand"
```

```
$ uv run dagster dev -m bike_sharing_prediction
```
