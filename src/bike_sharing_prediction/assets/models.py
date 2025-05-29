"""Bike sharing prediction model assets."""

import dagster as dg
import mlflow
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import (
    mean_absolute_error,
    r2_score,
    root_mean_squared_error,
    root_mean_squared_log_error,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from bike_sharing_prediction.config import (
    ASSET_GROUP_MODEL_TRAINING,
    AUTHOR,
    CATEGORICAL_FEATURES,
    FEATURES,
    NUMERICAL_FEATURES,
    RANDOM_STATE,
    TARGET,
)
from bike_sharing_prediction.utils import mlflow_get_run_id


def build_pipeline(estimator: BaseEstimator):
    """Construct a preprocessing and modeling pipeline using the provided
    estimator.

    Applies standard scaling to numerical features and one-hot encoding to
    categorical features.

    Parameters
    ----------
    estimator : BaseEstimator
        A scikit-learn compatible estimator (e.g., LinearRegression)

    Returns
    -------
    Pipeline
        A scikit-learn pipeline with preprocessing and estimator steps.
    """

    numerical_transformer = Pipeline(
        [
            (
                "scaler",
                StandardScaler(),
            )
        ]
    )

    categorical_transformer = Pipeline(
        [
            (
                "encoder",
                OneHotEncoder(sparse_output=False, handle_unknown="ignore"),
            )
        ]
    )

    preprocessor = ColumnTransformer(
        [
            ("numerical", numerical_transformer, NUMERICAL_FEATURES),
            ("categorical", categorical_transformer, CATEGORICAL_FEATURES),
        ]
    )

    return Pipeline(
        [
            ("preprocessor", preprocessor),
            ("estimator", estimator),
        ]
    )


def compute_performance_metrics(
    ground_truth: np.ndarray | pd.Series, predictions: np.ndarray | pd.Series
) -> dict[str, float]:
    """Compute regression metrics between ground truth and predictions.

    Parameters
    ----------
    ground_truth : np.ndarray or pd.Series
        The actual target values.
    predictions : np.ndarray or pd.Series
        The predicted target values.

    Returns
    -------
    dict[str, float]
        Dictionary containing RMSE, RMSLE, MAE, and R2 score.
    """

    return {
        "RMSE": root_mean_squared_error(ground_truth, predictions),
        "R2": r2_score(ground_truth, predictions),
        "MAE": mean_absolute_error(ground_truth, predictions),
        "RMSLE": root_mean_squared_log_error(ground_truth, predictions),
    }


def create_model_asset(
    estimator: BaseEstimator, asset_name: str, description: str
) -> dg.AssetsDefinition:
    """Factory function to generate a Dagster asset for training and evaluating
    a regression model for predicting bike sharing demand.

    Parameters
    ----------
    estimator : BaseEstimator
        The scikit-learn estimator to use (e.g., LinearRegression)
    asset_name : str
        The name of the resulting Dagster asset.
    description : str
        Description of the asset for Dagster UI.

    Returns
    -------
    dg.AssetsDefinition
        A Dagster asset that fits a model and logs evaluation metrics.
    """

    @dg.asset(
        name=asset_name,
        description=description,
        group_name=ASSET_GROUP_MODEL_TRAINING,
        compute_kind="scikit-learn",
        io_manager_key="pkl_io_manager",
    )
    def regression_model(
        context: dg.AssetExecutionContext,
        train_data_log_transformed: pd.DataFrame,
        test_data: pd.DataFrame,
    ) -> BaseEstimator:
        """Train a regression model and log evaluation metrics.

        The model parameters, performance metrics, and model file are logged
        to MLflow.

        Parameters
        ----------
        context : dg.AssetExecutionContext
            Dagster context used for logging and metadata tracking
        train_data_log_transformed : pd.DataFrame
            The training set with a log-transformed target.
        test_data : pd.DataFrame
            The test set for evaluation.

        Returns
        -------
        BaseEstimator
            A scikit-learn compatible pipeline that is fitted and evaluated.
        """
        dagster_run_id = context.run.run_id[:8]

        pipeline_run_name = f"pipeline-{dagster_run_id}"
        pipeline_run_id = mlflow_get_run_id(pipeline_run_name)

        training_run_name = f"{asset_name}-{dagster_run_id}"

        # Outer mlflow run is shared across the pipeline run, nested training
        # run is below.
        with (
            mlflow.start_run(
                run_name=pipeline_run_name,
                run_id=pipeline_run_id,
            ),
            mlflow.start_run(
                run_name=training_run_name,
                nested=True,
            ),
        ):
            # Log run metadata to mlflow
            mlflow.set_tags(
                {
                    "author": AUTHOR,
                    "dagster_run_id": context.run.run_id,
                },
            )

            # Fit the pipeline
            pipeline = build_pipeline(estimator)
            pipeline.fit(
                train_data_log_transformed[FEATURES],
                train_data_log_transformed[TARGET],
            )

            # Log model parameters to mlflow
            mlflow.log_params(pipeline.get_params())

            # Create predictions
            predictions = pipeline.predict(test_data[FEATURES])
            predictions = np.expm1(predictions)

            # Compute and log performance metrics to mlflow and dagster
            metrics = compute_performance_metrics(
                test_data[TARGET], predictions
            )

            mlflow.log_metrics(metrics)
            context.add_output_metadata(metrics)

            # Log the model to mlflow
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                registered_model_name=None,  # Do not register the model
            )

        return pipeline

    return regression_model


linear_regression_model = create_model_asset(
    estimator=LinearRegression(),
    asset_name="linear_regression_model",
    description="Linear regression model for predicting bike sharing demand.",
)

random_forest_regressor_model = create_model_asset(
    estimator=RandomForestRegressor(random_state=RANDOM_STATE),
    asset_name="random_forest_regressor_model",
    description="Random forest regressor model for predicting bike sharing demand.",
)
