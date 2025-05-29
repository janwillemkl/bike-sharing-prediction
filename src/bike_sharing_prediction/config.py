NUMERICAL_FEATURES = [
    "temp",
    "hum",
    "windspeed",
]
CATEGORICAL_FEATURES = [
    "season",
    "mnth",
    "hr",
    "holiday",
    "weekday",
    "workingday",
    "weathersit",
]

FEATURES = NUMERICAL_FEATURES + CATEGORICAL_FEATURES

TARGET = "cnt"

RANDOM_STATE = 42

TEST_SIZE = 0.2

ASSET_GROUP_DATA_INGESTION = "data_ingestion"
ASSET_GROUP_DATA_PREPROCESSING = "data_preprocessing"
ASSET_GROUP_MODEL_TRAINING = "model_training"
