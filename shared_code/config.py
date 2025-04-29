"""Configuration for the Alma Data Preparation function."""

import os

COMBINED_CONTAINER_NAME = os.getenv("COMBINED_CONTAINER_NAME", "alma-combined-reports")
NOTIFIER_QUEUE_NAME = os.getenv("NOTIFIER_QUEUE_NAME", "notifierqueue")

DATA_PREP_QUEUE_NAME = os.getenv("DATA_PREP_QUEUE_NAME", "datacombinequeue")

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING", "sqlite:///alma_data_prep.db")

