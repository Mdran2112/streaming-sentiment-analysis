"""
Useful constants
"""
import os
from os.path import join, dirname, abspath

ENV = os.getenv("ENV", "PROD")

# Spark configuration
SPARK_JARS_PACKAGE = os.getenv(
    "SPARK_JARS_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
)
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "sentiment_analyses_pyspark_and_kafka")
SPARK_HOST = os.getenv("SPARK_HOST", "local[*]")

PYSPARK_MODEL_PIPELINE = os.getenv("PYSPARK_MODEL_PIPELINE", "lr-sent-analyses")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_INCOMING_MSG_TOPIC = os.getenv("KAFKA_INCOMING_MSG_TOPIC", "messages")
KAFKA_PREDICTIONS_TOPIC = os.getenv("KAFKA_PREDICTIONS_TOPIC", "predictions")
KAFKA_ERRORS_TOPIC = os.getenv("KAFKA_ERRORS_TOPIC", "errors")
KAFKA_STARTING_OFFSET = os.getenv("KAFKA_STARTING_OFFSET", "latest")


def environ_basepath_resources() -> str:
    if ENV == "DEV":
        return join(dirname(dirname(abspath(__file__))), "test")
    return "/app"


# Path to resources
MODELS_DIR_PATH = join(environ_basepath_resources(), "models")
