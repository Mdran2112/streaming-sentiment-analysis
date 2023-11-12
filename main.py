"""
* For testing:

1. Open a console and create a Kafka Producer to send messages to 'messages' topic:
    docker compose exec broker kafka-console-producer --bootstrap-server localhost:9092 --topic messages

2. Open a console and create a Kafka Consumer that listens to 'predictions' topic.
    docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic predictions

3. Send messages with schema
    {"id": <int>, "clean_text": <str>}

4. To consume the errors topic:
  docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic errors
"""
import logging
import sys

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import findspark

findspark.find()
from pyspark.sql import SparkSession

from src.cfg import (
    KAFKA_INCOMING_MSG_TOPIC,
    KAFKA_PREDICTIONS_TOPIC,
    KAFKA_ERRORS_TOPIC,
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_STARTING_OFFSET,
    SPARK_JARS_PACKAGE,
    SPARK_APP_NAME,
    SPARK_HOST,
    PYSPARK_MODEL_PIPELINE,
)
from src.prediction_model.model_wrapper import ModelWrapperFactory
from src.batch_producer import BatchProducerToKafka


def main():
    """
    Entrypoint function.
    """
    logging.info(f"STARTING {SPARK_APP_NAME.upper()} APP")
    logging.info("----------------------------------------")

    spark = (
        SparkSession.builder.config("spark.jars.packages", SPARK_JARS_PACKAGE)
        .appName(SPARK_APP_NAME)
        .master(SPARK_HOST)
        .getOrCreate()
    )

    model_name = PYSPARK_MODEL_PIPELINE
    logging.info(f"Will use the model: {model_name}")

    model_wrapper = ModelWrapperFactory.build(model_name)
    logging.info("Model loaded!")
    logging.info(f"Messages input schema: {model_wrapper.input_schema}")

    batch_producer = BatchProducerToKafka(
        input_schema=model_wrapper.input_schema,
        output_kafka_topic=KAFKA_PREDICTIONS_TOPIC,
        output_kafka_error_topic=KAFKA_ERRORS_TOPIC,
        output_transformer=model_wrapper.output_transformer,
        p_model=model_wrapper.pipeline_model,
    )

    logging.info("Instantiated BatchProducerToKafka")

    logging.info(f"Listening messages from '{KAFKA_INCOMING_MSG_TOPIC}' topic...")
    logging.info(f"Predictions will be sent to '{KAFKA_PREDICTIONS_TOPIC}' topic...")
    logging.info(f"Errors will be sent to '{KAFKA_ERRORS_TOPIC}' topic...")
    logging.info("----------------------------------------")
    logging.info("The app is ready now!!")
    logging.info("----------------------------------------")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_INCOMING_MSG_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSET)
        .option("includeTimestamp", True)
        .option("failOnDataLoss", False)
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    kafka_df.writeStream.foreachBatch(batch_producer.run).option(
        "checkpointLocation", "checkpoint"
    ).start().awaitTermination()


if __name__ == "__main__":
    main()
