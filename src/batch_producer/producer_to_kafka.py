"""
Sub implementation of BatchProducer for delivering outputs to a Kafka topic.
"""
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import StructType

from src.batch_producer.batch_producer import BatchProducer
from src.cfg import KAFKA_BOOTSTRAP_SERVER
from src.prediction_model.output_transformer import OutputTransformer


class BatchProducerToKafka(BatchProducer):
    """
    .
    """

    def __init__(
        self,
        p_model: PipelineModel,
        input_schema: StructType,
        output_transformer: OutputTransformer,
        output_kafka_topic: str,
        output_kafka_error_topic: str,
    ):
        """
        Sub implementation of BatchProducer for sending outputs to a Kafka topic.
        Parameters
        ----------

        output_kafka_topic: str
            Name of the Kafka topic where the predictions will be sent.
        """
        super().__init__(
            p_model, input_schema, output_transformer, output_kafka_error_topic
        )
        self.output_kafka_topic = output_kafka_topic

    def _write(self, df: DataFrame):
        """
        Write processed data to output kafka topic
        Parameters
        ----------

        df: DataFrame
            Pyspark DataFrame
        """
        result_ = df.withColumn("value", to_json(struct("*")))
        result_ = result_.write
        result_ = result_.format("kafka")
        result_ = result_.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        result_ = result_.option("topic", self.output_kafka_topic)
        result_ = result_.option("checkpointLocation", "checkpoint")
        result_.save()
