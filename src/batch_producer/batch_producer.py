"""
Input processing of messages from Kafka.
"""
import logging
from typing import List, Optional, Tuple

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, from_json, col
from pyspark.sql.types import StructType

from src.cfg import KAFKA_BOOTSTRAP_SERVER
from src.prediction_model.output_transformer import OutputTransformer
from src.utils import validate_dataframe


class BatchProducer:
    """
    ...
    """

    def __init__(
        self,
        p_model: PipelineModel,
        input_schema: StructType,
        output_transformer: OutputTransformer,
        output_kafka_error_topic: str,
    ) -> None:
        """
        This class works as a processor of input messages from Kafka.
        A Pyspark model trained with MLlib will be used for obtaining predictions.
        The output will be handled by the _write() method, which has a 'dummy' implementation on this class. A subclass
        can be created, overriding the _write() in order to define the output target.

        Parameters
        ----------
        p_model: PipelineModel
            Machine Learning pipeline, crated with MLlib.
        input_schema: StructType
            Defines the schema of the data that will be inserted into p_model.
        output_transformer: OutputTransformer
            Object that applies certain logic for mapping the model pipeline outputs (for ex: int --> label string)
        output_kafka_error_topic: str
            Name of the Kafka topic in which invalid inputs will be sent.
        """
        self.p_model = p_model
        self.input_schema = input_schema
        self.output_transformer = output_transformer
        self.map_pred_UDF = udf(self.output_transformer.map)

        self.useful_output_predicted_df_cols = list(
            map(lambda s: s.name, input_schema.fields)
        ) + ["prediction", "timestamp"]

        self.output_kafka_error_topic = output_kafka_error_topic

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Takes the input messages and applies the model pipeline for obtaining the predictions.
        The inputs are splitted in two DataFrames: errors_df and ok_df.The first one contains the invalid inputs of
        the batch, and the second one the inputs without errors.

        Parameters
        ----------
        df: DataFrame
            Pyspark Dataframe. Must have 'timestamp' and 'value' columns.

        """
        parsed_df = df.select(
            "value",
            from_json(col("value").cast("string"), self.input_schema).alias(
                "parsed_value"
            ),
            "timestamp",
        )

        parsed_df = parsed_df.select(col("parsed_value.*"), "timestamp", "value")

        errors_df, ok_df = validate_dataframe(parsed_df)

        result_ = self.p_model.transform(ok_df.drop("value")).select(
            self.useful_output_predicted_df_cols
        )
        result_ = result_.withColumn(
            "prediction_label", self.map_pred_UDF("prediction")
        )

        return result_, errors_df

    def _write(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Processed output can be sent to different targets: Kafka topic, database, files in disk, etc.
        Override this method in a subclass of BatchProducer in order to implement the writing logic.
        """
        ...
        print(df.show())
        return df

    def _to_error_topic(self, df: DataFrame) -> None:
        """
        Send the invalid inputs to a Kafka topics of errors.
        Parameters
        ----------
        df: DataFrame
            Pyspark DataFrame.
        """
        try:
            result_ = df.write
            result_ = result_.format("kafka")
            result_ = result_.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
            result_ = result_.option("topic", self.output_kafka_error_topic)
            result_ = result_.option("checkpointLocation", "checkpoint")
            result_.save()
        except Exception as _:
            logging.error("Can't send messages to error topic. Check!")

    def run(self, df: DataFrame, batch_id: int):
        """
        Starts the streaming processing. This method will be used for .foreachBatch() method of DataStreamWriter.
        Parameters
        ----------
        df: DataFrame
        batch_id: int
        """
        try:
            # map to the input schema of the model and predict.
            result_, errors_df = self._transform(df)
            if len(errors_df.collect()) > 0:
                # If there were errors in inputs, send them to a Kafka error topic for further analyses.
                self._to_error_topic(errors_df)

            # Write processed data.
            return self._write(result_)

        except Exception as _:
            # TODO review try / except
            self._to_error_topic(df)
