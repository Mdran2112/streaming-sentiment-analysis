from test import test_setup

test_setup()
import findspark
findspark.find()
from src.prediction_model import ModelWrapperFactory

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.batch_producer import BatchProducer

from pyspark.sql import SparkSession


class TestBatchProducer:
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    mock1 = spark.createDataFrame(
        [
            (1, "{'id': 1, 'clean_text': 'hello!'}", "2023-07-07"),
            (2, "{'id': 2, 'clean_text': 'bye!'}", "2023-07-07"),
            (3, "{'id': 3, 'clean_text': 'I hate this!'}", "2023-07-07"),
            (4, "{'id': 4, 'clean_text': 'This is nice!'}", "2023-07-07"),
        ],
        StructType(  # Define the whole schema within a StructType
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        ),
    )

    inputschema = StructType(
        [StructField("id", IntegerType()), StructField("clean_text", StringType())]
    )

    def test_transform(self):
        """
        Test the run() method of BatchProducer
        """
        mock_model_name = "lr-sent-analyses"

        model_wrapper = ModelWrapperFactory.build(mock_model_name)

        batch_producer = BatchProducer(
            input_schema=model_wrapper.input_schema,
            output_kafka_error_topic="...",
            output_transformer=model_wrapper.output_transformer,
            p_model=model_wrapper.pipeline_model,
        )

        proc_df = batch_producer.run(self.mock1, 1)

        rdd = proc_df.collect()

        for i in rdd:
            print(i)

        assert rdd[2].prediction_label == "bad" and rdd[2].prediction == 2
        assert rdd[3].prediction_label == "good" and rdd[3].prediction == 0

    def test_input_validation(self):
        """
        Tests the separation of invalid records from the input batch.
        """
        mock1 = self.spark.createDataFrame(
            [
                (1, "{dsad}", "2023-07-07"),  # wrong input
                (2, "{'id': 2, 'clean_text': 'bye!}", "2023-07-07"),  # wrong input
                (3, "{'id': 3, 'clean_text': 'hello!'}", "2023-07-07"),  # ok input!
            ],
            StructType(  # Define the whole schema within a StructType
                [
                    StructField("id", IntegerType(), True),
                    StructField("value", StringType(), True),
                    StructField("timestamp", StringType(), True),
                ]
            ),
        )

        mock_model_name = "lr-sent-analyses"

        model_wrapper = ModelWrapperFactory.build(mock_model_name)

        batch_producer = BatchProducer(
            input_schema=model_wrapper.input_schema,
            output_kafka_error_topic="...",
            output_transformer=model_wrapper.output_transformer,
            p_model=model_wrapper.pipeline_model,
        )

        res, errors_df = batch_producer._transform(mock1)

        print(res.collect())
        print(errors_df.collect())

        errors_df = errors_df.collect()
        res = res.collect()

        assert len(res) == 1 and len(errors_df) == 2

        assert errors_df[0].value == "{dsad}"
        assert errors_df[1].value == "{'id': 2, 'clean_text': 'bye!}"
        assert res[0].clean_text == "hello!" and res[0].prediction_label == "neutral"
