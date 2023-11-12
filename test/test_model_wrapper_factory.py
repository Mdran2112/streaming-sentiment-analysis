from test import test_setup

test_setup()

from pyspark.ml import PipelineModel
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType

from src.prediction_model import ModelWrapperFactory
from src.prediction_model.output_transformer import ModelOutputTransformer


class TestModelWrapperFactory:
    def test_build_schema(self):
        """
        Tests the parse_schema method from ModelWrapperFactory.
        """
        input_schema = {"clear_text": "STRING", "number": "INTEGER", "db": "DOUBLE"}
        s = ModelWrapperFactory.parse_schema(input_schema)

        assert s.fields[0].dataType == IntegerType() and s.fields[0].name == "id"
        assert s.fields[1].dataType == StringType() and s.fields[1].name == "clear_text"
        assert s.fields[2].dataType == IntegerType() and s.fields[2].name == "number"
        assert s.fields[3].dataType == DoubleType() and s.fields[3].name == "db"

    def test_build_output_proc(self):
        """
        Tests the parse_transformer method from ModelWrapperFactory.
        """
        proc = ModelWrapperFactory.parse_transformer(
            mapper={"0": "cero", "1000": "mil"}
        )

        assert isinstance(proc, ModelOutputTransformer)
        assert proc.mapper == {0: "cero", 1000: "mil"}

    def test_build_pipeline(self):
        """
        Tests the load_model method from ModelWrapperFactory.
        """
        p_model = ModelWrapperFactory.load_model("lr-sent-analyses")
        assert isinstance(p_model, PipelineModel)

    def test_build_wrapper(self):
        """
        Tests the build method from ModelWrapperFactory.
        """
        mock_model_name = "lr-sent-analyses"

        model_wrapper = ModelWrapperFactory.build(mock_model_name)

        assert isinstance(model_wrapper.pipeline_model, PipelineModel)
        assert isinstance(model_wrapper.output_transformer, ModelOutputTransformer)
        assert isinstance(model_wrapper.input_schema, StructType)
