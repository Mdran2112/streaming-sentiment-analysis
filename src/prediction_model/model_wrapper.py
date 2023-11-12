"""
A wrapper that contains the PipelineModel, an input model Schema and an OutputTransformer.
"""
import json
from dataclasses import dataclass
from os.path import join
from typing import Dict, Union

from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StructField,
    StringType,
    DoubleType,
)

from src.cfg import MODELS_DIR_PATH
from src.prediction_model.output_transformer import (
    OutputTransformer,
    ModelOutputTransformer,
    DummyOutputTransformer,
)


@dataclass
class ModelWrapper:
    """Wrapper of pipe model, schema and output processor."""

    pipeline_model: PipelineModel
    input_schema: StructType
    output_transformer: OutputTransformer


class ModelWrapperFactory:
    """
    This class is used for parsing a ModelWrapper by only passing the pipeline model package name.
    """

    @staticmethod
    def parse_schema(input_schema: Dict[str, str]) -> StructType:
        """
        Parses a Pyspark StructType based on a dictionary that defines the model input schema.

        Parameters
        ----------

        input_schema: Dict[str, str]
            Dictionary with the model input schema. The schema should be:
            {<input_model_col>: "STRING" | "INTEGER" | "DOUBLE"}
            for example: {"clean_text": "STRING"}
        """
        schemas = [
            StructField("id", IntegerType()),
        ]

        for k, v in input_schema.items():
            match v:
                case "STRING":
                    d_type = StringType()
                case "INTEGER":
                    d_type = IntegerType()
                case "DOUBLE":
                    d_type = DoubleType()
                case _:
                    raise ValueError(
                        f"Not recognized dataType: {v}. "
                        f"Check ModelWrapperFactory code!"
                    )

            schemas.append(StructField(k, d_type))

        return StructType(schemas)

    @staticmethod
    def parse_transformer(
        mapper: Dict[Union[int, str], Union[int, str]]
    ) -> OutputTransformer:
        """
        Parses an OutputTransformer that will use the 'mapper'.

        Parameter
        ---------
        mapper: Dict[Union[int, str], Union[int, str]]
            Python dictionary that maps <output model label> ---> <target label>
        """
        if len(mapper) == 0:
            return DummyOutputTransformer()

        return ModelOutputTransformer(mapper=mapper)

    @staticmethod
    def load_model(model_name: str) -> PipelineModel:
        """
        Loads a PipelineModel trained with MLlib. The model folder has to be inside the directory MODELS_DIR_PATH
        (see cfg.py)

        Parameters
        ----------
        model_name: str
            Name of the ML Pipeline model.
        """
        model_path = join(MODELS_DIR_PATH, model_name)
        p_model = PipelineModel.load(model_path)
        return p_model

    @classmethod
    def build(cls, model_name: str) -> ModelWrapper:
        """
        Creates a ModelWrapper using a ML model package.

        Parameter
        ---------
        model_name: str
            Name of the ML Pipeline model.
        """

        model_path = join(MODELS_DIR_PATH, model_name)
        with open(join(model_path, "config.json"), "r") as jfile:
            cfg = json.load(jfile)

        input_schema = cls.parse_schema(cfg["input_schema"])
        output_transformer = cls.parse_transformer(cfg["output_labels"])
        model = cls.load_model(model_name)

        wrapper = ModelWrapper(model, input_schema, output_transformer)

        return wrapper
