"""
In this module we implement the classes that apply certain logic for mapping the model pipeline outputs
(for ex: int --> label string)
"""
from typing import Dict, Union


class OutputTransformer:
    """Output transformer base class."""

    def __init__(self, **kwargs):
        """
        Transformer base class
        """
        ...

    def map(self, pred: Union[int, str, float]) -> Union[str, int, float]:
        """
        Override this method in another OutputTransformer subclass in order to define the
        mapping logics.

        Parameters
        ---------

        pred: int | str | float
            Pipeline model prediction for a single record.

        """
        ...


class ModelOutputTransformer(OutputTransformer):
    """
    Transformer class that maps outputs by using a hashing object.
    """

    def __init__(self, mapper: Dict[Union[int, str], Union[int, str]]) -> None:
        super().__init__()
        """
        Transformer class that maps outputs by using a hashing object.
        
        Parameters
        ----------
        
        mapper: Dict[Union[int, str], Union[int, str]]
            Python dictionary that maps <output model label> ---> <target label>
        """
        self.mapper = mapper
        keys = list(self.mapper.keys())
        for key in keys:
            if key.isnumeric():
                self.mapper[int(key)] = self.mapper[key]
                self.mapper.pop(key)

    def map(self, pred: Union[int, str, float]) -> Union[str, int, float]:
        """
        Parameters
        ---------

        pred: int | str | float
            Pipeline model prediction for a single record.

        """
        return self.mapper[pred]


class DummyOutputTransformer(OutputTransformer):
    """
    Dummy transformer.
    """

    def __init__(self, **kwargs):
        """
        Dummy transformer, simply returns the same value of the output model.
        """
        super().__init__(**kwargs)

    def map(self, pred: Union[int, str, float]) -> Union[str, int, float]:
        """
        Parameters
        ---------

        pred: int | str | float
            Pipeline model prediction for a single record.

        """
        return pred
