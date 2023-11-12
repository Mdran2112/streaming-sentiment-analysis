from src.prediction_model.output_transformer import ModelOutputTransformer


class TestOutputTransformer:
    def test_model_output_transforme(self):
        """
        Tests the map() method from ModelOutputTransformer class.
        """
        mapper = {"0": "a", "1": "b", "2": "c"}

        tr = ModelOutputTransformer(mapper)

        assert list(tr.mapper.keys()) == [0, 1, 2]
        assert tr.map(0) == "a"
        assert tr.map(1) == "b"
        assert tr.map(2) == "c"
