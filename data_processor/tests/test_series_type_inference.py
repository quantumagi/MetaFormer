import unittest
import pandas as pd
from data_processor.utils.series_type_inference import SeriesTypeInference
from data_processor.utils.series_type_conversion import convert_type
from data_processor.utils.series_type_constants import InferenceType

class TestSeriesTypeInference(unittest.TestCase):
    def setUp(self):
        pass

    def test_mixed_numeric_and_complex_types_infers_complex(self):
        # Initalize the inference object with defaults
        inference = SeriesTypeInference()

        # Infer the data type for the given series
        series = pd.Series(['42', '42', '1', '1+2j'])
        type_stats, category_values = inference.gather_type_stats(series, 4, type_stats={}) # Candidates is used to store type_stats
        candidates = inference.candidates_from_type_stats(type_stats, tolerance=None)

        # Define the expected types
        expected_candidates = { InferenceType.COMPLEX.value, InferenceType.OBJECT.value }
        expected_categories = set()

        # Ensure the inferred types match the expected types
        self.assertSetEqual(candidates, expected_candidates)
        self.assertSetEqual(category_values, expected_categories)

    def test_mixed_int_and_float_types_infers_float(self):
        # Initalize the inference object with defaults
        inference = SeriesTypeInference()

        # Infer the data type for the given series
        series = pd.Series(['42', '3.14', '-1'])
        type_stats, category_values = inference.gather_type_stats(series, 3, type_stats={}) # Candidates is used to store type_stats
        candidates = inference.candidates_from_type_stats(type_stats, tolerance=None)

        # Define the expected types
        expected_candidates = { InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value }
        expected_categories = set()

        # Ensure the inferred types match the expected types
        self.assertSetEqual(candidates, expected_candidates)
        self.assertSetEqual(category_values, expected_categories)
