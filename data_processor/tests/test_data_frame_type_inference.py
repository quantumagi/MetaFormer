import unittest
import pandas as pd
from io import StringIO
from data_processor.utils.data_frame_type_inference import DataFrameTypeInference
from data_processor.utils.series_type_constants import InferenceType, inference_types_all

class TestDataFrameTypeInference(unittest.TestCase):
    def setUp(self):
        self.all_types = { InferenceType.BOOL.value, InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.TIMEDELTA.value, InferenceType.DATETIME.value, 
                      InferenceType.CATEGORY.value, InferenceType.OBJECT.value }
        self.inference = None

    def verify_inference(self, chunk, exact_expected_types, columns=4, tolerance=0, inference=None):
        if inference is None:
            # Each column name is associated with a dictionary of types and their counts
            inference = DataFrameTypeInference(column_types={f"col_{i}": {} for i in range(1, columns+1)}, 
                na_values=['NA', 'na', 'Not Available', '"Not Available"', '-'], 
                max_category_values=100, category_values={})

        # Convert the chunk to a DataFrame
        df = inference.csv_to_dataframe(StringIO(chunk), column_names=[f"col_{i}" for i in range(1, columns+1)], na_values=inference.na_values)

        # Directly infer data types
        inference.infer_data_types(df)
        column_types = DataFrameTypeInference.candidates_from_type_stats(inference.column_types, tolerance=tolerance)
        for col in column_types:
            self.assertSetEqual(column_types[col], exact_expected_types[col], "Inferred types do not exactly match the expected types.")

    def test_mixed_numeric_types(self):
        dataset = """42,42,1,1+2j
                3.14,3.145678,2,4+5j
                -1,-,3,4+6j"""
        
        exact_expected_types = {
            "col_1": {"float32", "float64", "complex", "object"},
            "col_2": {"float64", "complex", "object"},
            "col_3": {"int8", "int16", "int32", "int64", "float32", "float64", "complex", "object"},
            "col_4": {"complex", "object"}
        }
        
        self.verify_inference(dataset, exact_expected_types)        

    def test_mixed_numeric_types_with_tolerance(self):
        dataset = """42,42,1,1+2j
                3.14,3.145678,2,4+5j
                2001-1-1,-,3,4+6j"""
        
        exact_expected_types = {
            "col_1": {InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value, InferenceType.DATETIME_Y.value},
            "col_2": {InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value, InferenceType.TIMEDELTA.value, 
                      InferenceType.DATETIME.value, InferenceType.DATETIME_Y.value, InferenceType.DATETIME_D.value, InferenceType.BOOL.value},
            "col_3": {InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value, InferenceType.BOOL.value},
            "col_4": {InferenceType.COMPLEX.value, InferenceType.OBJECT.value}
        }
        
        self.verify_inference(dataset, exact_expected_types, tolerance=2)

    def test_single_int_exception_still_infers_int_with_tolerance_1(self):
        dataset = """1
                2
                Not Available
                X"""
        
        exact_expected_types = {
            "col_1": {InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value}
        }
        
        self.verify_inference(dataset, exact_expected_types, tolerance=1, columns=1)

    def test_category_inference(self):
        dataset = """42,Hello World,2021-01-01,A
                3.14,Python,2021-01-02,B
                -1,Test String,2021-01-03,A
                2.71,New Entry,2021-01-04,B"""
        
        exact_expected_types = {
            "col_1": {InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value},
            "col_2": {InferenceType.OBJECT.value},  # Expected 'object' due to text
            "col_3": {InferenceType.DATETIME_Y.value, InferenceType.OBJECT.value},  # Expected 'datetime64' due to date format
            "col_4": {InferenceType.CATEGORY.value, InferenceType.OBJECT.value}  # Expected 'category' due to limited unique values
        }
        
        self.verify_inference(dataset, exact_expected_types)

if __name__ == '__main__':
    unittest.main()
