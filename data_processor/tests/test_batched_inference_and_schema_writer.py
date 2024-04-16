import unittest
from data_processor.utils.batched_inference_and_schema_writer import BatchedInferenceAndSchemaWriter
from data_processor.utils.postgresql_repository import PostgresqlRepository
from data_processor.utils.series_type_constants import InferenceType
from data_processor.utils.data_frame_type_inference import DataFrameTypeInference
from data_processor.utils.schema import Schema

class TestBatchedInferenceAndSchemaWriter(unittest.TestCase):
    def setUp(self):
        # Define the initial broad set of candidate types for multiple columns
        self.file_name = "test_file"
        self.schema = Schema(max_categories=100, na_values=["NA", "na", "-"], column_types={
            "col_1": {},
            "col_2": {},
            "col_3": {},
            "col_4": {}
        })
        
        self.repository = PostgresqlRepository.get_repository('test', create_db=True)
        # Writing the schema will require a dataset id
        # That means en entry must exist for this dataset in the database        
        with self.repository.get_dataset_writer(self.file_name, [
            {"name": "col_1"},
            {"name": "col_2"},
            {"name": "col_3"},
            {"name": "col_4"}]) as writer:
            writer.write("")            
        
        self.processor = BatchedInferenceAndSchemaWriter(self.file_name, self.schema, self.repository)

    def verify_inference(self, dataset, exact_expected_types):    
        # Process the chunk
        self.processor.initialize()
        self.processor.process(dataset)
        self.processor.finalize()

        # Read the schema file to get the inferred types
        schema_info, _ = self.repository.read_schema(self.file_name)

        # Extract the inferred types from the schema
        inferred_types = DataFrameTypeInference.candidates_from_type_stats(schema_info.column_types)

        # Ensuring the inferred types exactly match the expectations
        self.assertDictEqual(inferred_types, exact_expected_types,
            "Inferred types do not exactly match the expected types.")

    def test_with_mixed_numeric_types(self):
        dataset = """42,42,1,1+2j
                3.14,3.145678,2,4+5j
                -1,-,3,4+6j"""
        
        # Update expected types to include 'float32' and 'object' for numeric_col
        exact_expected_types = {
            "col_1": {InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value},
            "col_2": {InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value},
            "col_3": {InferenceType.INT8.value, InferenceType.INT16.value, InferenceType.INT32.value, InferenceType.INT64.value, 
                      InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value},
            "col_4": {InferenceType.COMPLEX.value, InferenceType.OBJECT.value}
        }
        
        self.verify_inference(dataset, exact_expected_types)

    def test_with_category_inference(self):
        # Define a chunk with a column that can be inferred as a category
        dataset = """42,Hello World,2021-01-01,A
                3.14,Python,2021-01-02,B
                -1,Test String,2021-01-03,A
                2.71,New Entry,2021-01-04,B"""
        
        # Expect category-like values to be inferred as 'category'
        exact_expected_types = {
            "col_1": {InferenceType.FLOAT32.value, InferenceType.FLOAT64.value, InferenceType.COMPLEX.value, InferenceType.OBJECT.value},
            "col_2": {InferenceType.OBJECT.value},
            "col_3": {InferenceType.DATETIME_Y.value, InferenceType.OBJECT.value},
            "col_4": {InferenceType.CATEGORY.value, InferenceType.OBJECT.value}
        }
        
        self.verify_inference(dataset, exact_expected_types)

if __name__ == '__main__':
    unittest.main()
