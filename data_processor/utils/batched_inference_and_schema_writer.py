from io import StringIO
from .data_frame_type_inference import DataFrameTypeInference

# Analyzes a batch of CSV data to determine type statistics and updates the dataset and schema
# in the supplied abstract repository. The schema is updated after each batch which allows the
# client to read the schema and dataset in parallel.
class BatchedInferenceAndSchemaWriter:
    def __init__(self, dataset_name, schema, repository):
        self.schema = schema
        self.dataset_name = dataset_name
        self.inference = DataFrameTypeInference(schema.column_types, schema.na_values, schema.max_categories, schema.category_values)
        self.repository = repository

    def initialize(self):
        """
        Analyze the first chunk of data to determine the initial schema.
        """
        self.schema.from_inference(self.inference)
        self.schema.status = "incomplete"
        self.repository.write_schema(self.dataset_name, self.schema)

    def finalize(self):
        """
        Finalizes the schema and writes it to the repository.
        """
        self.schema.status = "complete" # TODO: Should only be able to set this if "upload_status" is "complete"
        self.repository.write_schema(self.dataset_name, self.schema)

    def process(self, dataset):
        """
        Updates the type statistics using 'infer_data_types' and writes the schema information to the repository.

        Args:
            dataset (list): A chunk of unbroken CSV lines to process.
        """
        df = self.inference.csv_to_dataframe(StringIO(dataset), list(self.inference.column_types.keys()), self.inference.na_values)
        self.inference.infer_data_types(df)
        self.schema.from_inference(self.inference)
        self.repository.write_schema(self.dataset_name, self.schema)
