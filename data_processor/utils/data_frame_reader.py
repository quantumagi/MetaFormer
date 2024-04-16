from io import StringIO
from .data_frame_type_inference import DataFrameTypeInference
import logging

logger = logging.getLogger(__name__)

# Retrieves a block of CSV data from the supplied repository based on the specified row range
# The schema is read from the same repository and applied to return a strongly typed dataframe
# as well as any exceptions encountered due to the specified tolerance (if any).
class DataFrameReader:
    def __init__(self):
        pass

    @staticmethod
    def read_csv_subset(repository, dataset_name, first_data_row, num_rows, tolerance=None, filter='', preferred_types=None):
        """
        Reads a subset of the CSV data based on the specified row range.
                    
        Columns are cast firstly to the preferred type if specified and then an inferred type otherwise. 
        Values that don't fit the resulting type are recorded in the exceptions column as JSON: {'<column name>': '<value>', ...}

        Args:
            repository: The repository containing the dataset.
            dataset_name (str): The name of the CSV dataset.
            first_data_row (int): The 1-based index of the first row to read.
            numrows (int): The number of rows to read.
            tolerance (int): The number of exceptions before any data type is considered invalid.
            filter (str): The filter to apply to the data.
            preferred_types (dict): The preferred data types for each column. E.g. [{"name": "col1", "type": "category", "values": ["A", "B", "C"] }]
            category_values (dict): The category values for each column.
        """       
        # Load the .schema file to get the data types and the number of processed lines
        logger.info("Reading data subset from %s: start row %d, number of rows %d", dataset_name, first_data_row, num_rows)
        schema, column_names = repository.read_schema(dataset_name)
        if not column_names:
            raise ValueError(f"Dataset not found {dataset_name}")
        
        # For columns without a preferred type inference selects the highest specificity type with exceptions <= tolerance. 
        # Otherwise 'object' is selected. The latest exception counts are read from schema.column_types.   
        preferred_types = DataFrameTypeInference.finalize_preferred_types(schema.column_types, column_names, preferred_types=preferred_types, tolerance=tolerance, category_values=schema.category_values)                
        dataset_reader = repository.get_dataset_reader(dataset_name, filter=filter)
        try:
            dataset = dataset_reader.read(first_data_row, num_rows)
            df, exceptions = DataFrameTypeInference.load_data_frame_custom(StringIO(dataset), preferred_types, schema.na_values)
            return df, exceptions, preferred_types
        except Exception as e:
            raise ValueError(f"Error reading dataframe: {str(e)}")
