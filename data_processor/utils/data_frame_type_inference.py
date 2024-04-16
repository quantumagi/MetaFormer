import pandas as pd
from data_processor.utils.series_type_constants import InferenceType, friendly_to_inference_type_map
from data_processor.utils.series_type_inference import SeriesTypeInference
from data_processor.utils.series_type_conversion import get_preferred_type

# Class for inferring data types for each column in a DataFrame
# Batches of CSV text chunks are analyzed and type statistics (exceptions per type) are gathered
class DataFrameTypeInference:
    def __init__(self, column_types, na_values, max_category_values, category_values):
        """
        Initialize the DataFrame type inference object

        Args:
            column_types (dict): The dictionary of column names and their inferred types
            na_values (list): The list of NA values
            max_category_values (int): The maximum number of category values to consider
            category_values (dict): The dictionary of column names and their category values
        """
        # Fixed parameters
        self.na_values:list = na_values
        self.max_category_values:int = max_category_values

        # Mutable parameters
        self.column_types:dict = column_types
        self.category_values:set = category_values                            
        self.rows_processed:int = 0
    
    def infer_data_types(self, df):    
        """
        Infer data types for each column in the DataFrame.

        Assumes that NA values have already been converted to NaN.
        """
        for col in df.columns:
            series_inference = SeriesTypeInference(
                name=col,
                max_category_values=self.max_category_values
            )
            
            # Infer the data type for the current series
            candidates, category_values = series_inference.gather_type_stats(df[col], self.rows_processed, self.column_types.get(col), self.category_values.get(col, set()))
            self.category_values[col] = category_values                                                                        
            self.column_types[col] = candidates
        self.rows_processed += len(df)

    @staticmethod
    def finalize_preferred_types(column_stats, column_names, preferred_types=None, tolerance=None, category_values=None):
        """
        Finalizes the data type inference process by converting any remaining 'category' types to the final category values.

        Args:
            column_stats (dict): Statistics about the columns in the dataset.
            column_names (list): Names of the columns.
            preferred_types (list, optional): Preferred data types for the columns.
            tolerance (dict, optional): Tolerance values for data type inference.
            category_values (dict, optional): Category values for categorical data types.

        Returns:
            list: Finalized types for each column, including inferred types and category values.
        """
        # Although the column_names include the persisted preferred types we expect the client
        # to send us everything via preferred_types after consolidating the info on the UI.
        # The same is true for the tolerance value, even though we keep a persisted value in the db.
        preferred_types_dict = {ptype['name']: ptype for ptype in preferred_types} if preferred_types else {}
        
        final_types = []
        for column_def in column_names:
            column_name = column_def['name']
            preferred_type = preferred_types_dict.get(column_name)
            if preferred_type is None or preferred_type.get('type') is None:
                final_type = 'object'
                final_category_values = category_values.get(column_name)
                candidates = column_stats.get(column_name)
                if candidates:
                    tolerance_value = tolerance[column_name] if isinstance(tolerance, dict) else tolerance
                    final_type = get_preferred_type(SeriesTypeInference.candidates_from_type_stats(candidates, tolerance=tolerance_value))
            else:
                final_type = preferred_type.get('type')
                final_category_values = preferred_type.get('category_values')
            
            if final_type == 'category':
                final_types.append({'name': column_name, 'type': final_type, 'category_values': final_category_values})
            else:
                final_types.append({'name': column_name, 'type': final_type })
        
        return final_types

    @staticmethod
    def candidates_from_type_stats(column_stats, tolerance=None):
        """
        Finalizes the data type inference process by converting any remaining 'category' types to the final category values.
        """
        final_types = {}
        for column_name, candidates in column_stats.items():
            final_types[column_name] = SeriesTypeInference.candidates_from_type_stats(candidates, tolerance=tolerance[column_name] if isinstance(tolerance, dict) else tolerance)
        return final_types

    # Load the CSV data into a DataFrame from a stream
    @staticmethod
    def csv_to_dataframe(csv_stream, column_names, na_values, nrows=None):
        df = pd.read_csv(
            csv_stream,
            header=None,
            names=column_names, # list(self.column_types.keys()),
            dtype='object', # Non-lossy load of CSV data for inference
            nrows=nrows,
            skip_blank_lines=False,
            skipinitialspace=True,
            na_values=na_values
        )
        
        def custom_na_values(val):
            # Check if `val` is explicitly None or pandas NA; then strip and check against `na_values` if `val` is a string
            if val is None or pd.isna(val):
                return pd.NA
            elif isinstance(val, str) and val.strip() in na_values:
                return pd.NA
            else:
                return val

        df = df.map(custom_na_values)
        return df
    
    # Type-rich load of CSV data using inferred types
    # Operations such as trimming space or skipping blank lines should be done by the client
    @staticmethod
    def load_data_frame_custom(csv_stream, preferred_types, na_values=None):
        # Non-lossy load of the CSV data into the dataframe
        # Enumerate the objects in preferred_types reading the "name" attribute
        column_names = [col['name'] for col in preferred_types]        
        type_dict = {col['name']: col.get('type', 'object') for col in preferred_types}
        category_values = {col['name']: col.get('category_values') for col in preferred_types}
        
        df_subset = DataFrameTypeInference.csv_to_dataframe(csv_stream, column_names, na_values or [])
        # Keep track of exceptions for each column
        exceptions = None
        # Iterate over dataframe columns and apply the inferred types
        for col in column_names:
            # Getting the "InferenceType" as expected by the "apply" method
            inference_type = friendly_to_inference_type_map[type_dict[col]]
            # Apply the inferred type to the column
            inference = SeriesTypeInference(
                name=col
            )
            categories = category_values.get(col, set()) if inference_type == InferenceType.CATEGORY and category_values else None
            df_subset[col], exceptions = inference.apply(df_subset[col], inference_type, categories, exceptions)

        return df_subset, exceptions