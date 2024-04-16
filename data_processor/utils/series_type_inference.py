import pandas as pd
from data_processor.utils.series_type_conversion import convert_type
from data_processor.utils.series_type_constants import InferenceType, inference_types_numeric, inference_types_non_numeric, inference_types_all, inference_to_pandas_type_map, friendly_to_inference_type_map

# Class for inferring data types for a series
# An "object" series is analyzed and type statistics (exceptions per type) are gathered
class SeriesTypeInference:
    def __init__(self, name=None, max_category_values=None):
        """
        Initialize the series type inference object

        Args:
            name (str): The name of the series
            max_category_values (int): The maximum number of category values to consider
        """
        self.name = name
        self.max_category_values = max_category_values

    # Assumes "NA values" have already been converted to NaN
    def gather_type_stats(self, raw_series, rows_processed, type_stats=None, category_values=None):
        """
        Gathers statistics for a given series of data

        Args:
            raw_series (pd.Series): The series of data to infer the type for
            rows_processed (int): The number of rows processed so far
            type_stats (dict): Each type and the number of times it has failed
            category_values (set): The set of category values being accumulated
        """
        # Remove NA values as they should not be considered in the inference
        series = raw_series.copy().dropna()

        rows_processed += len(series)

        category_values = category_values or set()

        # Numeric types evaluation
        numeric_s = series.copy()
        numeric_type_order =  [member.value for member in inference_types_numeric]
        for dtype in numeric_type_order:
            if dtype not in type_stats:
                type_stats[dtype] = 0
            if not numeric_s.empty:
                converted = convert_type(numeric_s, friendly_to_inference_type_map[dtype], category_values)
                is_na = pd.isna(converted)
                type_stats[dtype] += (is_na).sum()
                numeric_s = numeric_s[is_na]

        # Non-numeric types evaluation
        non_numeric_s = series
        for dtype in [member.value for member in inference_types_non_numeric]:
            converted = convert_type(non_numeric_s, friendly_to_inference_type_map[dtype], category_values)
            is_na = pd.isna(converted)
            non_confirming_values = (is_na).sum()
            type_stats[dtype] = type_stats.get(dtype, 0) + non_confirming_values

        # Categorical type evaluation
        type_stats.pop('category', None)
        if category_values is not None and self.max_category_values is not None and len(category_values) <= self.max_category_values:
            unique_values = set(series.unique())
            max_to_add = self.max_category_values - len(category_values)
            if len(unique_values) <= max_to_add:
                category_values.update(unique_values)
                unique_ratio = len(unique_values) / rows_processed
                if self.max_category_values and len(unique_values) <= self.max_category_values and unique_ratio <= 0.5:
                    type_stats['category'] = 0

        return type_stats, category_values
    
    @staticmethod
    def candidates_from_type_stats(type_stats, tolerance):
        """
        Determine the candidates based on the type statistics and tolerance of the series

        Args:
            type_stats (dict): The type statistics
            tolerance (dict): The tolerance for each type
            
        Returns:
            The set of candidate types based on the statistics and tolerance. Each candidate is a type value of InferenceType.
            
        """
        def get_tolerance(dtype):
            return tolerance.get(dtype, 0) if isinstance(tolerance, dict) else tolerance or 0
        
        # For now finalize the candidates based on tolerance
        # Iterate over all the possible new data types and
        # add the ones that have not exceeded the tolerance
        candidates = set()
        for dtype in inference_types_all:
            if (dtype.value in type_stats and type_stats[dtype.value] <= get_tolerance(dtype.value)) or dtype == InferenceType.OBJECT:
                candidates.add(dtype.value)
        return candidates
    
    # After using this class to infer data types over multiple batches, 
    # A subset of those batches can be passed and loaded into a DataFrame
    def apply(self, data: pd.Series, dtype, category_values, exceptions: pd.Series = None):
        """
        Apply the inferred data type to the data series

        Args:
            data (pd.Series): The data series to convert
            dtype (pd.type): The data type to convert to
            exceptions (pd.Series): The exceptions for each value
        """
        # If the data is already of the correct type, return it
        if data.dtype == inference_to_pandas_type_map[dtype]:
            return data, exceptions
        
        # We will check which values become NA after conversion
        na_mask = data.isna()
        # Preserve the original data
        converted = data.copy()
        # If an exception series is not provided, create a new series
        if exceptions is None:
            exceptions = pd.Series([{}] * len(data))
        # Convert the data to the specified type
        converted = convert_type(converted, dtype, category_values)
        # Taking care to preserve the row position store any values that could not be converted
        conversion_failures = converted[converted.isna() & ~na_mask]
        # Update exceptions for conversion failures
        for idx in conversion_failures.index:
            exceptions.at[idx] = {**exceptions.at[idx], self.name: data.at[idx]}

        return converted, exceptions