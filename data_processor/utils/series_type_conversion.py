# Core inference functions
# Vectorized operations are used where possible.
import numpy as np
import pandas as pd
from data_processor.utils.series_type_constants import InferenceType, inference_to_pandas_type_map
import warnings

def convert_type(series:pd.Series, inftype: InferenceType, categories=None):     
    """
    Convert a series to a specific data type

    Args:
        series: The series to convert
        inftype: The InferenceType to convert to
    """
    converted = series

    if inftype == InferenceType.BOOL:
        return convert_to_bool(converted)
    elif inftype == InferenceType.TIMEDELTA:
        return convert_to_timedelta(converted)
    elif inftype == InferenceType.DATETIME:
        return convert_to_datetime(converted, '%m/%d/%Y')
    elif inftype == InferenceType.DATETIME_Y:
        return convert_to_datetime(converted, '%Y/%m/%d')
    elif inftype == InferenceType.DATETIME_D:
        return convert_to_datetime(converted, '%d/%m/%Y')
    elif inftype == InferenceType.CATEGORY:
        return convert_to_category(converted, categories)
    elif inftype == InferenceType.INT8 or inftype == InferenceType.INT16 or inftype == InferenceType.INT32 or inftype == InferenceType.INT64:
        return convert_to_int(converted, inftype)
    elif inftype == InferenceType.FLOAT32 or inftype == InferenceType.FLOAT64:
        return convert_to_float(converted, inftype)
    elif inftype == InferenceType.COMPLEX:
        return convert_to_complex(converted)
    else:
        raise ValueError(f"Unsupported data type: {inftype.value}")
        
def convert_to_bool(series):
    """
    Convert a series to a boolean data type
    """
    # We mainly operate against strings but adding this for completeness
    if pd.api.types.is_bool_dtype(series.dtype):
        return series.astype('boolean')
    
    # Avoid dealing with complex types
    # We mainly operate against strings but adding this for completeness
    if np.issubdtype(series.dtype, np.complexfloating):
        converted = complex_to_float64(series)
    else:
        converted = series

    # Handling boolean types requires special consideration due to boolean-like strings
    result = pd.Series(pd.NA, index=series.index, dtype='boolean')
    if pd.api.types.is_string_dtype(converted.dtype):
        converted = converted.str.lower()
        result[converted.isin({'yes', 'y', 'true', '1'})] = True
        result[converted.isin({'no', 'n', 'false', '0'})] = False
    elif pd.api.types.is_numeric_dtype(converted.dtype):
        # Directly convert numeric 0 and 1 to False and True, respectively
        result[(converted == 1)] = True
        result[(converted == 0)] = False

    return result

def convert_to_timedelta(series):
    """
    Convert a series to a timedelta data type
    """
     # We mainly operate against strings but adding this for completeness
    if pd.api.types.is_timedelta64_ns_dtype(series.dtype):
        return series
            
    # We mainly operate against strings but adding this for completeness
    if not pd.api.types.is_object_dtype(series.dtype):
        # Return na for all
        return pd.Series([pd.NA] * len(series), index=series.index).astype('datetime64[ns]') 
    
    # Convert each numeric element to NaT
    converted = series.copy()
    index = ~pd.to_numeric(converted, errors='coerce').isna()
    converted[index] = pd.NaT
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        return pd.to_timedelta(converted, errors='coerce')

def convert_to_datetime(series, dateFormat):
    """
    Convert a series to a datetime data type, handling both expected and 'special' date formats.
    """
    if pd.api.types.is_datetime64_ns_dtype(series.dtype):
        return series
    
    if not pd.api.types.is_object_dtype(series.dtype):
        return pd.Series([pd.NA] * len(series), index=series.index).astype('datetime64[ns]')
    
    converted = series.copy()
    numeric_mask = ~pd.to_numeric(converted, errors='coerce').isna()
    converted[numeric_mask] = pd.NaT
    
    nona = converted.dropna()
    if not nona.empty:
        dates_parts = nona.str.replace('-', '/').str.extract(r'(^[0-9]+/[0-9]+/[0-9]+)(.*)$', expand=True)
        
        date_candidate_mask = dates_parts[0].str.strip().str.len() > 0
        
        # All candidate dates (n/n/n) will receive an outcome in this block of code
        # and will not be looked at by the fallback mechanism.
        candidate_dates = dates_parts[0][date_candidate_mask]
        candidate_times = dates_parts[1][date_candidate_mask].replace(r'^\s*$', ' 00:00:00', regex=True)        
        parsed_dates = pd.to_datetime(candidate_dates, errors='coerce', format=dateFormat)
        successful_parse_mask = parsed_dates.notna()
        # Everything else must be set to NaT
        converted.loc[nona.index[date_candidate_mask][~successful_parse_mask]] = pd.NaT
        if successful_parse_mask.any():
            # Combine the parsed date and time parts
            final_datetimes = pd.to_datetime(parsed_dates[successful_parse_mask].astype(str) + candidate_times[successful_parse_mask], 
                errors='coerce', dayfirst=dateFormat.startswith('%d'), yearfirst=dateFormat.startswith('%Y'))
            # Calculate the combined mask relative to the original non-Na series 'nona'
            combined_mask = nona.index[date_candidate_mask][successful_parse_mask]
            converted.loc[combined_mask] = final_datetimes
        
        # Utilize a fallback for everything not of the form n/n/n
        fallback_mask = ~date_candidate_mask
        fallback_dates = nona[fallback_mask]
        if fallback_dates.any():
            # Using mixed format should be safer now.
            converted.loc[fallback_dates.index] = pd.to_datetime(fallback_dates, errors='coerce', format='mixed')

    return converted.astype('datetime64[ns]')

def convert_to_category(series, categories):
    """
    Convert a series to a category data type
    """
    return series.astype('category')

def convert_to_int(series, pdtype):
    """
    Convert a series to an integer data type
    """
    converted = pd.to_numeric(series, errors='coerce').copy()                
    # Set numbers with imaginary parts to NA
    # We mainly operate against strings but adding this for completeness
    if np.issubdtype(converted.dtype, np.complexfloating):
        converted = complex_to_float64(converted)

    # Handling floating-point numbers, identifying non-integer values
    if converted.dropna().dtype.kind in ['f']:
        no_decimals = (converted % 1 == 0)
        converted[~no_decimals] = pd.NA

    # Adjust dtype for np.iinfo if it's a pandas nullable integer type
    numpy_dtype = pdtype.value.lower()
    if 'int' in numpy_dtype:
        min_val, max_val = np.iinfo(numpy_dtype).min, np.iinfo(numpy_dtype).max
        # Set values outside the range to NA
        converted = converted.where(converted.between(min_val, max_val), pd.NA)
    
    return converted.astype(inference_to_pandas_type_map[pdtype])

def convert_to_float(series, pdtype):
    """
    Convert a series to a float data type
    """
    converted = series.copy()
    # We read the csv data in a non-lossy manner (as 'object'). Having the original
    # number as a string allows us to check the intended number of significant digits.
    if pd.api.types.is_string_dtype(series.dtype):
        if pdtype == InferenceType.FLOAT32:
            digits = converted.apply(count_significant_digits)
            converted.loc[digits > 6] = pd.NA

        converted = pd.to_numeric(converted, errors='coerce')
    # Avoid dealing with complex types
    # We mainly operate against strings but adding this for completeness
    if np.issubdtype(converted.dtype, np.complexfloating):
        converted = complex_to_float64(converted)
    return converted.astype(inference_to_pandas_type_map[pdtype])

def convert_to_complex(series):
    """
    Convert a series to a complex data type
    """
    # If everything is already complex return the series as-is
    # We mainly operate against strings but adding this for completeness
    if np.issubdtype(series.dtype, np.complexfloating):
        return series        

    # All numbers are also complex number, so identify those first using vectorized operations
    numeric_part = pd.to_numeric(series, errors='coerce')
    non_numeric_mask = numeric_part.isna() & series.notna()
    # Now use apply to convert the non-numeric part to complex
    converted = series.where(~non_numeric_mask, series[non_numeric_mask].apply(safe_complex_convert))
    return converted.astype('complex128')

def count_significant_digits(str_number):
    """
    Counts the number of significant digits in a numeric value, ignoring decimal points
    and leading/trailing zeros.
    
    Args:
        str_number: The text representation of a number whose significant digits are to be counted.
        
    Returns:
        The count of significant digits in `str_number`.
        0 if `str_number` is not a number.
    """
    if pd.isna(str_number):
        return pd.NA
    # Convert to string and strip from 'e' or 'E' onwards
    str_number = str_number.split('e', 1)[0].split('E', 1)[0].replace('.', '')
    # If its a number then only digits should be left
    if not str_number.isdigit():
        return 0
    # Strip leading and trailing zeros
    str_number = str_number.lstrip('0').rstrip('0')
    # Return the length of the string
    return len(str_number) if str_number else 1

def complex_to_float64(series):
    """
    Converts a pandas Series of complex numbers to float64, setting the imaginary part to NA.
    
    Args:
        series: Pandas Series of complex numbers.
        
    Returns:
        A pandas Series of the real part of the complex numbers as float64, with NA where
        there was an imaginary part.
    """
    converted = series.values.real
    converted = pd.Series(converted, index=series.index, name=series.name, dtype='Float64')
    converted[series.values.imag != 0] = pd.NA
    return converted

def safe_complex_convert(str_number):
    """
    Safely converts a value to a complex number if possible, otherwise returns np.nan.
    
    This function is used to handle cases where the conversion to complex might fail due to
    invalid format or data type issues.
    
    Args:
        str_number: The text of the complex number to be converted.
        
    Returns:
        The complex representation of `str_number` or np.nan if the conversion is not possible.
    """
    try:
        if pd.isna(str_number) or not isinstance(str_number, object):
            return np.nan
        return complex(str_number)
    except ValueError:
        return np.nan

def get_preferred_type(candidate_types):
    """
    Determines the most preferred data type from a list of candidate types based on a predefined
    order of preference.
    
    Args:
        candidate_types: A list of strings representing candidate data types.
        
    Returns:
        The most preferred data type as a string.
    """
    preferred_types_order = ['bool', 'int8', 'int16', 'int32', 'int64', 'float32', 'float64', 'complex', 'timedelta[ns]', 'datetime64[ns]', 'category', 'object']
    return next((dtype for dtype in preferred_types_order if dtype in candidate_types), None) or 'object'
