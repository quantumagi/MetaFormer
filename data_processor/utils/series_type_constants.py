# Data type constants used in 'column_types' and 'na_values' keys of the schema dictionary
# Represents the data types supported for inference and schemas
from enum import Enum

class InferenceType(Enum):
    # Friendly names for the data types
    # Additional date types are not for inference but for final schema
    BOOL = 'bool'
    INT8 = 'int8'
    INT16 = 'int16'
    INT32 = 'int32'
    INT64 = 'int64'
    FLOAT32 = 'float32'
    FLOAT64 = 'float64'
    COMPLEX = 'complex'
    DATETIME = 'datetime'      #MDY
    DATETIME_Y = 'datetime_y'  #YMD
    DATETIME_D = 'datetime_d'  #DMY
    TIMEDELTA = 'timedelta'
    CATEGORY = 'category'
    OBJECT='object'

# Numeric types. These should be ordered with most restrictive first
inference_types_numeric = [InferenceType.BOOL, InferenceType.INT8, InferenceType.INT16, InferenceType.INT32, InferenceType.INT64, InferenceType.FLOAT32, InferenceType.FLOAT64, InferenceType.COMPLEX]
# Non-numeric types. These should be ordered with most restrictive first
inference_types_non_numeric = [InferenceType.TIMEDELTA, InferenceType.DATETIME, InferenceType.DATETIME_D, InferenceType.DATETIME_Y]
# All types
inference_types_all = inference_types_numeric + inference_types_non_numeric + [InferenceType.CATEGORY, InferenceType.OBJECT]

# Use a dictionary to map the InferenceType to the pandas dtype string
inference_to_pandas_type_map = {
    # Pandas series types
    InferenceType.BOOL: 'boolean',
    InferenceType.INT8: 'Int8',
    InferenceType.INT16: 'Int16',
    InferenceType.INT32: 'Int32',
    InferenceType.INT64: 'Int64',
    InferenceType.FLOAT32: 'Float32',
    InferenceType.FLOAT64: 'Float64',
    InferenceType.COMPLEX: 'complex128',
    InferenceType.DATETIME: 'datetime64[ns]',
    InferenceType.DATETIME_Y: 'datetime64[ns]',
    InferenceType.DATETIME_D: 'datetime64[ns]',
    InferenceType.TIMEDELTA: 'timedelta64[ns]',
    InferenceType.CATEGORY: 'category',
    InferenceType.OBJECT: 'object'
}

# Reverse the enumeration to map friendly name to InferenceType
friendly_to_inference_type_map = {v.value: v for v in InferenceType}
