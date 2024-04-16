import unittest
import pandas as pd
import numpy as np
from data_processor.utils.series_type_conversion import convert_type
from data_processor.utils.series_type_constants import InferenceType

class TestSeriesTypeInferenceUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_convert_mixed_numeric_to_bool(self):
        series = pd.Series(['42', '42', '1', '1+2j'])
        expected = pd.Series([pd.NA, pd.NA, True, pd.NA], dtype='boolean')
        converted = convert_type(series, InferenceType.BOOL)
        pd.testing.assert_series_equal(converted, expected, check_dtype=True)

    def test_convert_mixed_numeric_to_int8(self):
        series = pd.Series(['42', '342', '1', '1+2j'])
        expected = pd.Series([42, pd.NA, 1, pd.NA], dtype='Int8')
        converted = convert_type(series, InferenceType.INT8)
        pd.testing.assert_series_equal(converted, expected, check_dtype=True)
    
    def test_convert_mixed_numeric_to_float32(self):
        series = pd.Series(['42', '3.14', '1+2j', '2.71'])
        expected = pd.Series([42, 3.14, pd.NA, 2.71], dtype='Float32')
        converted = convert_type(series, InferenceType.FLOAT32)
        pd.testing.assert_series_equal(converted, expected, check_dtype=True)

    def test_convert_mixed_numeric_to_complex(self):
        series = pd.Series(['42', '3.14', 'A', '2.71'])
        expected = pd.Series([42, 3.14, np.nan, 2.71], dtype='complex')
        converted = convert_type(series, InferenceType.COMPLEX)
        pd.testing.assert_series_equal(converted, expected, check_dtype=True)
        
    def test_convert_datetime_to_datetimeMDY_with_exceptions(self):
        series = pd.Series(['5', 'abc', '2001-12-31 01:00:00', '1/1/2001', '16/1/2002', '1/16/2002 02:10:03'])
        expected = pd.Series([np.nan, np.nan, np.nan, np.datetime64('2001-01-01T00:00:00'), np.nan, np.datetime64('2002-01-16T02:10:03')], dtype='datetime64[ns]')
        converted = convert_type(series, InferenceType.DATETIME)
        pd.testing.assert_series_equal(converted, expected, check_dtype=True)
        
