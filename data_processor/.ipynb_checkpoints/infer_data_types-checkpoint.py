import pandas as pd
import numpy as np
import concurrent.futures

def process_csv_file(file_path, n_batches=4, batch_size=1000):
    """
    Processes the CSV file, infers data types, and applies the schema.

    Args:
    file_path (str): The file path of the CSV file to process.
    n_batches (int): Number of batches to split the DataFrame for parallel processing.
    batch_size (int): Size of each micro-batch.

    Returns:
    pd.DataFrame: DataFrame with inferred and applied data types.
    """
    # Step 1: Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    
    # Step 2: Process the DataFrame in batches to infer data types
    common_types = batch_process_sequential(df, batch_size=batch_size, n_batches=n_batches)
    
    # Step 3: Apply the inferred schema to the DataFrame
    df = apply_schema(df, common_types)
    
    return df

def batch_process_sequential(df, batch_size=1000, n_batches=4):
    """
    Split DataFrame into sequential batches and delegate to batch_process_parallel to process each 
    in parallel as a number of micro batches to infer data types.

    Args:
    df (pd.DataFrame): DataFrame to process.
    batch_size (int): Size of each sequential batch.
    n_batches (int): Number of micro-batches.

    Returns:
    dict: Dictionary containing inferred data types for each column.
    """

    # Step 1: Split DataFrame into sequential batches
    sequential_batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
    
    # Step 2: Process sequential batches
    type_candidates_list = [batch_process_parallel(batch, n_batches=n_batches) for batch in sequential_batches]
    
    # Step 3: Find common data types across all sequential batches
    common_types = find_common_types(type_candidates_list)

    # Step 4: For string types use the Category type if the number of unique values is less than 50% of the total number of values
    for col in common_types:
        if common_types[col] == {'object'}:
            if df[col].nunique() < len(df[col]) * 0.5:
                common_types[col] = {'category'}

    return common_types

def batch_process_parallel(batch, n_batches=4):
    """
    Split DataFrame batch into micro-batches and process each in parallel to infer data types.

    Args:
    batch (pd.DataFrame): Sequential batch of DataFrame.
    n_batches (int): Number of micro-batches.

    Returns:
    dict: Dictionary containing inferred data types for each column.
    """
    # Step 1: Split DataFrame batch into micro-batches
    batch_size = len(batch) // n_batches
    micro_batches = [batch[i:i+batch_size] for i in range(0, len(batch), batch_size)]
    
    # Step 2: Process micro-batches in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each micro-batch
        futures = [executor.submit(process_batch, micro_batch) for micro_batch in micro_batches]
        
        # Retrieve results from futures
        type_candidates_list = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    return find_common_types(type_candidates_list)

def process_batch(batch):
    """
    Process a single batch to infer data types for each column.
    """
    type_candidates = {}
    for col in batch.columns:
        candidates = infer_candidate_types(batch[col])
        type_candidates[col] = candidates
    return type_candidates

def infer_candidate_types(column):
    """
    Infer candidate data types for a given column by attempting to convert
    it to numeric types with downcasting, checking for datetime compatibility,
    identifying complex numbers, and distinguishing between datetime64 and timedelta[ns].
    """
    candidates = set()
    
    if column.dtype == 'object':
        # Attempt to convert the column to numeric types with downcasting
        try:
            converted_series = pd.to_numeric(column, errors='raise', downcast='integer')
            dtype = str(converted_series.dtype)
            if 'int' in dtype:
                if dtype == 'int8':
                    candidates.update(['int8', 'int16', 'int32', 'int64'])
                elif dtype == 'int16':
                    candidates.update(['int16', 'int32', 'int64'])
                elif dtype == 'int32':
                    candidates.update(['int32', 'int64'])
                else:  # int64
                    candidates.add('int64')
            elif 'float' in dtype:
                # Check if the precision exceeds that of float32
                max_precision = converted_series.apply(lambda x: len(str(x).split('.')[-1])).max()
                if max_precision > 7:
                    candidates.add('float64')
                else:
                    candidates.update(['float32', 'float64'])                    
        except ValueError:
            # Conversion to numeric failed, proceed to check for datetime and timedelta
            pass

        # Attempt to convert the column to datetime64
        try:
            pd.to_datetime(column, errors='raise')
            candidates.add('datetime64')
        except (ValueError, TypeError):
            # Not a datetime64 column
            pass

        # Attempt to identify complex numbers
        try:
            column.apply(lambda x: complex(x))
            candidates.add('complex')
        except ValueError:
            # Conversion to complex failed
            pass

        # Attempt to convert the column to timedelta[ns]
        try:
            pd.to_timedelta(column, errors='raise')
            candidates.add('timedelta[ns]')
        except ValueError:
            # Not a timedelta[ns] column
            pass

    if not candidates:
        candidates.add('object')

    return candidates

def find_common_types(type_candidates_list):
    """
    Find common data types across all batches for each column.
    """
    common_types = {}
    for type_candidates in type_candidates_list:
        for col, candidates in type_candidates.items():
            if col not in common_types:
                common_types[col] = candidates
            else:
                common_types[col] = common_types[col].intersection(candidates)
    return common_types

def apply_schema(df, preferred_candidates):
    """
    Apply consistent schema to the entire dataset based on the preferred candidate types.

    Args:
    df (DataFrame): Input DataFrame.
    preferred_candidates (dict): Dictionary containing column names as keys and preferred data types as values.
        The preferred_candidates dictionary specifies the preferred data types for each column.
        Keys are column names, and values are the preferred data types to which the corresponding columns should be converted.
        Example: {'column1': 'float32', 'column2': 'datetime64', 'column3': 'int16'}

    Returns:
    DataFrame: DataFrame with schema applied.
    """
    # Define conversion functions for each data type
    conversion_map = {
        'bool': lambda x: x.astype('bool'),
        'int8': lambda x: pd.to_numeric(x, errors='coerce', downcast='integer').astype('int8'),
        'int16': lambda x: pd.to_numeric(x, errors='coerce', downcast='integer').astype('int16'),
        'int32': lambda x: pd.to_numeric(x, errors='coerce', downcast='integer').astype('int32'),
        'float32': lambda x: pd.to_numeric(x, errors='coerce', downcast='float').astype('float32'),
        'float64': lambda x: pd.to_numeric(x, errors='coerce').astype('float64'),
        'datetime64': lambda x: pd.to_datetime(x, errors='coerce'),
        'timedelta64': lambda x: pd.to_timedelta(x, errors='coerce'),
        'category': lambda x: x.astype('category'),
        'complex': lambda x: x.astype('complex'),
        'object': lambda x: x.astype('object')
    }

    # Iterate over each column and its preferred data type
    for col, preferred_type in preferred_candidates.items():
        # Apply the schema based on the preferred data type
        df[col] = conversion_map[preferred_type](df[col])

    return df

