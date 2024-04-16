"""
This module contains the API for the data processor application.
Its used by the views to interact with the data processor.
"""
import json
from celery.result import AsyncResult
from data_processor.utils.data_frame_reader import DataFrameReader
from data_processor.utils.schema import Schema
from .models import FileSession
from .utils.postgresql_repository import PostgresqlRepository
from django.utils.text import get_valid_filename
from .tasks import process_file_task
import logging

logger = logging.getLogger(__name__)

def _try_start_inference_task(repository, user, dataset_name, schema):
    # If there's an existing task then don't start another
    repository.cursor.execute("BEGIN;")
    repository.cursor.execute("SELECT task_id FROM dataset_paths WHERE name = %s;", (dataset_name,))
    task_id = repository.cursor.fetchone()[0]
    task_result = AsyncResult(task_id) if task_id else None
    # If already pending then don't start another
    if task_result and task_result.status in ['PENDING', 'STARTED', 'SUCCESS']:
        logger.info("Inference task already started for dataset %s. Status is %s", dataset_name, task_result.status)
        return False
    task = process_file_task.delay(str(user.username), dataset_name, json.dumps(schema.to_json_object()))
    if task is None:
        repository.cursor.execute("ROLLBACK;")
        return False
    repository.cursor.execute("UPDATE dataset_paths SET task_id = %s WHERE name = %s;", (task.task_id, dataset_name))
    repository.cursor.execute("COMMIT;")
    logger.info("Inference task started for dataset %s", dataset_name)
    return True

def upload_and_process_file(user, file, schema, column_types):
    """
    Uploads a CSV file and processes it in the background.

    Args:
        user (str): The username.
        file (File): The CSV file to upload.
        schema (Schema): The schema with na_values and max_categories.
        column_types (list): The column types.
    """
    file_name = get_valid_filename(file.name)
    
    # Remove older file sessions for the same user and dataset
    FileSession.objects.filter(user=user, dataset_name=file_name).delete()
    session = FileSession.objects.create(status='Initiated', user=user, dataset_name=file_name)
    logger.info("%s: File upload session initiated by user: %s", session.session_id, user.username)
    
    with PostgresqlRepository.get_repository(user, create_db=True) as repository:
        with repository.get_dataset_writer(file_name, column_types, schema=schema) as writer:
            rowcount = 0
            # Make sure the table is empty
            for chunk in file.chunks():
                data = chunk.decode('utf-8')
                writer.write(data)
                rowcount += data.count('\n') + (1 if data and data[-1] != '\n' else 0)
            
            repository.cursor.execute("UPDATE dataset_paths SET upload_status = 'Ready' WHERE name = %s;", (file_name,))
            repository.cursor.execute("COMMIT;")

    session.status = 'Ready'
    session.save()
    return session.session_id, rowcount

def download_dataframe_subset(user, dataset_name, start_row, num_rows, tolerance=None, filter='', preferred_types=None):
    """
    Reads a subset of the CSV data based on the specified row range.
                
    Columns are cast firstly to the preferred type if specified and then an inferred type otherwise. 
    Values that don't fit the resulting type are recorded in the exceptions column as JSON: {'<column name>': '<value>', ...}
    Inference selects the highest specificity type with exceptions <= tolerance. Otherwise 'object' is selected.
    The latest exception counts are read from schema.column_types.                

    Args:
        user (str): The username.
        dataset_name (str): The name of the CSV dataset.
        start_row (int): The 1-based index of the first row to read.
        num_rows (int): The number of rows to read.
        tolerance (int): The number of exceptions before any data type is considered invalid.
        filter (str): The filter to apply to the data.
        preferred_types (dict): The preferred data types for each column.
    """
    repository = PostgresqlRepository.get_repository(str(user.username))
    df, exceptions, preferred_types = DataFrameReader.read_csv_subset(repository, dataset_name, start_row, num_rows, tolerance, filter, preferred_types=preferred_types)
    return df, exceptions, preferred_types

def get_datasets(user, path, depth=1):
    """
    Enumerates the datasets in the specified path.

    Args:
        user (str): The username.
        path (str): The path to enumerate.
    """
    repository = PostgresqlRepository.get_repository(user, create_db=True)
    return repository.enumerate_datasets(path, depth=depth)

def set_preferred_types(user, dataset_name, preferred_types, tolerance):
    repository = PostgresqlRepository.get_repository(user, create_db=True)
    return repository.set_preferred_types(dataset_name, preferred_types, tolerance)

def do_manage_inference(user, dataset_name, command, schema = None):
    try:
        with PostgresqlRepository.get_repository(user) as repository:
            if command == 'reset':
                if schema is None:
                    schema, _ = repository.read_schema(dataset_name)
                    
                schema = Schema(max_categories=schema.max_categories, na_values=schema.na_values)
                with repository.connection.cursor() as cursor:
                    cursor.execute("UPDATE dataset_paths SET schema_data = %s, task_id = NULL WHERE name = %s;", (json.dumps(schema.to_json_object(), default=int), dataset_name))
                    cursor.execute("COMMIT;")
            elif command == 'start':
                curschema, _ = repository.read_schema(dataset_name)
                if schema is not None and curschema.position <= 1:
                    curschema.na_values = schema.na_values
                    curschema.max_categories = schema.max_categories
                
                if not _try_start_inference_task(repository, user, dataset_name, curschema):
                    raise RuntimeError(f"Failed to start inference task for dataset {dataset_name}")
            else:
                raise ValueError(f"Unknown command {command}")
    except Exception as e:
        return {'status': 'Error', 'message': str(e)}
    
