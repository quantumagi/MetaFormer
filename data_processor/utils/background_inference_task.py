import time
from data_processor.utils.postgresql_repository import PostgresqlRepository, DATASET_DATABASE_PREFIX
from data_processor.utils.batched_inference_and_schema_writer import BatchedInferenceAndSchemaWriter
from data_processor.utils.schema import Schema
from data_processor.models import FileSession
from contextlib import contextmanager
from django.core.cache import cache
import logging

logger = logging.getLogger(__name__)

# Background task kicked-off when a new file is uploaded
# Updates the schema with progressively collected type inference statistics
class BackgroundInferenceTask:
    def __init__(self):
        pass

    @staticmethod
    @contextmanager
    def dataset_lock(for_user, dataset_name):
        lock_key = f"lock_{for_user}_{dataset_name}"
        lock_acquired = cache.get(lock_key)
        if not lock_acquired:
            cache.set(lock_key, "true")  # Set the lock
            try:
                yield True  # Pass the lock status to the caller
            finally:
                cache.delete(lock_key)  # Release the lock after processing
        else:
            yield False 

    @staticmethod
    def do_work(for_user, dataset_name, schema_str):
        """
        Process the dataset and update the schema with type inference statistics.

        Only supply a 'column_types' for new datasets. For existing datasets, the schema is read from the database.
        """
        logger.info(f"Processing dataset {dataset_name} for user {for_user}. Schema is {schema_str}")
        with BackgroundInferenceTask.dataset_lock(for_user, dataset_name) as lock_acquired:
            if not lock_acquired:
                # Another process is already working on this dataset
                logger.info(f"Another process is already working on dataset {dataset_name}")
                return
            try:
                with PostgresqlRepository.get_repository(for_user) as repository:
                    # Write the schema for the dataset
                    if schema_str is not None:
                        schema = Schema.from_json_string(schema_str)
                        repository.write_schema(dataset_name, schema)
                    else:
                        schema, _ = repository.read_schema(dataset_name)
                        if not schema:
                            raise ValueError(f"Dataset not found {dataset_name}")                        
                    if schema.status == "complete":
                        return
                    
                    # See if we have a hard row-limit
                    query = """
                        SELECT upload_status, column_types
                        FROM dataset_paths
                        WHERE name = %s and is_dataset = true;
                        """
                    repository.cursor.execute(query, (dataset_name ,))
                    result = repository.cursor.fetchall()
                    upload_status = result[0][0] if result else None
                    column_types = result[0][1] if result else None
                    
                    logger.info(f"Dataset {dataset_name} upload status is {upload_status}")
                    upload_complete = upload_status == 'Ready'
                    
                    # Ensure any column_types [{'name':'col_1'}, ...] that are not in the schema
                    # are added as column types: ({'col_1':{'int8': 0, 'int16': 0 }, 'col_2': ...})
                    for column in column_types:
                        if column['name'] not in schema.column_types:
                            schema.column_types[column['name']] = {}
                                
                    logger.info(f"Starting with {schema.to_json_object()} schema for dataset {dataset_name}")

                    # Create a BatchedInferenceAndSchemaWriter object
                    inference = BatchedInferenceAndSchemaWriter(dataset_name, schema, repository)
                    # Call initialize if the schema is None
                    if schema is None:
                        # Initialize the schema
                        inference.initialize()
                    # Compare the number of rows in the schema with the number of rows in the dataset
                    dataset_reader = repository.get_dataset_reader(dataset_name)
                    # Back off incrementally if no data received
                    back_off = 1
                    start_row = inference.schema.position
                    while back_off < 60: # Stop processing if no new chunks received for 1 minute
                        ready_list = FileSession.objects.filter(dataset_name=dataset_name, status='Ready')
                        logger.info(f"Ready list: {ready_list}")
                        upload_complete = upload_complete or ready_list
                        while True:
                            # Get the next chunk of data
                            dataset = dataset_reader.read(start_row=start_row, chunk_size=1000)
                            # Indicate that we just want the next chunk from here onwards
                            start_row = None
                            if dataset is None:
                                if upload_complete:
                                    inference.finalize()
                                    return
                                back_off = back_off * 2
                                break
                            else:
                                # Process the data
                                inference.process(dataset)
                                logger.info(f"Processed {inference.schema.to_json_object()} schema for dataset {dataset}")
                                back_off = 1
                        # Sleep a bit before processing the next chunk
                        time.sleep(back_off)
                        
            except Exception as e:
                logger.error(f"Error processing dataset {dataset_name}: {str(e)}")
                raise e
