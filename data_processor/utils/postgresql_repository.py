import sys
import psycopg2
import json
from data_processor.utils.postgresql_dataset_writer import PostgresqlDatasetWriter
from data_processor.utils.postgresql_dataset_reader import PostgresqlDatasetReader
from .schema import Schema
from django.conf import settings
from celery.result import AsyncResult
import logging
import threading
import logging

logger = logging.getLogger(__name__)

DATASET_DATABASE_PREFIX = 'datasets_'
MAXIMUM_TABLE_NAME_LENGTH = 63

class RepositoryPool:
    def __init__(self, repository_factory = None):
        self.lock = threading.Lock()
        self.user_repositories = {}
        self.repository_factory = repository_factory
        
    def get_repository(self, user_id):
        """Cache user repositories."""
        with self.lock:
            if user_id in self.user_repositories:
                return self.user_repositories[user_id]
            
            repository = self.repository_factory(user_id)
            self.user_repositories[user_id] = repository
            return repository
        
    def release_repository(self, user_id):
        """Releases the repository assigned to a user."""
        with self.lock:
            if user_id in self.user_repositories:
                repository = self.user_repositories.pop(user_id)
                # Close the repository
                repository.close()

    def close_all_repositories(self):
        """Closes all managed repositories."""
        with self.lock:
            for user_id in list(self.repositories.keys()):
                self.user_repositories[user_id].close()
                del self.user_repositories[user_id]
            self.user_repositories.clear()

# Abstracts the PostgreSQL database operations for storing datasets and schemas
class PostgresqlRepository:
    """
    A repository implementation that uses a PostgreSQL database to store datasets and schemas.

    Implements the Repository interface for PostgreSQL databases:
        def write_schema(self, dataset_name, schema: Schema) - Writes the schema for a dataset.
        def read_schema(self, dataset_name) - Reads the schema for a dataset.
        def get_dataset_writer(self, dataset_name) - Returns a writer for the dataset.
        def get_dataset_reader(self, dataset_name, filter=None) - Returns a reader for the dataset.
        def enumerate_datasets(self, path='') - Lists all datasets and subfolders at the specified level in the hierarchy.
        def close() - Closes the connection to the repository.
    Static:
        def get_repository(forUser, create_db=False) - Returns the repository for the specified user.
        def drop_repository(for_user) - Drops the repository for the specified user.
    """
    def __init__(self, forUser):
        try:
            # Connect to the newly created database or existing one
            self.connection = self.get_connection(forUser)
            self.cursor = self.connection.cursor()
            self.dataset_reader = None # Cache the dataset reader for multiple chunked reads
        except psycopg2.OperationalError as e:
            logger.error("Error occurred while connecting to PostgreSQL:", exc_info=True)
            logger.error("Make sure PostgreSQL is installed and running.")
            raise e
            
    @staticmethod
    def get_dbname(for_user):
        dbname = f"{DATASET_DATABASE_PREFIX}{for_user}"
        dbname = ''.join(c for c in dbname if c.isalnum() or c == '_')
        return dbname
            
    @staticmethod
    def get_connection(for_user, create_repository = True):
        dbname = PostgresqlRepository.get_dbname(for_user)
        
        # Connect to the PostgreSQL server with default database 'postgres'
        with PostgresqlRepository._get_postgres_connection() as connection:
            #connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            cursor = connection.cursor()
            # Check if the database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}';")
            if cursor.fetchone() is None:
                # Only create the database if it doesn't exist
                cursor.execute("COMMIT;")
                cursor.execute(f"CREATE DATABASE {dbname};")

        # Connect to the newly created database or existing one
        connection = PostgresqlRepository._get_postgres_connection(dbname)
        if connection is None:
            raise Exception("Error occurred while connecting to PostgreSQL.")
        #connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        PostgresqlRepository._ensure_tables_exist(connection)
        return connection
    
    @staticmethod
    def _get_postgres_connection(dbname='postgres'):
        try:
            dbname = dbname or settings.DATABASES['test']['NAME']
            dbuser = settings.DATABASES['test']['USER']
            dbpassword = settings.DATABASES['test']['PASSWORD']
            dbhost = settings.DATABASES['test']['HOST']
            connection = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpassword, host=dbhost)
            return connection
        except psycopg2.OperationalError as e:
            logger.error("Error occurred while connecting to PostgreSQL:", exc_info=True)
            logger.error("Make sure PostgreSQL is installed and running.")   
            raise Exception("Error occurred while connecting to PostgreSQL.")             
            
    @staticmethod
    def drop_repository(for_user):
        # Drops the database and all tables
        dbname = f"{DATASET_DATABASE_PREFIX}{for_user}"
        connection = PostgresqlRepository._get_postgres_connection()
        if connection is None:
            return
        #connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        cursor = connection.cursor()
        cursor.execute("COMMIT;")
        cursor.execute(f"DROP DATABASE IF EXISTS {dbname};")

    @staticmethod
    def get_repository(forUser, create_db=True):
        return PostgresqlRepository(forUser)
    
    @staticmethod
    def _ensure_tables_exist(connection):
        try:
            with connection.cursor() as cursor:                 
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS dataset_paths (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        parent_id INTEGER REFERENCES dataset_paths(id) ON DELETE CASCADE,
                        is_dataset BOOLEAN DEFAULT FALSE,
                        upload_status TEXT NOT NULL DEFAULT 'Initiated',
                        column_types JSONB,
                        tolerance INT,
                        schema_data JSONB,
                        task_id TEXT
                    );
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dataset_paths_parent_id ON dataset_paths(parent_id);
                """)
                cursor.execute("COMMIT;")
        except Exception as e:
            print("An error occurred:", e)

    # Helper function to create a folder structure in the database
    def _get_path_id(self, path, is_dataset=True, createInfo=None):
        if path is None or path == '' or path == '/':  # Base case: root folder
            return None     
        
        # If the file or folder exists then just return it.
        self.cursor.execute("SELECT id FROM dataset_paths WHERE name = %s AND is_dataset = %s;", (path, is_dataset))
        folder_id = self.cursor.fetchone()
        if folder_id is not None:
            return folder_id[0]
        
        if createInfo is None:
            return None
        
        # Split the path into folder and filename components
        foldersplit = path.rsplit('/', 1)
        if (len(foldersplit) > 1):
            folder_id = self._get_path_id(foldersplit[0], is_dataset=False, createInfo=createInfo)
        else:
            folder_id = None

        # Extract column_types from createInfo
        prepared_column_types = createInfo.get('column_types')
        if prepared_column_types is None:
            if is_dataset:
                raise ValueError("Column types must be provided for datasets.")
        else:
            prepared_column_types = json.dumps(prepared_column_types, default=int)
        schema_data = createInfo.get('schema')
        if schema_data is not None:
            schema_data = json.dumps(schema_data.to_json_object(), default=int)

        # Insert an entry with that folder_id and full path
        self.cursor.execute("INSERT INTO dataset_paths (name, parent_id, is_dataset, column_types, schema_data) VALUES (%s, %s, %s, %s, %s) RETURNING id;", 
            (path, folder_id, is_dataset, prepared_column_types, schema_data))
        dataset_id = self.cursor.fetchone()[0]
        self.cursor.execute("COMMIT;")
        return dataset_id
    
    def _get_table_name(self, dataset_name, createInfo=None):
        dataset_id = self._get_path_id(dataset_name, createInfo=createInfo)
        if dataset_id is None:
            return None
        dataset_id_length = len(str(dataset_id))
        dataset_name = f"dataset_{dataset_name.rsplit('.', 1)[0]}"
        dataset_name = ''.join(c for c in dataset_name if c.isalnum() or c == '_')
        dataset_name = dataset_name[:MAXIMUM_TABLE_NAME_LENGTH - dataset_id_length - 1] + '_' + str(dataset_id)
        return dataset_name

    def write_schema(self, dataset_name, schema: Schema):
        dataset_id = self._get_path_id(dataset_name, createInfo={"schema": schema})
        if dataset_id is None:
            raise ValueError(f"Dataset not found {dataset_name}")
        self.cursor.execute(
            "UPDATE dataset_paths SET schema_data = %s WHERE id = %s AND is_dataset = TRUE;",
            (json.dumps(schema.to_json_object(), default=int), dataset_id)
        )
        self.cursor.execute("COMMIT;")

    def read_schema(self, dataset_name):
        dataset_id = self._get_path_id(dataset_name)
        if dataset_id is None:
            return None
        self.cursor.execute("SELECT schema_data, column_types FROM dataset_paths WHERE id=%s and is_dataset = TRUE LIMIT 1;", (dataset_id,))
        result = self.cursor.fetchone()
        if result:
            schema = Schema()
            schema.from_json_object(result[0] or {})            
            return schema, result[1] or []
        else:
            return None, None

    def get_dataset_writer(self, dataset_name, column_types, schema:Schema=None):
        """
        Returns a writer for the dataset.

        Args:
            dataset_name (str): The name of the dataset.

        Returns:
            PostgresqlDatasetWriter: A writer for the dataset.
        """
        # Assuming table_name is derived from dataset_name
        table_name = self._get_table_name(dataset_name, createInfo={'schema': schema, 'column_types': column_types})

        # Initialize the dataset writer with the database connection and table name
        dataset_writer = PostgresqlDatasetWriter(self.connection, table_name)
        return dataset_writer

    def get_dataset_reader(self, dataset_name, filter=None):
        """
        Returns a reader for the dataset.

        Args:
            dataset_name (str): The name of the dataset.
            filter (str): Optional filter condition to apply when reading the dataset.

        Returns:
            PostgresqlDatasetReader: A reader for the dataset.
        """
        # Assuming table_name is derived from dataset_name
        table_name = self._get_table_name(dataset_name)

        # Initialize the dataset reader with the database connection and table name
        # Cache it to allow multiple chunked reads
        if self.dataset_reader is None or self.dataset_reader.table_name != table_name or self.dataset_reader.full_text_filter != filter:
            self.dataset_reader = PostgresqlDatasetReader(self.connection, table_name, full_text_filter=filter)
        
        return self.dataset_reader
    
    def enumerate_datasets(self, path='', depth=1):
        """
        Lists all datasets and subfolders at the specified level in the hierarchy based on the path and depth,
        including their IDs. Utilizes the 'is_dataset' flag to distinguish between datasets and subfolders.

        Args:
            path (str): The path to enumerate.
            depth (int): The depth of the enumeration.

        Returns:
            A list of dictionaries, each containing the 'name', 'is_dataset', 'schema_data', 'row_count',
            and 'upload_status' of datasets or subfolders.
        """        
        enumeration = []
        
        def append(result):
            name, is_dataset, schema_data, column_types, tolerance, upload_status, task_id = result[1], result[2], result[3], result[4], result[5], result[6], result[7]
            if is_dataset:
                row_count = 0
                try:
                    table_name = self._get_table_name(name)
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = self.cursor.fetchone()[0]
                    task_result = AsyncResult(task_id) if task_id else None
                except Exception as e:
                    pass                    
                enumeration.append({
                    'name': name.rsplit('/', 1)[-1],  # Extract the last part of the path
                    'is_dataset': is_dataset,
                    'schema_data': schema_data,
                    'column_types': column_types,
                    'tolerance': tolerance,
                    'row_count': row_count,
                    'upload_status': upload_status,
                    'inference_status': task_result.status if task_result else None
                })

        if depth == 0:
            query = """
            SELECT id, name, is_dataset, schema_data, column_types, tolerance, upload_status, task_id
            FROM dataset_paths
            WHERE name = %s and is_dataset = true;
            """
            self.cursor.execute(query, (path ,))
            result = self.cursor.fetchall()
            append(result[0])
            return enumeration
            
        base_path_id = self._get_path_id(path)
        
        if base_path_id is None:
            current_parents = []
        else:
            current_parents = [base_path_id]  # Initialize with the base path ID

        for current_depth in range(1, depth + 1):
            # Convert list of parent IDs to a format suitable for SQL IN clause
            parent_ids_tuple = tuple(current_parents)
            # Modify the query if base_path_id is None
            if current_parents == []:
                query = """
                SELECT id, name, is_dataset, schema_data, column_types, tolerance, upload_status, task_id
                FROM dataset_paths
                WHERE parent_id is null;
                """
                self.cursor.execute(query)
            else:
                query = """
                SELECT id, name, is_dataset, schema_data, column_types, tolerance, upload_status, task_id
                FROM dataset_paths
                WHERE parent_id IN %s;
                """
                self.cursor.execute(query, (parent_ids_tuple,))
                
            results = self.cursor.fetchall()

            current_parents = []  # Reset for the next level
            for result in results:
                current_parents.append(result[0])  # Prepare parent IDs for the next depth level
                append(result)

        return enumeration    

    def set_preferred_types(self, dataset_name, preferred_types, tolerance=None):
        # Fetch the current column types from the database for the given dataset
        self.cursor.execute("SELECT column_types, tolerance FROM dataset_paths WHERE name=%s and is_dataset = TRUE LIMIT 1;", (dataset_name,))
        result = self.cursor.fetchone()
        if result is None:
            raise ValueError("Dataset not found.")
        
        columns = result[0]
        tolerance = result[1] if tolerance is None else tolerance

        # Dictionary to hold the new column types after merging
        updated_columns = {col['name']: col for col in preferred_types}

        # Convert the dictionary back to the list format expected by the database
        # If type is nothing then omit the type attribute but keep the name attribute.
        final_columns = [updated_columns.get(col['name'], {'name': col['name']}) for col in columns]

        # Update the preferred_types in the database with the merged column types
        self.cursor.execute("UPDATE dataset_paths SET column_types=%s, tolerance=%s WHERE name=%s AND is_dataset=TRUE", 
                            (json.dumps(final_columns), tolerance, dataset_name))

        # Commit the transaction to save changes
        self.cursor.connection.commit()
        
        return final_columns
        
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.dataset_reader:
            self.dataset_reader.close()
        if self.connection:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()        

# Create a long-lived pool of connections
repository_pool = RepositoryPool(PostgresqlRepository.get_repository)
