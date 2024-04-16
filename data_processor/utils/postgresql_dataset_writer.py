from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import io
import logging

logger = logging.getLogger(__name__)

class PostgresqlDatasetWriter:
    def __init__(self, connection, table_name):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.table_name = table_name
        self._create_table()
        self.partial_data = ""  # Buffer for storing partial data

    def _create_table(self):
        create_table_query = f"""
        DROP TABLE IF EXISTS {self.table_name};
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()  # Commit the changes for table creation
        except Exception as e:
            logger.error(f"Failed to create table {self.table_name}: {e}")
            self.connection.rollback()  # Rollback on error
            raise Exception(f"Failed to create table {self.table_name}: {e}")

    def write(self, csv_rows):
        if not csv_rows:
            return  # No data to process

        complete_data = self.partial_data + csv_rows  # Append new data to any stored partial data
        last_newline = complete_data.rfind('\n')  # Find the last complete line

        if last_newline == -1:  # No newlines found, entire chunk is partial
            self.partial_data += csv_rows
            return
        else:
            self.partial_data = complete_data[last_newline+1:]  # Store remaining partial line for next write
            complete_data = complete_data[:last_newline+1]  # Data to be written now

        try:
            self.cursor.execute("BEGIN;")
            csv_file_like_object = io.StringIO(complete_data)
            self.cursor.copy_from(csv_file_like_object, self.table_name, columns=('data',), sep='\1')
            self.cursor.execute("COMMIT;")
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            self.connection.rollback()  # Rollback on error
            raise Exception(f"Failed to write data: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.partial_data:  # Attempt to write any remaining partial data at closure
            self.write('\n')  # Ensuring it ends with a newline to process the last partial data
        if exc_type:
            self.connection.rollback()
        self.cursor.close()
