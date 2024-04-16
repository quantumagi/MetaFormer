import logging

logger = logging.getLogger(__name__)

# Abstracts the reading of data from a PostgreSQL table
class PostgresqlDatasetReader:
    def __init__(self, connection, table_name, full_text_filter=None):
        self.connection = connection
        self.table_name = table_name
        self.full_text_filter = full_text_filter
        self.cursor = None

    def _initialize_cursor(self, start_row=0):
        try:            
            query = f"SELECT * FROM {self.table_name} WHERE id >= %s ORDER BY id ASC"
            params = (start_row, )
            
            if self.full_text_filter:
                query += " AND to_tsvector('english', data) @@ to_tsquery(%s);"
                params = (start_row, self.full_text_filter)
            else:
                query += ";"
                
            self.cursor = self.connection.cursor()
            self.cursor.execute(query, params)
        except Exception as e:
            self.cursor = None
            logger.error(f"Failed to initialize cursor: {e}")
            raise Exception(f"Failed to initialize cursor: {e}")

    def read(self, start_row=None, chunk_size=100):
        """Fetches a chunk of data."""
        # If start_row is not provided then just read the next chunk
        if start_row is not None:
            self._initialize_cursor(start_row=start_row)  # Initialize cursor with start_row if not already done

        try:
            results = self.cursor.fetchmany(size=chunk_size)
            if results:
                data_contents = '\n'.join([row[1] for row in results]) 
            else:
                logger.info("End of dataset reached.")
                data_contents = None
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            raise Exception(f"Failed to fetch data: {e}")

        return data_contents
    
    def get_num_rows(self):
        """Fetches the total number of rows in the table."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to fetch row count: {e}")
            raise Exception(f"Failed to fetch row count: {e}")

    def close(self):
        """Closes the cursor."""
        if self.cursor:
            self.cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
