import unittest
from data_processor.utils.postgresql_repository import PostgresqlRepository

class TestPostgresqlDatasetReader(unittest.TestCase):
    def setUp(self):
        # Initialize the repository and create a test dataset
        with PostgresqlRepository.get_repository('test') as repository:
            # Prepare a CSV file and index with known content
            with repository.get_dataset_writer('dataset', [{'name': 'Name'}, {'name': 'Age'}]) as writer:
                writer.write("John,30\nJane,25\nDoe,40")

    def tearDown(self):
        # Drop the temporary database
        try:
            PostgresqlRepository.drop_repository('test')
        except Exception as e:
            pass

    def test_read_single_row(self):
        """Test reading a single row."""
        with PostgresqlRepository.get_repository('test') as repository:
            with repository.get_dataset_reader('dataset') as reader:
                data = reader.read(start_row=2, chunk_size=1)
                expected_data = 'Jane,25'
                self.assertEqual(data, expected_data)
        # See if the file is present in the repository by enumerating the files
        with PostgresqlRepository.get_repository('test') as repository:
            files = repository.enumerate_datasets()
            self.assertTrue(any(d['name'] == 'dataset' and d['is_dataset'] == True for d in files))

    def test_read_beyond_file_length_returns_nothing(self):
        """Test reading beyond the file length."""
        with PostgresqlRepository.get_repository('test') as repository:
            with repository.get_dataset_reader('dataset') as reader:
                data = reader.read(100, 10)
                self.assertIsNone(data)

if __name__ == '__main__':
    unittest.main()
