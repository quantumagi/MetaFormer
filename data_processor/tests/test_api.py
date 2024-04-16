import unittest
import json
from unittest.mock import patch
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth.models import User
from rest_framework.test import APITestCase
from data_processor.api import upload_and_process_file, download_dataframe_subset, get_datasets
from data_processor.tasks import process_file_task
from data_processor.utils.schema import Schema

class TestAPI(APITestCase):
    @patch('data_processor.api.process_file_task.delay', return_value=None)
    def test_upload_data_success(self, mock_process_file_task):
        schema = Schema(column_types={"IntColumn": {}, "StringColumn": {}, "CategoryColumn": {}}, na_values=["NA", "na"])
        column_types = [{"name": "IntColumn"}, {"name": "StringColumn"}, {"name": "CategoryColumn"}]        
        
        # The file and schema are part of the same POST data
        csv_content = b"1,Hello,TypeA\n2,World,TypeB\nX,Test,TypeA"
        filename = "test.csv"
        username = 'test'
        file_mock = SimpleUploadedFile(filename, csv_content, content_type="text/csv")
        user = User.objects.get(username=username)
        session_id, rowcount = upload_and_process_file(user, file_mock, schema, column_types)
        process_file_task(username, filename, json.dumps(schema.to_json_object()))
        self.assertEqual(rowcount, 3)
        self.assertIsNotNone(session_id)

        # Now download it
        df, exceptions, preferred_types = download_dataframe_subset(user, filename, 1, 3, 1)
        df['exceptions'] = exceptions
        response_data = df.to_dict(orient='records')
        expected_data = [
            {"IntColumn": 1, "StringColumn": "Hello", "CategoryColumn": "TypeA", "exceptions": {}},
            {"IntColumn": 2, "StringColumn": "World", "CategoryColumn": "TypeB", "exceptions": {}},
            {"IntColumn": None, "StringColumn": "Test", "CategoryColumn": "TypeA", "exceptions": {'IntColumn': 'X'}}
        ]
        self.assertEqual(response_data, expected_data)

        # Enumerate the datasets
        directory_structure = get_datasets(user, '')
        # get_datasets returns a result of the form [{'name': 'test.csv'}]
        self.assertTrue(any(d['name'] == 'test.csv' for d in directory_structure))        
        
if __name__ == '__main__':
    unittest.main()
