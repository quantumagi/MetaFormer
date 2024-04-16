import json
from rest_framework.decorators import api_view
from rest_framework.decorators import parser_classes
from rest_framework.decorators import permission_classes
from rest_framework.parsers import MultiPartParser, JSONParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from data_processor.utils.data_frame_reader import DataFrameReader
from data_processor.utils.postgresql_repository import PostgresqlRepository, repository_pool
from data_processor.utils.schema import Schema
from .api import download_dataframe_subset, upload_and_process_file, get_datasets, set_preferred_types, do_manage_inference
import logging

logger = logging.getLogger(__name__)

@api_view(['POST'])
@parser_classes([MultiPartParser, JSONParser])
@permission_classes([IsAuthenticated]) 
def upload_data(request):
    """
    Handles the upload of CSV data and schema using Django Rest Framework.
    """
    session = None
    try:
        file = request.FILES.get('file')
        if not file:
            raise ValueError("Value 'file' is missing")        
        column_types = request.data.get('column_types')  # Getting the schema as a JSON string
        if not column_types:
            raise ValueError("Value 'column_types' is missing")        
        try:
            column_types = json.loads(column_types)
        except json.JSONDecodeError:
            raise ValueError("Value 'column_types' must be a JSON string")
        schema_str = request.data.get('schema')  # Getting the schema as a JSON string
        if not schema_str:
            raise ValueError("Value 'schema' is missing")        
        try:
            schema = Schema()
            schema.from_json_object(json.loads(schema_str))
        except json.JSONDecodeError:
            raise ValueError("Value 'schema' must be a valid JSON string")        

        session_id, rowcount = upload_and_process_file(request.user, file, schema=schema, column_types=column_types)

        # Report the session ID and status
        response_data = {'status': 'Success', 'message': 'File uploaded successfully', 'session_id': session_id, 'row_count': rowcount}
        logger.info("%s: File uploaded successfully.", session_id)
        return Response(response_data)
    except Exception as e:
        if session:
            logger.error("%s: File upload failed: %s", session.session_id, str(e))
            session.status = 'Failed'
            session.save()
        else:
            logger.error("%s: File upload failed: %s", request.user, str(e))
        return Response({'status': 'Error', 'message': str(e)}, status=500)

@api_view(['GET'])
@parser_classes([MultiPartParser, JSONParser])
@permission_classes([IsAuthenticated]) 
def download_data(request):
    """
    Fetches a subset of the CSV data based on the user, file name, start row, and number of rows using Django Rest Framework.
    """
    # Extracting query parameters
    dataset_name = request.query_params.get('dataset_name')
    start_row = request.query_params.get('start_row')
    num_rows = request.query_params.get('num_rows')
    tolerance = request.query_params.get('tolerance', 0)
    filter = request.query_params.get('filter')
    preferred_types = request.query_params.get('preferred_types')

    # Ensure required parameters are provided
    if not all([dataset_name, start_row, num_rows]):
        return Response({'status': 'Error', 'message': 'Missing required parameters'}, status=400)

    # Convert start_row and num_rows to integers
    try:
        if start_row:
            start_row = int(start_row)
        else:
            # 'None' indicates that the next chunk of data should be read
            start_row = None
        num_rows = int(num_rows)
        tolerance = int(tolerance)
        # The preferred types are of the form:
        # preferred_types = [
        #   {'name': 'Age', 'type': 'int8', 'values': ''},
        #   {'name': 'Lastname', 'type': 'object'}
        #   ...]
        # Decode the JSON string (if any) to a dictionary
        if preferred_types:
            preferred_types = json.loads(preferred_types)
    except ValueError:
        return Response({'status': 'Error', 'message': 'Invalid parameters'}, status=400)
    
    try:
        repository = repository_pool.get_repository(str(request.user.username))
        df, exceptions, preferred_types = DataFrameReader.read_csv_subset(repository, dataset_name, start_row, num_rows, tolerance, filter, preferred_types=preferred_types)
        df['_exceptions'] = exceptions
        res = { 'rows': df.to_json(orient='values'), 'inferred_types': preferred_types }
        return Response(res)
    except Exception as e:
        return Response({'status': 'Error', 'message': str(e)}, status=404)

@api_view(['GET'])
@parser_classes([MultiPartParser, JSONParser])
@permission_classes([IsAuthenticated]) 
def enumerate_datasets(request):
    try:
        path = request.query_params.get('path')
        depth = request.query_params.get('depth', 1)
        try:
            depth = int(depth)
        except ValueError:
            return Response({'status': 'Error', 'message': 'Invalid parameters'}, status=400)
        
        directory_structure = get_datasets(request.user, path, depth)    
        return Response(directory_structure)
    except Exception as e:
        return Response({'status': 'Error', 'message': str(e)}, status=500)
        
@api_view(['POST'])
@parser_classes([MultiPartParser, JSONParser])
@permission_classes([IsAuthenticated]) 
def preferred_types(request):
    """
    Handles the upload of CSV data and schema using Django Rest Framework.
    """
    try:
        # The name of the dataset to update
        dataset_name = request.data.get('dataset_name')
        if not dataset_name:
            raise ValueError("Value 'dataset_name' is missing")
        # This can be a subset of columns that have overridden types        
        preferred_types = request.data.get('preferred_types')
        if not preferred_types:
            raise ValueError("Value 'column_types' is missing")
        tolerance = request.data.get('tolerance', None)
        tolerance = int(tolerance) if tolerance else None
    
        set_preferred_types(request.user, dataset_name, preferred_types, tolerance)

        # Report success
        response_data = {'status': 'Success', 'message': 'Preferred types set successfully'}
        logger.info("Preferred types set successfully.")
        return Response(response_data)
    except Exception as e:
        return Response({'status': 'Error', 'message': str(e)}, status=500)

@api_view(['POST'])
@parser_classes([MultiPartParser, JSONParser])
@permission_classes([IsAuthenticated]) 
def manage_inference(request):
    try:
        # The name of the dataset to update
        dataset_name = request.data.get('dataset_name')
        if not dataset_name:
            raise ValueError("Value 'dataset_name' is missing")
        # This can be a subset of columns that have overridden types        
        command = request.data.get('command')
        if not command:
            raise ValueError("Value 'command' is missing")
        # Use a schema to pass na_values and max_categories
        schema_str = request.data.get('schema')  # Getting the schema as a JSON string
        schema = None
        if schema_str:
            try:
                schema = Schema()
                schema.from_json_object(json.loads(schema_str))
            except json.JSONDecodeError:
                raise ValueError("Value 'schema' must be a valid JSON string")        
        
        do_manage_inference(request.user, dataset_name, command, schema)
        # Report success
        response_data = {'status': 'Success', 'message': 'Command executed successfully'}
        logger.info("Command executed successfully.")
        return Response(response_data)
    except Exception as e:
        return Response({'status': 'Error', 'message': str(e)}, status=500)
        