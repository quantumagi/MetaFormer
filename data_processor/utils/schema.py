import json

from data_processor.utils.data_frame_type_inference import DataFrameTypeInference

class Schema:
    def __init__(self, max_categories=100, column_types=None, category_values=None, position=1, status="incomplete", na_values=None):
        self.max_categories:int = max_categories
        self.column_types:dict = Schema.lists_to_sets(column_types or {})
        self.category_values:dict = Schema.lists_to_sets(category_values or {})
        self.position:int = position
        self.status = status
        self.na_values:list = na_values or []

    # Serialize the schema to a JSON object
    def from_json_object(self, schema):
        # Must be a dictionary
        if not isinstance(schema, dict):
            raise ValueError("Schema data must be a dictionary.")        
        self.max_categories = schema.get("max_categories", self.max_categories)
        self.column_types = Schema.lists_to_sets(schema.get("column_types", self.column_types))
        self.category_values = Schema.lists_to_sets(schema.get("category_values", self.category_values)) 
        self.position = schema.get("position", self.position)
        self.status = schema.get("status", self.status)
        self.na_values = schema.get("na_values", self.na_values)

    # Update the schema from an inference object
    def from_inference(self, inference: DataFrameTypeInference):
        self.column_types = inference.column_types
        self.category_values = inference.category_values
        self.position = inference.rows_processed + 1
    
    # Deserialize the schema from a JSON object
    def to_json_object(self):
        return {
            "max_categories": self.max_categories,
            "column_types": Schema.sets_to_lists(self.column_types),
            "category_values": Schema.sets_to_lists(self.category_values),
            "position": self.position,
            "status": self.status,
            "na_values": self.na_values
        }
        
    @staticmethod
    def lists_to_sets(data):
        if isinstance(data, list):
            return set(data)
        if isinstance(data, dict):
            newdata = {}
            for key, value in data.items():
                newdata[key] = Schema.lists_to_sets(value)
            return newdata
        return data
    
    @staticmethod
    def sets_to_lists(data):
        if isinstance(data, set):
            return list(data)
        if isinstance(data, dict):
            newdata = {}
            for key, value in data.items():
                newdata[key] = Schema.sets_to_lists(value)
            return newdata
        return data
        
    @staticmethod
    def from_json_string(json_schema):
        """
        Reads the a schema string and returns the schema information. Converts the column_types back to sets for each column.
        """
        json_object = json.loads(json_schema)
        schema = Schema()
        schema.from_json_object(json_object)
        return schema
        
    def write_schema_file(self, file_path):
        """
        Writes schema information and na_values to disk.
    
        Args:
            position (int): The current position in the data stream indicating how much data has been processed.
        """
        json_object = self.to_json_object()
        with open(file_path, 'w') as schema_file:
            json.dump(json_object, schema_file)
