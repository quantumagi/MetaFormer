# consumers.py
from channels.generic.websocket import AsyncWebsocketConsumer
import json

class DataTypeConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        # You might want to add logic here to subscribe the client to specific updates

    async def disconnect(self, close_code):
        # Handle disconnection
        pass

    async def send_data_type_update(self, event):
        # Called when someone updates a data type
        data_type_update = event['data_type_update']
        await self.send(text_data=json.dumps({
            'data_type_update': data_type_update
        }))
