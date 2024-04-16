from django.urls import path
from .views import download_data, upload_data, enumerate_datasets, preferred_types, manage_inference

urlpatterns = [
    path('download_data', download_data, name='download_data'),
    path('upload_data', upload_data, name='upload_data'),
    path('enumerate_datasets', enumerate_datasets, name='enumerate_datasets'),
    path('preferred_types', preferred_types, name='preferred_types'),
    path('manage_inference', manage_inference, name='manage_inference')
]
