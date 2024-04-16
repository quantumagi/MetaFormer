from django.apps import AppConfig

class DataProcessorConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'data_processor'

    def ready(self):
        import data_processor.signals
