
from django.db.models.signals import post_save
from django.dispatch import receiver
from concurrent.futures import ThreadPoolExecutor
from data_processor.models import FileSession
from data_processor.tasks import process_file_task
from django.db import transaction

executor = ThreadPoolExecutor(max_workers=10)  # Limit the number of concurrent workers

@receiver(post_save, sender=FileSession)    
def received_signal(sender, instance, created, **kwargs):
    if 'Initiated' in instance.status:
        # Not implemnted yet
        pass
