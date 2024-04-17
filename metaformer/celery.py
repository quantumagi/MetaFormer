import os
import eventlet
eventlet.monkey_patch()

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metaformer.settings')

app = Celery('data_processor')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
