import os
import django

"""Run administrative tasks."""
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metaformer.settings')
django.setup()