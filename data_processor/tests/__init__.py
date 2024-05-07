import os
import django
from django.contrib.auth.models import User

# Run administrative tasks.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metaformer.settings')
django.setup()

# Add a test user.
if not User.objects.filter(username='test').exists():
    User.objects.create_user('test', 'test@test.com', 'testpassword')