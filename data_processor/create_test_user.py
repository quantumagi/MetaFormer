from django.conf import settings
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

class Command(BaseCommand):
    help = 'Create a test user'

    def handle(self, *args, **options):
        if not settings.DEBUG:
            self.stdout.write(self.style.ERROR('This command can only be run in debug mode.'))
            return

        User = get_user_model()
        if User.objects.count() == 0:
            User.objects.create_superuser('testuser', 'test@example.com', 'testpassword')
            self.stdout.write(self.style.SUCCESS('Successfully created test user'))
        else:
            # The == 0 check further safeguards against creating a test user in a production environment
            # but also prevents the command from running multiple times in a development environment
            self.stdout.write(self.style.WARNING('A user already exists; no test user created.'))

