# handlers.py
from django.dispatch import receiver
from .signals import session_expired
from .utils.postgresql_repository import RepositoryPool, repository_pool

repository_pool = RepositoryPool()  # Add this line to define the repository_pool

@receiver(session_expired)
def handle_session_expired(sender, user_id, **kwargs):
    print(f"Handle cleanup for expired session of user ID {user_id}.")
    repository_pool.release_repository(user_id=user_id)

exported = [handle_session_expired]