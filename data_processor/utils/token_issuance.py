from django.dispatch import receiver, Signal
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
import threading
from rest_framework_simplejwt.tokens import AccessToken
import logging

# TODO: This is a work in progress. The idea is to create a signal that 
# will be triggered when a user's token expires. This approach leverages
# the robust token refresh "keep-alive" mechanism to 100% reliably 
# determine when the user's session has expired. When the session expires, 
# a signal will be triggered to perform proper clean-up of the user session. 

logger = logging.getLogger(__name__)

# Define a signal for session expiration
session_expired = Signal()

timers = {}  # Dictionary to store user_id to timer mapping

def schedule_expiry_event(user_id, expiry_seconds):
    """Schedules or reschedules an expiry event to run when the user's token expires."""
    global timers
    # Retrieve the old timer before creating a new one
    old_timer = timers.get(user_id)
    
    # Create and start the new timer
    new_timer = threading.Timer(expiry_seconds, lambda: trigger_expiry_event(user_id))
    new_timer.start()
    
    # Update the dictionary with the new timer
    timers[user_id] = new_timer
    
    # Cancel the old timer after safely setting up the new one
    if old_timer:
        try:
            old_timer.cancel()
        except Exception as e:
            logger.error(f"Error cancelling the old timer for user {user_id}: {e}")

def trigger_expiry_event(user_id):
    """Triggered when a user's session should be considered expired."""
    try:
        logger.info(f"Session for user ID {user_id} has expired.")
        session_expired.send(sender=None, user_id=user_id)  # Send the signal
    finally:
        # Clean up the timer from the global dictionary to ensure no leaks
        timers.pop(user_id, None)

def cancel_timer(user_id):
    """Cancels the existing timer for a user, if any, typically called on user logout."""
    timer = timers.pop(user_id, None)
    if timer:
        try:
            timer.cancel()
        except Exception as e:
            logger.error(f"Error cancelling timer for user {user_id}: {e}")

class CustomTokenObtainPairView(TokenObtainPairView):
    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        access_token = response.data.get('access')
        if access_token:
            token = AccessToken(access_token)
            schedule_expiry_event(request.user.id, token.lifetime.total_seconds())
        return response

class CustomTokenRefreshView(TokenRefreshView):
    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        access_token = response.data.get('access')
        if access_token:
            token = AccessToken(access_token)
            schedule_expiry_event(request.user.id, token.lifetime.total_seconds())
        return response