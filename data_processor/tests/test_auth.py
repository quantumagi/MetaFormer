from django.urls import reverse
from django.contrib.auth.models import User
from rest_framework import status
from rest_framework.test import APITestCase

class TokenObtainPairViewTest(APITestCase):
    
    def setUp(self):
        # Setup run before every test method.
        self.test_user = User.objects.create_user('testuser', 'test@example.com', 'testpassword')
        self.token_obtain_pair_url = reverse('token_obtain_pair')

    def test_obtain_token(self):
        """
        Ensure we can obtain a token pair with valid credentials.
        """
        data = {'username': 'testuser', 'password': 'testpassword'}
        response = self.client.post(self.token_obtain_pair_url, data, format='json')
        
        # Check that the response status code is 200 (OK)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Check that the response contains an 'access' token
        self.assertTrue('access' in response.data)
