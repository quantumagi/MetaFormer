"""
URL configuration for MetaFormer project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from data_processor.utils.token_issuance import CustomTokenObtainPairView, CustomTokenRefreshView
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path('admin/', admin.site.urls),
    # Include Django's authentication URLs
    path('accounts/', include('django.contrib.auth.urls')),
    # Include the data_processor app's URLs. This will include 'data-processor/' in the URL path.
    path('api/', include('data_processor.urls')), 
    # For JWT token obtain
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    # For JWT token refresh
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
]