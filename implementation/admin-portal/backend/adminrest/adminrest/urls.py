"""adminrest URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
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
from django.urls import include, path
from rest_framework import routers
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)

from models import views

router = routers.DefaultRouter()
router.register(r'communities', views.CommunityViewSet)
router.register(r'databases', views.DatabaseViewSet)
router.register(r'filters', views.FilterViewSet)
router.register(r'applications', views.ApplicationViewSet)
router.register(r'applicationdatasent', views.ApplicationDataSentViewSet)
router.register(r'applicationdatasent', views.ApplicationDataSentViewSet)
router.register(r'databasesuploads', views.DatabaseUploadViewSet)
router.register(r'agentshealthchecks', views.AgentHealthCheckViewSet)

urlpatterns = [
    path('api/', include(router.urls)),

    path('api/databases/<int:database_id>/request_health_check/', views.request_health_check),
    path('api/filters/<int:filter_id>/stop/', views.stop_filter),
    path('api/applications/<int:application_id>/stop/', views.stop_application),
    path('api/applications/<int:application_id>/start/', views.start_application),

    # auth
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
]
