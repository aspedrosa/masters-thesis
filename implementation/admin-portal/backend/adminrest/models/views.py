import rest_framework.filters
from django.conf import settings
from kafka import KafkaProducer
from rest_framework import viewsets
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django_filters.rest_framework import DjangoFilterBackend

from . import models, serializers


class CommunityViewSet(viewsets.ModelViewSet):
    queryset = models.Community.objects.all()
    serializer_class = serializers.CommunitySerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("filters",)


class DatabaseViewSet(viewsets.ModelViewSet):
    queryset = models.Database.objects.all()
    serializer_class = serializers.DatabaseSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("database_identifier", "community")


class FilterViewSet(viewsets.ModelViewSet):
    queryset = models.Filter.objects.all()
    serializer_class = serializers.FilterSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("status", "communities")


class ApplicationViewSet(viewsets.ModelViewSet):
    queryset = models.Application.objects.all()
    serializer_class = serializers.ApplicationSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("status", "community")


class ApplicationDataSentViewSet(viewsets.ModelViewSet):
    queryset = models.ApplicationSentData.objects.all()
    serializer_class = serializers.ApplicationDataSentSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("application",)


class AgentHealthCheckViewSet(viewsets.ModelViewSet):
    queryset = models.DatabaseHealthCheck.objects.all()
    serializer_class = serializers.AgentHealthCheckSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("database",)


class DatabaseUploadViewSet(viewsets.ModelViewSet):
    queryset = models.DatabaseUpload.objects.all()
    serializer_class = serializers.DatabaseUploadSerializer
    filter_backends = (DjangoFilterBackend, rest_framework.filters.OrderingFilter,)
    filter_fields = ("database",)


@api_view(["PUT"])
def stop_filter(request, filter_id):
    filter = models.Filter.objects.get(id=filter_id)

    if filter.status == models.STATUS_STOPPED:
        return Response()

    filter.status = models.STATUS_STOPPED
    filter.save(update_fields=("status",))

    for application in filter.applications.all():
        application.status = models.STATUS_STOPPED
        application.save(update_fields=("status",))

    return Response()


@api_view(["PUT"])
def start_application(request, application_id):
    application, change = _manage_application(application_id, models.STATUS_ACTIVE)
    if not change:
        return Response()

    filter = application.filter
    if filter.status == models.STATUS_STOPPED:
        filter.status = models.STATUS_ACTIVE
        filter.save(update_fields=("status",))

    return Response()


@api_view(["PUT"])
def stop_application(request, application_id):
    application, change = _manage_application(application_id, models.STATUS_STOPPED)
    if not change:
        return Response()

    filter = application.filter
    if filter.status == models.STATUS_ACTIVE and not filter.applications.filter(status=models.STATUS_ACTIVE).exists():
        filter.status = models.STATUS_STOPPED
        filter.save(update_fields=("status",))

    return Response()


def _manage_application(application_id, new_status):
    application = models.Application.objects.get(id=application_id)

    if application.status == new_status:
        return application, False

    application.status = new_status
    application.save(update_fields=("status",))

    return application, True


@api_view(["POST"])
def request_health_check(request, database_id):
    database_identifier = get_object_or_404(models.Database, id=database_id).database_identifier

    producer = KafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVERS)
    producer.send(
        "HEALTH_CHECKS_REQUESTS",
        database_identifier.encode("utf-8"),
    )

    return Response()