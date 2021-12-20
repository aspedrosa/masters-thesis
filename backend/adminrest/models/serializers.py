from django.conf import settings
from django.db import transaction
from django.db.models import Q
from rest_framework import serializers
import requests

from . import models


def update_instance(instance, validated_data):
    for attr, value in validated_data.items():
        setattr(instance, attr, value)
    instance.save()


class CommunitySerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Community
        fields = ("id", "name")


class DatabaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Database
        fields = ("id", "community", "name", "database_identifier")


class FilterSelectionSerializer(serializers.ListField):
    child = serializers.ChoiceField(settings.COLUMNS)

    def to_representation(self, data):
        return super().to_representation(data.all())


class FilterSerializer(serializers.ModelSerializer):
    communities = serializers.PrimaryKeyRelatedField(
        many=True,
        queryset=models.Community.objects.all(),
        required=False,
    )
    status = serializers.CharField(read_only=True)
    selections = FilterSelectionSerializer(required=False)

    class Meta:
        model = models.Filter
        fields = ("id", "name", "filter", "communities", "selections", "status")

    def create(self, validated_data):
        selections = validated_data.pop("selections") if "selections" in validated_data else tuple()
        communities = validated_data.pop("communities") if "communities" in validated_data else tuple()

        filter = models.Filter.objects.create(**validated_data)
        for column in selections:
            models.FilterSelection.objects.create(filter=filter, column=column)

        if communities:
            for community in communities:
                filter.communities.add(community)

        return filter

    def update(self, instance, validated_data):
        selections = set(validated_data.pop("selections")) if "selections" in validated_data else set()

        with transaction.atomic():
            instance.selections.filter(~Q(column__in=selections)).delete()

            to_create = selections - set(
                instance.selections.filter(column__in=selections).values_list("column", flat=True)
            )

            for column in to_create:
                models.FilterSelection.objects.create(filter=instance, column=column)

        update_instance(instance, validated_data)

        return instance


class ApplicationSerializer(serializers.ModelSerializer):
    status = serializers.CharField(read_only=True)

    class Meta:
        model = models.Application
        fields = ("id", "community", "name", "status", "filter", "request_template")

    def create(self, validated_data):
        application = models.Application.objects.create(**validated_data)

        return application

    def update(self, instance, validated_data):
        if "filter" in validated_data:
            validated_data.pop("filter")

        update_instance(instance, validated_data)

        return instance


class ApplicationDataSentSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ApplicationSentData
        fields = ("id", "application", "time", "response_code", "response_data")


class AgentHealthCheckSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.DatabaseHealthCheck
        fields = ("id", "database", "time", "reason")


class DatabaseUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.DatabaseUpload
        fields = ("id", "database", "time", "rows")
