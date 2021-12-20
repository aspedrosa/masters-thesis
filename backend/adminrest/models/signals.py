import json

import requests
from django.conf import settings
from django.db.models import signals
from django.dispatch import receiver
from kafka import KafkaProducer

from . import models, serializers

producer = KafkaProducer(
    bootstrap_servers=settings.BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@receiver(signals.post_save, sender=models.Database)
def database_create(instance, created, **kwargs):
    if created:
        requests.post(
            settings.KSQL_URL,
            json={
                "ksql":
                    f"""
                        CREATE STREAM db_{instance.database_identifier}_healthchecks (reason string, time timestamp)
                        WITH (kafka_topic='db_{instance.database_identifier}_healthchecks', partitions=1, value_format='json');
                        
                        CREATE STREAM healthchecks_{instance.database_identifier}
                        WITH (kafka_topic='	AGENTS_HEALTH_CHECKS') AS
                        SELECT {instance.id} as "database_id", time as "time", reason as "reason"
                        FROM db_{instance.database_identifier}_healthchecks;
                    
                        CREATE STREAM db_{instance.database_identifier}_status (status VARCHAR, offset STRUCT<rows BIGINT, timestamp timestamp>)
                        WITH (kafka_topic='db_{instance.database_identifier}_status', partitions=1, value_format='json');
                        
                        CREATE STREAM upload_notifications_{instance.database_identifier}
                        WITH (kafka_topic='DATABASES_UPLOAD_NOTIFICATIONS') AS
                        SELECT
                        '{instance.database_identifier}' as "database_identifier",
                        {instance.id} as "database_id",
                        offset->timestamp as "time",
                        offset->rows - 1 AS "rows"
                        FROM db_{instance.database_identifier}_status WHERE status = 'COMPLETED';
                    """,
            },
        )


@receiver(signals.post_delete, sender=models.Database)
def database_delete(instance, **kwargs):
    requests.post(
        settings.KSQL_URL,
        json={
            "ksql":
                f"""
                    DROP STREAM db_{instance.database_identifier}_healthchecks;
                    DROP STREAM healthchecks_{instance.database_identifier};
                    DROP STREAM upload_notifications_{instance.database_identifier};
                    DROP STREAM db_{instance.database_identifier}_status;
                """,
        },
    )


@receiver(signals.post_save, sender=models.Filter)
def filter_change(instance, created, update_fields, **kwargs):
    if not created:
        if update_fields is not None and "status" in update_fields:
            message = dict(
                action=instance.status,
                filter_id=instance.id,
            )

            if instance.status == "ACTIVE":
                filter = serializers.FilterSerializer().to_representation(instance)
                for key in ("status", "id"):
                    filter.pop(key)
                message.update(filter)

            producer.send("FILTER_WORKERS_MANAGEMENT", message)
        elif instance.status == "ACTIVE":
            message = dict(
                action="EDIT",
                filter_id=instance.id,
            )

            filter = serializers.FilterSerializer().to_representation(instance)
            for key in ("status", "id"):
                filter.pop(key)
            message.update(filter)

            producer.send("FILTER_WORKERS_MANAGEMENT", message)


@receiver(signals.post_delete, sender=models.Filter)
def filter_delete(instance, **kwargs):
    message = dict(
        action="STOPPED",
        filter_id=instance.id,
    )
    producer.send("FILTER_WORKERS_MANAGEMENT", message)


@receiver(signals.post_save, sender=models.Application)
def application_change(instance: models.Application, created, update_fields, **kwargs):
    if not created:
        if update_fields is not None and "status" in update_fields:
            message = dict(
                action=instance.status,
                filter_id=instance.filter.id,
                application_id=instance.id,
                community=instance.community.name,
                request_template=instance.request_template
            )

            producer.send("SENDERS_MANAGEMENT", message)
        elif instance.status == "ACTIVE":
            message = dict(
                action="EDIT",
                filter_id=instance.filter.id,
                application_id=instance.id,
                community=instance.community.name,
                request_template=instance.request_template
            )

            producer.send("SENDERS_MANAGEMENT", message)


@receiver(signals.post_delete, sender=models.Filter)
def application_delete(instance, **kwargs):
    message = dict(
        action="STOPPED",
        filter_id=instance.filter.id,
        application_id=instance.id,
    )
    producer.send("SENDERS_MANAGEMENT", message)

    filter = instance.filter
    if filter.status == models.STATUS_ACTIVE and not filter.applications.filter(status=models.STATUS_ACTIVE).exists():
        filter.status = models.STATUS_STOPPED
        filter.save(update_fields=("status",))
