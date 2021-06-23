import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.structs import TopicPartition
from sqlalchemy.ext.asyncio import create_async_engine

from task import send_updates


async def main():
    db_engine = create_async_engine("sqlite:///demo.db")

    consumer = AIOKafkaConsumer(
        "PIPELINES_SETS_UPLOAD_NOTIFICATIONS",
        bootstrap_servers="localhost:9092",
        group_id="sender_main",
        enable_auto_commit=False,
    )

    with db_engine.connect() as conn:
        async with consumer:
            async for upload in consumer:
                asyncio.create_task(
                    parse_uploads(conn, consumer, upload),
                )


async def parse_uploads(conn, main_consumer, upload_info):
    pipelines_set = 0  # TODO get this from the upload_info record

    consumer = AIOKafkaConsumer(
        f"PIPELINES_SET_{pipelines_set}_DATA_READY_TO_SEND",
        bootstrap_servers="localhost:9092",
        group_id="sender_task_creator",
        value_deserializer=json.loads
    )

    done_producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
    )

    pipelines_done = set()
    active_at_start = _get_active_pipelines(conn)

    async with consumer:
        while not _all_done(conn, active_at_start, pipelines_done):
            done_notification = await consumer.getone()

            pipeline_id = done_notification.value["pipeline_id"]
            filename = done_notification.value["filename"]

            # TODO for each subscription associated with this pipeline create a new task
            #  or do all this on the celery task
            send_updates.delay(pipeline_id, filename)

            pipelines_done.add(pipeline_id)

    async with done_producer:
        await asyncio.gather(
            done_producer.send("PIPELINES_DONE", None, pipelines_set),
            main_consumer.commit({TopicPartition(upload_info.topic, upload_info.partition): upload_info.offset}),
        )


def _all_done(conn, active_at_start, pipelines_done):
    currently_active = _get_active_pipelines(conn)

    active_at_start.intersection_update(currently_active)

    return len(set.intersection(active_at_start, pipelines_done)) == len(active_at_start)


def _get_active_pipelines(conn):
    results = await conn.execute('SELECT id FROM pipelines WHERE status = "ACTIVE"')
    return set(row[0] for row in results)


asyncio.run(main())
