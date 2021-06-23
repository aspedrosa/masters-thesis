import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from kafka import KafkaAdminClient
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from task import send_updates


def json_deserializer(value):
    return json.loads(value.decode("utf-8"))


async def main():
    admin = KafkaAdminClient(bootstrap_servers=["localhost:29092"])
    try:
        admin.create_topics((NewTopic("PIPELINES_SETS_UPLOAD_NOTIFICATIONS", -1, -1, {}),))
    except TopicAlreadyExistsError:
        pass

    db_engine = create_async_engine("sqlite+aiosqlite:///demo.db")

    consumer = AIOKafkaConsumer(
        "PIPELINES_SETS_UPLOAD_NOTIFICATIONS",
        bootstrap_servers="localhost:29092",
        group_id="sender_main",
        enable_auto_commit=False,
        value_deserializer=json_deserializer
    )

    async with db_engine.connect() as conn, consumer:
        async for upload in consumer:
            asyncio.create_task(
                parse_uploads(conn, consumer, upload),
            )


async def parse_uploads(conn, main_consumer, upload_info):
    pipelines_set = upload_info.value["pipelines_set"]

    consumer = AIOKafkaConsumer(
        f"PIPELINES_SET_{pipelines_set}_DATA_READY_TO_SEND",
        bootstrap_servers="localhost:29092",
        group_id="sender_task_creator",
        value_deserializer=json_deserializer
    )

    done_producer = AIOKafkaProducer(
        bootstrap_servers="localhost:29092",
    )

    pipelines_done = set()
    active_at_start = await _get_active_pipelines(conn)

    async with consumer:
        while not await _all_done(conn, active_at_start, pipelines_done):
            done_notification = await consumer.getone()

            pipeline_id = done_notification.value["pipeline_id"]

            # TODO for each subscription associated with this pipeline create a new task
            #  or do all this on the celery task
            send_updates.delay(upload_info.value["db_hash"], pipeline_id, done_notification.value["filename"])

            pipelines_done.add(pipeline_id)

    async with done_producer:
        await asyncio.gather(
            done_producer.send("PIPELINES_SETS_DONE", pipelines_set.to_bytes(4, "big")),
            main_consumer.commit({TopicPartition(upload_info.topic, upload_info.partition): upload_info.offset}),
        )


async def _all_done(conn, active_at_start, pipelines_done):
    currently_active = await _get_active_pipelines(conn)

    active_at_start.intersection_update(currently_active)

    return len(set.intersection(active_at_start, pipelines_done)) == len(active_at_start)


async def _get_active_pipelines(conn):
    results = await conn.execute(text('SELECT id FROM pipelines WHERE status = "ACTIVE"'))
    return set(row[0] for row in results)


asyncio.run(main())
