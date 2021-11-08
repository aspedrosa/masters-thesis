import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

import filters
import globals
import send
import applications

# logger = logging.getLogger(__name__)
logger = logging
logging.root.setLevel(logging.INFO)


def json_deserializer(value):
    return json.loads(value.decode("utf-8"))


async def main():
    await applications.init_applications()
    filters.fetch_active_filters()
    await send.init_statistics_producer()

    asyncio.create_task(filters_done_watcher())
    asyncio.create_task(filters_management())

    async with AIOKafkaConsumer(
        "SENDERS_MANAGEMENT",
        bootstrap_servers=globals.BOOTSTRAP_SERVERS,
        value_deserializer=json_deserializer,
    ) as consumer:
        async for upload in consumer:
            action = upload.value.pop("action")
            if action == "ACTIVE":
                await applications.start_application(**upload.value)
            elif action == "STOPPED":
                await applications.stop_application(**upload.value)
            elif action == "EDIT":
                await applications.edit_application(**upload.value)


async def filters_management():
    async with AIOKafkaConsumer(
        "FILTER_WORKERS_MANAGEMENT",
        bootstrap_servers=globals.BOOTSTRAP_SERVERS,
        value_deserializer=json_deserializer,
    ) as consumer:
        async for upload in consumer:
            upload = upload.value
            action = upload.pop("action")
            if action == "ACTIVE":
                filters.start_filter(upload["filter_id"], upload["selections"])
            elif action == "STOPPED":
                filters.stop_filter(upload["filter_id"])
            elif action == "EDIT":
                filters.edit_filter(upload["filter_id"], upload["selections"])


async def filters_done_watcher():
    async with AIOKafkaConsumer(
        "FILTER_WORKERS_DATA_READY_TO_SEND",
        bootstrap_servers=globals.BOOTSTRAP_SERVERS,
        group_id="sender",
        #enable_auto_commit=False,
        value_deserializer=json_deserializer,
    ) as consumer:
        async for upload in consumer:
            logger.info(
                'An upload from db "%s" was redirected to filter worker with id %d',
                upload.value["database_identifier"],
                upload.value["filter_worker_id"],
            )
            asyncio.create_task(send.launch_workers(upload.value))


asyncio.run(main())
