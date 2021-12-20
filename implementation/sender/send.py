import asyncio
import datetime
import io
import json
import logging
import tempfile

import aiokafka
import avro.io
import avro.schema
import pandas
import requests
from aiokafka import AIOKafkaConsumer
from jinja2 import Template
from kafka.structs import TopicPartition

import applications
import filters
import globals


all_normal_order = [
    "analysis_id",
    "stratum_1",
    "stratum_2",
    "stratum_3",
    "stratum_4",
    "stratum_5",
    "count_value",
    "min_value",
    "max_value",
    "avg_value",
    "stdev_value",
    "median_value",
    "p10_value",
    "p25_value",
    "p75_value",
    "p90_value",
]  # TODO receive this as a environment variable

logger = logging
logging.root.setLevel(logging.INFO)

statistics_producer = None
SENDER_STATISTICS_TOPIC = "SENDER_STATISTICS"


async def init_statistics_producer():
    global statistics_producer
    statistics_producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=globals.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await statistics_producer.start()


async def launch_workers(upload_info):
    logger.info("get application lock")
    async with applications.applications_mtx.reader_lock:
        for application in applications.applications[upload_info["filter_id"]].values():
            logger.info("created task for app " + str(application.id))
            asyncio.create_task(send_updates(upload_info, application))


async def send_updates(upload_info: dict, application: applications.Application):
    logger.info(
        'Sending updates of application with id  %d of db with identifier "%s"',
        application.id, upload_info["database_identifier"],
    )

    logger.info("generating file")
    data_file = await transform_avro_records_to_csv_file(
        filters.filters.get(upload_info["filter_id"]),
        upload_info,
    )

    data_file.seek(0)
    with data_file, requests.Session() as session:
        logger.info("generating contexts")
        jinja_context = {
            "community": application.community,
            "database_identifier": upload_info["database_identifier"],
            "filtered_data": pandas.read_csv(data_file),
        }
        data_file.seek(0)  # since pandas will read from the file
        eval_context = {
            "data_file": data_file
        }

        try:
            logger.info("rendering template")
            render_result = Template(application.request_template).render(**jinja_context)

            logger.info("sending request")
            response = session.request(**eval(render_result, eval_context))  # TODO check if a dict was returned
        except Exception as e:
            logger.info("exception")
            await statistics_producer.send(
                SENDER_STATISTICS_TOPIC,
                {
                    "application_id": application.id,
                    "time": int(datetime.datetime.now().timestamp() * 1000),
                    "response_code": 0,
                    "response_data": str(e),
                },
            )
        else:
            logger.info("all good")
            await statistics_producer.send(
                SENDER_STATISTICS_TOPIC,
                {
                    "application_id": application.id,
                    "time": int(datetime.datetime.now().timestamp() * 1000),
                    "response_code": response.status_code,
                    "response_data": response.content.decode("utf-8"),
                },
            )


async def transform_avro_records_to_csv_file(filter_selections, upload_info: dict[str, any]):
    data_file = tempfile.TemporaryFile("w+")

    logger.info("writting header")
    if len(filter_selections) == 0:
        data_file.write(",".join(all_normal_order))
        order = all_normal_order
    else:
        order = filter_selections
        for i, column in enumerate(order):
            data_file.write(column)
            if i != len(order) - 1:
                data_file.write(",")
    data_file.write("\n")

    topic = f'FILTER_WORKER_{upload_info["filter_worker_id"]}_FILTER_{upload_info["filter_id"]}'
    topic_partition = TopicPartition(topic, 0)

    logger.info("initilize consumer to read from data topic")
    async with AIOKafkaConsumer(topic, bootstrap_servers=globals.BOOTSTRAP_SERVERS) as consumer:
        consumer.seek(topic_partition, upload_info["last_offset"] + 1 - upload_info["count"])

        schema = globals.SCHEMA_REGISTRY_CLIENT.get_schema(f"{topic}-value", "latest").schema.raw_schema
        schema = avro.schema.parse(json.dumps(schema))
        reader = avro.io.DatumReader(schema)

        count = 0
        logger.info("will read from data topic")
        async for record in consumer:
            # the first 5 bytes are the schema id
            decoder = avro.io.BinaryDecoder(io.BytesIO(record.value[5:]))
            record = reader.read(decoder)

            for i, column in enumerate(order):
                if record[column.upper()] is not None:
                    data_file.write(str(record[column.upper()]))
                if i != len(order) - 1:
                    data_file.write(",")

            data_file.write("\n")

            count += 1
            if count >= upload_info["count"]:
                break

    return data_file
