import ast
import io
import json
import os
import tempfile
import traceback

import avro.io
import avro.schema
import pandas
import requests
from celery import Celery
from celery.utils.log import get_task_logger
from jinja2 import Template
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from schema_registry.client import SchemaRegistryClient
from sqlalchemy import create_engine, text

app = Celery("sender", broker="redis://localhost:6379")  # TODO don't have this hardcoded
registry = SchemaRegistryClient("http://localhost:8081")  # TODO don't have this hardcoded

all_normal_order = "".join([
    "ANALYSIS_ID",
    "STRATUM_1",
    "STRATUM_2",
    "STRATUM_3",
    "STRATUM_4",
    "STRATUM_5",
    "COUNT_VALUE",
    "MIN_VALUE",
    "AVG_VALUE",
    "MEDIAN_VALUE",
    "P10_VALUE",
    "P25_VALUE",
    "P75_VALUE",
    "P90_VALUE",
])

logger = get_task_logger(__name__)


def create_db_engine():
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


@app.task
def launch_workers(db_hash, pipeline_id, pipelines_set, last_offset, count):
    db_engine = create_db_engine()

    with db_engine.connect() as conn:
        for subscription in conn.execute(text(
                'SELECT id from subscriptions '
                f"WHERE status = 'ACTIVE' AND pipeline_id = {pipeline_id}"
        )):
            send_updates.delay(db_hash, pipeline_id, pipelines_set, last_offset, count, subscription[0])


@app.task
def send_updates(db_hash, pipeline_id, pipelines_set, last_offset, count, subscription_id):
    logger.info('Sending updates of subscription with %d of db with hash "%s"', subscription_id, db_hash)

    db_engine = create_db_engine()

    with db_engine.connect() as conn:
        results = conn.execute(text(
            "SELECT 1 from subscriptions "
            f"WHERE id = {subscription_id} AND status = 'ACTIVE' AND pipeline_id = {pipeline_id}"
        ))

        try:
            next(results)
        except StopIteration:
            logger.info(
                'Subscription with %d is longer active. Skipping processing data of db with hash "%s"',
                subscription_id, db_hash,
            )
            return

        data_file = transform_avro_records_to_csv_file(conn, pipeline_id, pipelines_set, last_offset, count)

        subscription_requests = conn.execute(text(
            "SELECT id, request_arguments_template, success_condition_template "
            f"FROM requests WHERE subscription_id = {subscription_id} "
            "ORDER BY request_order"
        ))

        data_file.seek(0)
        with data_file, requests.Session() as session:

            responses = []
            context = {
                "db_hash": db_hash,
                "data_file": data_file,
                "data": pandas.read_csv(data_file),
                "responses": responses,
            }
            data_file.seek(0)  # since pandas will read from the file

            results = conn.execute(
                "INSERT INTO subscription_logs (success_count, subscription_id) "
                f"VALUES (0, {subscription_id}) "
                "RETURNING id"
            )
            subscription_logs_id = results.fetchone()[0]

            success_count = 0

            for request_id, request_arguments_template, success_condition_template in subscription_requests:
                try:
                    render_result = Template(request_arguments_template).render(**context)

                    response = session.request(**ast.literal_eval(render_result))  # TODO check if a dict was returned

                    if success_condition_template is not None:
                        render_result = Template(success_condition_template).render(response=response)

                        if not bool(
                                eval(  # noqa - we trust the admins
                                    render_result
                                )
                        ):
                            raise AssertionError("Success condition not met")
                except:
                    logger.exception(
                        'Error for request with id %d of subscription with id %d '
                        'while processing data of db with hash "%s"',
                        request_id, subscription_id, db_hash,
                    )

                    conn.execute(text(
                        "INSERT INTO request_logs (request_id, subscription_log_id, success, exception) "
                        f"VALUES ({request_id}, {subscription_logs_id}, false, '{traceback.format_exc()}')"
                    ))
                else:
                    success_count += 1
                    conn.execute(text(
                        "INSERT INTO request_logs (request_id, subscription_log_id, success) "
                        f"VALUES ({request_id}, {subscription_logs_id}, true)"
                    ))

                    logger.exception(
                        'Request with id %d of subscription with id %d '
                        'while processing data of db with hash "%s" done',
                        request_id, subscription_id, db_hash,
                    )

                responses.append(response)

            if success_count > 0:
                conn.execute(text(
                    "UPDATE subscription_logs "
                    f"SET success_count = {success_count} "
                    f"WHERE id = {subscription_logs_id}"
                ))


def transform_avro_records_to_csv_file(conn, pipeline_id, pipelines_set, last_offset, count):
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

    data_file = tempfile.TemporaryFile("w+")

    results = conn.execute(text(
        "SELECT selection_column from pipeline_selections "
        f"WHERE id = {pipeline_id} "
        "ORDER BY selection_order"
    ))
    order = results.fetchall()
    if len(order) == 0:
        data_file.write(all_normal_order)
        order = all_normal_order
    else:
        order = [col[0].upper() for col in order]
        for i, column in enumerate(order):
            data_file.write(column)
            if i != len(order) - 1:
                data_file.write(",")
    data_file.write("\n")

    topic = f"PIPELINES_SET_{pipelines_set}_PIPELINE_{pipeline_id}"
    topic_partition = TopicPartition(topic, 0)
    consumer = KafkaConsumer(bootstrap_servers=(BOOTSTRAP_SERVERS,))
    consumer.assign((topic_partition,))
    consumer.seek(topic_partition, last_offset + 1 - count)

    schema = registry.get_schema(f"{topic}-value", "latest").schema.raw_schema
    schema = avro.schema.parse(json.dumps(schema))
    reader = avro.io.DatumReader(schema)

    for _ in range(count):
        while True:
            try:
                record = next(consumer)
                break
            except StopIteration:
                pass

        # the first 5 bytes are the schema id
        decoder = avro.io.BinaryDecoder(io.BytesIO(record.value[5:]))
        record = reader.read(decoder)

        for i, column in enumerate(order):
            if record[column] is not None:
                data_file.write(str(record[column]))
            if i != len(order) - 1:
                data_file.write(",")

        data_file.write("\n")

    consumer.close(autocommit=False)

    return data_file
