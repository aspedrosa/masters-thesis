import ast
import os
import traceback

import pandas
import requests
from celery import Celery
from jinja2 import Template
from sqlalchemy import create_engine, text

app = Celery("sender", broker="redis://localhost:6379")


def create_db_engine():
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


@app.task
def launch_workers(db_hash, pipeline_id, filename):
    db_engine = create_db_engine()

    with db_engine.connect() as conn:
        for subscription in conn.execute(text(
                'SELECT id from subscriptions '
                f"WHERE status = 'ACTIVE' AND pipeline_id = {pipeline_id}"
        )):
            send_updates.delay(db_hash, filename, subscription[0])


@app.task
def send_updates(db_hash, filename, subscription_id):
    DATA_READY_DIRECTORY = os.getenv("DATA_READY_DIRECTORY")

    db_engine = create_db_engine()

    with db_engine.connect() as conn:
        results = conn.execute(text(
            "SELECT 1 from subscriptions "
            f"WHERE id = {subscription_id} AND status = 'ACTIVE'"
        ))

        try:
            next(results)
        except StopIteration:
            return

        subscription_requests = conn.execute(text(
            "SELECT id, request_arguments_template, success_condition_template "
            f"FROM requests WHERE subscription_id = {subscription_id} "
            "ORDER BY request_order"
        ))

        with open(os.path.join(DATA_READY_DIRECTORY, filename)) as data_file, requests.Session() as session:

            responses = []
            context = {
                "db_hash": db_hash,
                "data_file": data_file,
                "data": pandas.read_csv(data_file),
                "responses": responses,
            }

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

                responses.append(response)

            if success_count > 0:
                conn.execute(text(
                    "UPDATE subscription_logs "
                    "SET success_count = {success_count} "
                    f"WHERE id = {subscription_logs_id}"
                ))
