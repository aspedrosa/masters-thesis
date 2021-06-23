from celery import Celery

app = Celery("sender", broker="redis://localhost")


@app.task
def send_updates(db_hash, pipeline_id, filename):
    # TODO check if the associated subscription is active
    pass
