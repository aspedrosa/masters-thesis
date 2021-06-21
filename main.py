import asyncio

from aiokafka import AIOKafkaConsumer
from kafka import TopicPartition

from task import send_updates


async def main():
    consumer = AIOKafkaConsumer(
        'my_topic', 'my_other_topic',
        bootstrap_servers='localhost:9092',
        group_id="my-group",
        enable_auto_commit=False,
    )

    async for upload in consumer:
        asyncio.create_task(
            parse_uploads(
                consumer,
                upload
            )
        )


async def parse_uploads(main_consumer, upload_info):
    consumer = AIOKafkaConsumer(
        'my_topic', 'my_other_topic',
        bootstrap_servers='localhost:9092',
        group_id="my-group")

    # TODO get active pipelines

    await consumer.start()

    count = 0
    async for done_notification in consumer:
        # TODO get active subscriptions subscriptions

        send_updates.delay(done_notification)

        pipelines_to_wait = 0  # TODO do the logic to check if all pipelines are done

        count += 1
        if count == pipelines_to_wait:
            break

    # TODO tell orchestrator is done

    main_consumer.commit({TopicPartition(upload_info.topic, upload_info.partition): upload_info.offset})

    await consumer.stop()

asyncio.run(main())
