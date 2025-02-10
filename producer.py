import csv
import time

from confluent_kafka import Producer

from log import logger
from settings import BATCH_SIZE, INPUT_CSV_PATH, PRODUCER_CONF, TOPIC
from utils import timeit


def delivery_report(err, msg, num_rows, message_count):
    if err is not None:
        logger.error(
            f"Failed to deliver message to {msg.topic()} [{msg.partition()}]: {err} - Message Count: {message_count[0]}")
    else:
        logger.info(
            f"Message successfully delivered to {msg.topic()} [{msg.partition()}] - Offset: {msg.offset()} - Message Length (Rows): {num_rows} - Message Count: {message_count[0]}")

    message_count[0] += 1


@timeit
def produce_kafka_messages():
    global producer
    producer = Producer(PRODUCER_CONF)
    try:
        message_count = [0]

        for csv_file in INPUT_CSV_PATH:
            with open(csv_file, 'r') as file:
                logger.debug(f"Reading data from {csv_file}")
                reader = csv.reader(file)
                rows = []

                for row in reader:
                    rows.append(','.join(row))

                    if len(rows) == BATCH_SIZE:
                        messages = '\n'.join(rows)
                        producer.produce(
                            TOPIC,
                            value=messages,
                            callback=lambda err, msg: delivery_report(
                                err, msg, len(rows), message_count)
                        )
                        rows = []
                        time.sleep(0.07)
                        producer.poll(0)

                if rows:
                    messages = '\n'.join(rows)
                    producer.produce(
                        TOPIC,
                        value=messages,
                        callback=lambda err, msg: delivery_report(
                            err, msg, len(rows), message_count)
                    )

        producer.flush()
        logger.info(f"Total number of messages sent to Kafka: {message_count[0]}")
    finally:
        producer.close()


if __name__ == "__main__":
    producer = None
    try:
        produce_kafka_messages()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error in producer: {str(e)}")
