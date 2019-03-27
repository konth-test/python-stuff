import pika
import json

QUEUE_NAME = "test"
QUEUE_URL = "localhost"

def publish(country, year, db_path):
    mq_conn = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_URL))
    mq_channel = mq_conn.channel()
    mq_channel.queue_declare(queue=QUEUE_NAME)

    body = {
        "country": country,
        "year": year,
        "db_path": db_path
    }

    mq_channel.publish(
        exchange = '',
        routing_key ='test',
        body=json.dumps(body)
    )

    mq_channel.close()

publish("Brazil", 2009, "C:/Users/konth/Desktop/exam/db/chinook.db")
